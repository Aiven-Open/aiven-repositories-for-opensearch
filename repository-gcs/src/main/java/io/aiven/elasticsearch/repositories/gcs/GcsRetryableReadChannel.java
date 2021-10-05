/*
 * Copyright 2021 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.elasticsearch.repositories.gcs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.core.internal.io.IOUtils;

import com.google.cloud.ReadChannel;
import com.google.cloud.RestorableState;
import com.google.cloud.RetryHelper;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.StorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper around reads from GCS that will retry blob downloads that fail part-way through, resuming from where the 
 * failure occurred. It uses the fact that ReadChannel supports capture and restore functionality and could be recreated
 * from the last successful checkpoint.
 */
public class GcsRetryableReadChannel implements ReadableByteChannel {
    private static final Logger LOGGER = LoggerFactory.getLogger(GcsRetryableReadChannel.class);
    private static final int MAX_SUPPRESSED_EXCEPTIONS = 10;

    private ReadChannel delegate;
    private final List<StorageException> failures = new ArrayList<>(MAX_SUPPRESSED_EXCEPTIONS);
    private final BlobId blobId;
    private final int maxAttempts;
    private int attempt = 1;
    
    public GcsRetryableReadChannel(final ReadChannel delegate, final BlobId blobId, final int maxAttempts) {
        this.delegate = delegate;
        this.blobId = blobId;
        this.maxAttempts = maxAttempts;
    }
    
    @Override
    public boolean isOpen() {
        return delegate.isOpen();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public int read(final ByteBuffer dst) throws IOException {
        // The loop will stop on success or max attempts exhaustion
        while (true) {
            try {
                return delegate.read(dst);
            } catch (final IOException ex) {
                // The read() operation failed because all retry attempts have been exhausted
                if (ex.getCause() instanceof RetryHelper.RetryHelperException) {
                    final RetryHelper.RetryHelperException helper = (RetryHelper.RetryHelperException) ex.getCause();
                    final Throwable cause = helper.getCause();
                    if (cause instanceof IOException) {
                        restoreReaderOrFail(StorageException.translate((IOException) cause));
                    } else if (cause instanceof StorageException) {
                        restoreReaderOrFail((StorageException) cause);
                    } else { /* we don't know the cause */
                        throw ex;
                    }
                } else {
                    restoreReaderOrFail(StorageException.translate(ex));
                }
            }
        }
    }

    private void restoreReaderOrFail(final StorageException ex) throws IOException {
        if (ex.getCode() == 404) {
            throw withSuppressed(new NoSuchFileException("Blob object ["
                + blobId.getName() + "] not found: " + ex.getMessage()));
        }
        
        if (attempt >= maxAttempts || !ex.isRetryable()) {
            throw withSuppressed(ex);
        }
        
        final RestorableState<ReadChannel> state = delegate.capture();
        LOGGER.debug("Failed reading blob [{}], retrying", state, ex);
        
        attempt += 1;
        if (failures.size() < MAX_SUPPRESSED_EXCEPTIONS) {
            failures.add(ex);
        }
        
        IOUtils.closeWhileHandlingException(delegate);
        delegate = state.restore();
    }

    private <T extends Throwable> T withSuppressed(final T ex) {
        failures.forEach(ex::addSuppressed);
        return ex;
    }
}
