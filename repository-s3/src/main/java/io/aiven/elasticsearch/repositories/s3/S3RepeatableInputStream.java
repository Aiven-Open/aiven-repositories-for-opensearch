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

package io.aiven.elasticsearch.repositories.s3;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import io.aiven.elasticsearch.repositories.Permissions;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.apache.http.HttpStatus;
import org.elasticsearch.core.internal.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3RepeatableInputStream extends InputStream {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3RepeatableInputStream.class);

    private final AmazonS3 client;

    private final String bucketName;

    private final String blobName;

    private final int maxRetries;

    private S3ObjectInputStream s3InputStream;

    private long offset = 0;

    protected int attempt = 0;

    private boolean closed = false;

    private final List<IOException> failures = new ArrayList<>(MAX_SUPPRESSED_EXCEPTIONS);

    private static final int MAX_SUPPRESSED_EXCEPTIONS = 10;

    public S3RepeatableInputStream(final AmazonS3 client,
                                   final String bucketName,
                                   final String blobName,
                                   final int maxRetries) throws IOException {
        this.client = client;
        this.bucketName = bucketName;
        this.blobName = blobName;
        this.maxRetries = maxRetries;
        this.s3InputStream = openStream();
    }

    private S3ObjectInputStream openStream() throws IOException {
        try {
            final var getObjectRequest = new GetObjectRequest(bucketName, blobName);
            if (offset > 0) {
                getObjectRequest.setRange(offset);
            }
            final var s3Object = Permissions.doPrivileged(() -> client.getObject(getObjectRequest));
            return s3Object.getObjectContent();
        } catch (final AmazonClientException e) {
            if (e instanceof AmazonS3Exception) {
                if (((AmazonS3Exception) e).getStatusCode() == HttpStatus.SC_NOT_FOUND) {
                    throw withSuppressed(new IOException("Couldn't find blob " + blobName));
                }
            }
            throw e;
        }
    }

    @Override
    public int read() throws IOException {
        isOpen();
        while (true) {
            try {
                final var result = s3InputStream.read();
                incrementOffset(result);
                return result;
            } catch (final IOException e) {
                reopenStreamOnFailure(e);
            }
        }
    }

    @Override
    public int read(final byte[] bytes, final int off, final int len) throws IOException {
        isOpen();
        while (true) {
            try {
                final var bytesRead = s3InputStream.read(bytes, off, len);
                incrementOffset(bytesRead);
                return bytesRead;
            } catch (final IOException e) {
                reopenStreamOnFailure(e);
            }
        }
    }

    private void incrementOffset(final int result) {
        if (result > 0) {
            offset += result;
        }
    }

    private void isOpen() {
        if (closed) {
            throw new IllegalStateException("Stream was closed");
        }
    }

    private void reopenStreamOnFailure(final IOException e) throws IOException {
        if (attempt > maxRetries) {
            throw withSuppressed(e);
        }
        if (failures.size() < MAX_SUPPRESSED_EXCEPTIONS) {
            failures.add(e);
        }
        attempt += 1;
        IOUtils.closeWhileHandlingException(s3InputStream);
        LOGGER.debug("Failed reading {}. Reopen stream attempt #{}", blobName, attempt);
        s3InputStream = openStream();
    }

    @Override
    public void close() throws IOException {
        try {
            s3InputStream.close();
        } finally {
            closed = true;
        }
    }

    @Override
    public synchronized void reset() throws IOException {
        throw new UnsupportedEncodingException("The reset method is not supported");
    }

    @Override
    public long skip(final long n) throws IOException {
        throw new UnsupportedEncodingException("The skip method is not supported");
    }

    private <T extends Throwable> T withSuppressed(final T ex) {
        failures.forEach(ex::addSuppressed);
        return ex;
    }

}
