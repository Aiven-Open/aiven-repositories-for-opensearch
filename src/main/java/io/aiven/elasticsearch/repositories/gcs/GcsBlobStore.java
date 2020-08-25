/*
 * Copyright 2020 Aiven Oy
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

import io.aiven.elasticsearch.repositories.io.CryptoIOProvider;

import com.google.cloud.storage.Storage;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GcsBlobStore implements BlobStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(GcsBlobStore.class);

    private final String bucketName;

    private final GcsSettingsProvider gcsSettingsProvider;

    private final CryptoIOProvider cryptoIOProvider;

    public GcsBlobStore(final GcsSettingsProvider gcsSettingsProvider,
                        final CryptoIOProvider cryptoIOProvider,
                        final String bucketName) {
        this.gcsSettingsProvider = gcsSettingsProvider;
        this.bucketName = bucketName;
        this.cryptoIOProvider = cryptoIOProvider;
    }

    @Override
    public BlobContainer blobContainer(final BlobPath path) {
        LOGGER.info("Create container for path: {}", path);
        return new GcsBlobContainer(path, this, bucketName);
    }

    Storage client() throws IOException {
        return gcsSettingsProvider.gcsClient();
    }

    CryptoIOProvider cryptoIOProvider() {
        return cryptoIOProvider;
    }

    @Override
    public void close() throws IOException {
    }
}
