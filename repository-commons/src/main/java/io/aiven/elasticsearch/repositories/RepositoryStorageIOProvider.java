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

package io.aiven.elasticsearch.repositories;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import io.aiven.elasticsearch.repositories.io.CryptoIOProvider;
import io.aiven.elasticsearch.repositories.security.EncryptionKeyProvider;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;

public abstract class RepositoryStorageIOProvider<C>
        implements CommonSettings.RepositorySettings, Closeable {

    public static final String REPOSITORY_METADATA_FILE_NAME = "repository_metadata.json";

    protected final C client;

    protected final EncryptionKeyProvider encryptionKeyProvider;

    public RepositoryStorageIOProvider(final C client, final EncryptionKeyProvider encryptionKeyProvider) {
        this.client = client;
        this.encryptionKeyProvider = encryptionKeyProvider;
    }

    protected String metadataFilePath(final String basePath) {
        return basePath + REPOSITORY_METADATA_FILE_NAME;
    }

    public abstract StorageIO createStorageIO(final String basePah, final Settings repositorySettings)
            throws IOException;

    public abstract static class StorageIO {

        protected final String bucketName;

        protected CryptoIOProvider cryptoIOProvider;

        public StorageIO(final String bucketName, final CryptoIOProvider cryptoIOProvider) {
            this.bucketName = bucketName;
            this.cryptoIOProvider = cryptoIOProvider;
        }

        public abstract boolean exists(final String blobName) throws IOException;

        public abstract InputStream read(final String blobName) throws IOException;

        public abstract void write(final String blobName,
                                   final InputStream inputStream,
                                   final long blobSize,
                                   final boolean failIfAlreadyExists) throws IOException;

        public abstract Tuple<Integer, Long> deleteDirectories(final String path) throws IOException;

        public abstract void deleteFiles(final List<String> blobNames,
                                         final boolean ignoreIfNotExists) throws IOException;

        public abstract List<String> listDirectories(final String path) throws IOException;

        public abstract Map<String, Long> listFiles(final String path, final String prefix) throws IOException;

    }

}
