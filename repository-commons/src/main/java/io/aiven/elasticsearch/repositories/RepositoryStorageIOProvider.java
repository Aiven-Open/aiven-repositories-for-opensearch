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

import javax.crypto.SecretKey;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import io.aiven.elasticsearch.repositories.io.CryptoIOProvider;
import io.aiven.elasticsearch.repositories.metadata.EncryptedRepositoryMetadata;
import io.aiven.elasticsearch.repositories.security.EncryptionKeyProvider;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.internal.io.Streams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.aiven.elasticsearch.repositories.BlobStoreRepository.BUFFER_SIZE_SETTING;

public abstract class RepositoryStorageIOProvider<C, S extends CommonSettings.ClientSettings>
        implements CommonSettings.RepositorySettings, Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(RepositorySettingsProvider.class);

    public static final String REPOSITORY_METADATA_FILE_NAME = "repository_metadata.json";

    private final S clientSettings;

    private SecretKey encryptionKey;

    private final EncryptionKeyProvider encryptionKeyProvider;

    private final ClientProvider<C, S> clientProvider;

    public RepositoryStorageIOProvider(final ClientProvider<C, S> clientProvider,
                                       final S clientSettings,
                                       final EncryptionKeyProvider encryptionKeyProvider) {
        this.clientProvider = clientProvider;
        this.clientSettings = clientSettings;
        this.encryptionKeyProvider = encryptionKeyProvider;
    }

    public StorageIO createStorageIO(final String basePath, final Settings repositorySettings) throws IOException {
        final var bufferSize = Math.toIntExact(BUFFER_SIZE_SETTING.get(repositorySettings).getBytes());
        final var client =
                Permissions.doPrivileged(() -> {
                    final var c = clientProvider.buildClientIfNeeded(clientSettings, repositorySettings);
                    createOrRestoreEncryptionKey(c, basePath, repositorySettings);
                    return c;
                });
        return createStorageIOFor(client, repositorySettings, new CryptoIOProvider(encryptionKey, bufferSize));
    }

    private void createOrRestoreEncryptionKey(final C client,
                                              final String basePath,
                                              final Settings repositorySettings) throws IOException {
        if (Objects.isNull(encryptionKey)) {
            final var repositoryMetadataFilePath = basePath + REPOSITORY_METADATA_FILE_NAME;
            final var encKeyRepoMetadata =
                    // restore a repository metadata file which contains the encryption key
                    // encrypted without compression and use different Cipher compare to
                    // regular backup files, that's why CryptoIOProvider reads/writes directly to
                    // the storage without compression and encryption, and it doesn't use encryption key and buffer size
                    createStorageIOFor(client, repositorySettings, new CryptoIOProvider(null, 0) {
                        @Override
                        public InputStream decryptAndDecompress(final InputStream in) throws IOException {
                            return in;
                        }

                        @Override
                        public long compressAndEncrypt(final InputStream in,
                                                       final OutputStream out) throws IOException {
                            return Streams.copy(in, out);
                        }

                    });
            final var repositoryMetadata = new EncryptedRepositoryMetadata(encryptionKeyProvider);
            if (encKeyRepoMetadata.exists(repositoryMetadataFilePath)) {
                LOGGER.info("Restore encryption key for repository. Path: {}", repositoryMetadataFilePath);
                try (final var in = encKeyRepoMetadata.read(repositoryMetadataFilePath)) {
                    encryptionKey = repositoryMetadata.deserialize(in.readAllBytes());
                }
            } else {
                LOGGER.info("Create new encryption key for repository. Path: {}", repositoryMetadataFilePath);
                encryptionKey = encryptionKeyProvider.createKey();
                final var repoMetadata = repositoryMetadata.serialize(encryptionKey);
                encKeyRepoMetadata.write(repositoryMetadataFilePath, new ByteArrayInputStream(repoMetadata),
                        repoMetadata.length, true);
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (Objects.nonNull(clientProvider)) {
            clientProvider.close();
        }
    }

    protected abstract StorageIO createStorageIOFor(final C client,
                                                    final Settings repositorySettings,
                                                    final CryptoIOProvider cryptoIOProvider);

    public interface StorageIO {

        boolean exists(final String blobName) throws IOException;

        InputStream read(final String blobName) throws IOException;

        void write(final String blobName,
                   final InputStream inputStream,
                   final long blobSize,
                   final boolean failIfAlreadyExists) throws IOException;

        Tuple<Integer, Long> deleteDirectories(final String path) throws IOException;

        void deleteFiles(final List<String> blobNames,
                         final boolean ignoreIfNotExists) throws IOException;

        List<String> listDirectories(final String path) throws IOException;

        Map<String, Long> listFiles(final String path, final String prefix) throws IOException;

    }

}
