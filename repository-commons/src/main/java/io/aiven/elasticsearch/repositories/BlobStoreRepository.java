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

import java.io.IOException;
import java.io.UncheckedIOException;

import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.EncryptedBlobContainer;
import org.opensearch.common.crypto.CryptoHandler;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.compress.Compressor;
import org.opensearch.core.compress.CompressorRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.repositories.RepositoryException;

import io.aiven.elasticsearch.repositories.RepositoryStorageIOProvider.StorageIO;

public class BlobStoreRepository<C, S extends CommonSettings.ClientSettings>
        extends org.opensearch.repositories.blobstore.BlobStoreRepository
        implements CommonSettings.RepositorySettings {

    private final RepositorySettingsService<C, S> repositorySettingsProvider;

    private final BlobPath basePath;
    private final String repositoryName;
    private final boolean encryptionFormatV2;

    public BlobStoreRepository(final RepositoryMetadata metadata,
                               final NamedXContentRegistry namedXContentRegistry,
                               final ClusterService clusterService,
                               final RecoverySettings recoverySettings,
                               final RepositorySettingsService<C, S> repositorySettingsProvider) {
        super(metadata, namedXContentRegistry, clusterService, recoverySettings);
        if (metadata.cryptoMetadata() != null) {
            throw new RepositoryException(metadata.name(), "crypto_settings are not supported by this repository type");
        }
        this.encryptionFormatV2 = ENCRYPTION_FORMAT_V2.get(metadata.settings());
        if (encryptionFormatV2 && !repositorySettingsProvider.supportsEncryptionFormatV2()) {
            throw new RepositoryException(
                metadata.name(), ENCRYPTION_FORMAT_V2.getKey() + " is not supported by this repository type");
        }
        this.repositorySettingsProvider = repositorySettingsProvider;
        this.repositoryName = metadata.name();
        final String basePath = BASE_PATH.get(metadata.settings());
        var blobPath = BlobPath.cleanPath();
        if (!Strings.isNullOrEmpty(basePath)) {
            final var paths = basePath.split("/");
            for (final String elem : paths) {
                blobPath = blobPath.add(elem);
            }
        }
        this.basePath = blobPath;
    }

    @Override
    public Compressor getCompressor() {
        return CompressorRegistry.none();
    }

    @Override
    public BlobPath basePath() {
        return basePath;
    }

    @Override
    protected ByteSizeValue chunkSize() {
        return CHUNK_SIZE.get(metadata.settings());
    }

    @Override
    protected BlobStore createBlobStore() throws Exception {
        return new AivenBlobStore();
    }

    private record Storage(StorageIO stored, StorageIO repositoryKey, CryptoHandler<?, ?> cryptoHandler) {
    }

    private final class AivenBlobStore implements BlobStore {
        private int blobStoreGeneration = -1;
        private Storage storage;

        @Override
        public BlobContainer blobContainer(final BlobPath path) {
            final var current = currentStorage();
            if (current.cryptoHandler() == null) {
                return new RepositoryBlobContainer(path, current.repositoryKey());
            }
            final var stored = new RepositoryBlobContainer(path, current.stored());
            final var encrypted = new EncryptedBlobContainer<>(stored, current.cryptoHandler());
            if (current.repositoryKey() == null) {
                return encrypted;
            }
            final var repositoryKey = new RepositoryBlobContainer(path, current.repositoryKey());
            return new AivenBlobContainerV2(
                encrypted,
                stored,
                repositoryKey,
                encryptionFormatV2 ? encrypted : repositoryKey,
                current.cryptoHandler(),
                this::blobContainer);
        }

        @Override
        public void reload(final RepositoryMetadata repositoryMetadata) {
            // This is called when the isSystemRepository = true (only for remote storage repositories),
            // otherwise the repository is recreated with the new settings.

            // the repositoryMetadata is synced across the cluster
            // the reload plugin is called in turn only from prune leader, so we can safely reload the storageIO
        }

        @Override
        public void close() throws IOException {
            synchronized (this) {
                closeCryptoHandler();
            }
            repositorySettingsProvider.closeRepository(repositoryName);
        }

        private synchronized Storage currentStorage() {
            final var generation = repositorySettingsProvider.generation();
            if (generation == blobStoreGeneration) {
                return storage;
            }
            final var clear = storage != null;
            try {
                closeCryptoHandler();
                storage = repositorySettingsProvider.supportsEncryptionFormatV2()
                    ? createStorageV2(clear)
                    : new Storage(null, createStorageIO(clear), null);
            } catch (final IOException e) {
                throw new UncheckedIOException("Failed to create storage IO for repository", e);
            }
            blobStoreGeneration = generation;
            return storage;
        }

        private Storage createStorageV2(final boolean clear) throws IOException {
            final var stored = repositorySettingsProvider.createStoredStorageIO(repositoryName, metadata.settings(), clear);
            final var repositoryKey = encryptionFormatV2
                ? repositorySettingsProvider.createRepositoryKeyStorageIO(
                    basePath().buildAsString(), repositoryName, metadata.settings())
                : createStorageIO(false);
            final var cryptoHandler = repositorySettingsProvider.createCryptoHandler(repositoryName, metadata.settings());
            return new Storage(stored, repositoryKey, cryptoHandler);
        }

        private StorageIO createStorageIO(final boolean clear) throws IOException {
            return repositorySettingsProvider.createStorageIO(
                basePath().buildAsString(), repositoryName, metadata.settings(), clear);
        }

        private void closeCryptoHandler() throws IOException {
            if (storage != null && storage.cryptoHandler() != null) {
                storage.cryptoHandler().close();
            }
        }
    }

}
