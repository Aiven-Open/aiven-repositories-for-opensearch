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
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.compress.Compressor;
import org.opensearch.core.compress.CompressorRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.indices.recovery.RecoverySettings;

public class BlobStoreRepository<C, S extends CommonSettings.ClientSettings>
        extends org.opensearch.repositories.blobstore.BlobStoreRepository
        implements CommonSettings.RepositorySettings {

    private final RepositorySettingsService<C, S> repositorySettingsProvider;

    private final BlobPath basePath;
    private final String repositoryName;

    public BlobStoreRepository(final RepositoryMetadata metadata,
                               final NamedXContentRegistry namedXContentRegistry,
                               final ClusterService clusterService,
                               final RecoverySettings recoverySettings,
                               final RepositorySettingsService<C, S> repositorySettingsProvider) {
        super(metadata, namedXContentRegistry, clusterService, recoverySettings);
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

    private final class AivenBlobStore implements BlobStore {
        private int blobStoreGeneration = -1;
        private RepositoryStorageIOProvider.StorageIO enryptedStorageIo;

        @Override
        public BlobContainer blobContainer(final BlobPath path) {
            return new RepositoryBlobContainer(path, currentStorageIo());
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
            repositorySettingsProvider.closeRepository(repositoryName);
        }

        private synchronized RepositoryStorageIOProvider.StorageIO currentStorageIo() {
            try {
                final var generation = repositorySettingsProvider.generation();
                if (generation != blobStoreGeneration) {
                    final var clear = enryptedStorageIo != null;
                    enryptedStorageIo = repositorySettingsProvider.createStorageIO(
                        basePath().buildAsString(), repositoryName, metadata.settings(), clear);
                    blobStoreGeneration = generation;
                }
                return enryptedStorageIo;
            } catch (final IOException e) {
                throw new UncheckedIOException("Failed to create storage IO for repository", e);
            }
        }
    }

}
