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
import java.util.Objects;

import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Strings;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.indices.recovery.RecoverySettings;

public class BlobStoreRepository<C>
        extends org.opensearch.repositories.blobstore.BlobStoreRepository
        implements CommonSettings.RepositorySettings {

    private final RepositorySettingsProvider<C> repositorySettingsProvider;

    private final BlobPath basePath;

    public BlobStoreRepository(final RepositoryMetadata metadata,
                               final NamedXContentRegistry namedXContentRegistry,
                               final ClusterService clusterService,
                               final RecoverySettings recoverySettings,
                               final RepositorySettingsProvider<C> repositorySettingsProvider) {
        super(metadata, false, namedXContentRegistry, clusterService, recoverySettings);
        this.repositorySettingsProvider = repositorySettingsProvider;
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
    public BlobPath basePath() {
        return basePath;
    }

    @Override
    protected ByteSizeValue chunkSize() {
        return CHUNK_SIZE.get(metadata.settings());
    }

    @Override
    protected BlobStore createBlobStore() throws Exception {
        final var storageIo =
                repositorySettingsProvider
                        .repositoryStorageIOProvider()
                        .createStorageIO(basePath().buildAsString(), metadata.settings());

        return new BlobStore() {
            @Override
            public BlobContainer blobContainer(final BlobPath path) {
                return new RepositoryBlobContainer(path, storageIo);
            }

            @Override
            public void close() throws IOException {
                if (Objects.nonNull(repositorySettingsProvider.repositoryStorageIOProvider())) {
                    repositorySettingsProvider.repositoryStorageIOProvider().close();
                }
            }
        };
    }

}
