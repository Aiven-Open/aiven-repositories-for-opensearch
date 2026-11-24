/*
 * Copyright 2026 Aiven Oy
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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.KeyPair;
import java.util.Map;
import java.util.function.Function;

import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.service.ClusterApplierService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.crypto.CryptoHandler;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.indices.recovery.RecoverySettings;

import io.aiven.elasticsearch.repositories.BlobStoreRepository;
import io.aiven.elasticsearch.repositories.CommonSettings;
import io.aiven.elasticsearch.repositories.RepositoryStorageIOProvider;
import io.aiven.elasticsearch.repositories.RepositoryStorageIOProvider.StorageIO;
import io.aiven.elasticsearch.repositories.io.CryptoIOProvider;

import com.google.cloud.storage.Storage;

import static io.aiven.elasticsearch.repositories.RepositoryStorageIOProvider.REPOSITORY_METADATA_FILE_NAME;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

final class BenchmarkSettingsService extends GcsSettingsProvider {

    private final Function<CryptoIOProvider, StorageIO> storage;

    private final KeyPair keyPair;

    private final CryptoIOProvider repositoryKey;

    BenchmarkSettingsService(final Function<CryptoIOProvider, StorageIO> storage,
                             final KeyPair keyPair,
                             final CryptoIOProvider repositoryKey) {
        this.storage = storage;
        this.keyPair = keyPair;
        this.repositoryKey = repositoryKey;
    }

    BlobContainer blobContainer(final String basePath, final boolean encryptionFormatV2) {
        final var clusterService = mock(ClusterService.class);
        when(clusterService.getClusterApplierService()).thenReturn(mock(ClusterApplierService.class));
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
        when(clusterService.getClusterSettings())
            .thenReturn(new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
        final var metadata = new RepositoryMetadata(
            "benchmark",
            GcsRepositoryPlugin.REPOSITORY_TYPE,
            Settings.builder()
                .put(CommonSettings.RepositorySettings.BASE_PATH.getKey(), basePath)
                .put(CommonSettings.RepositorySettings.ENCRYPTION_FORMAT_V2.getKey(), encryptionFormatV2)
                .build());
        final var repository = new BlobStoreRepository<>(
            metadata, NamedXContentRegistry.EMPTY, clusterService, mock(RecoverySettings.class), this);
        repository.start();
        return repository.blobStore().blobContainer(repository.basePath());
    }

    @Override
    public StorageIO createStorageIO(final String basePath,
                                     final String repositoryName,
                                     final Settings repositorySettings,
                                     final boolean clear) throws IOException {
        writeRepositoryMetadata(basePath);
        return storage.apply(repositoryKey);
    }

    void writeRepositoryMetadata(final String basePath) throws IOException {
        final var stored = storage.apply(CryptoIOProvider.UNENCRYPTED);
        if (!stored.exists(basePath + REPOSITORY_METADATA_FILE_NAME)) {
            stored.write(basePath + REPOSITORY_METADATA_FILE_NAME, new ByteArrayInputStream(new byte[0]), 0, false);
        }
    }

    @Override
    public StorageIO createStoredStorageIO(final String repositoryName,
                                           final Settings repositorySettings,
                                           final boolean clear) {
        return storage.apply(CryptoIOProvider.UNENCRYPTED);
    }

    @Override
    public StorageIO createRepositoryKeyStorageIO(final String basePath,
                                                  final String repositoryName,
                                                  final Settings repositorySettings) throws IOException {
        return storage.apply(CryptoIOProvider.UNENCRYPTED).exists(basePath + REPOSITORY_METADATA_FILE_NAME)
            ? storage.apply(repositoryKey)
            : null;
    }

    @Override
    public CryptoHandler<?, ?> createCryptoHandler(final String repositoryName, final Settings repositorySettings) {
        return createCryptoHandler(keyPair, CommonSettings.RepositorySettings.KEY_PROVIDER_NAME.get(repositorySettings));
    }

    @Override
    protected Map<String, GcsClientSettings> getEffectiveClientSettings(final Settings settings) {
        return Map.of();
    }

    @Override
    protected RepositoryStorageIOProvider<Storage, GcsClientSettings> createRepositoryStorageIOProvider() {
        throw new UnsupportedOperationException();
    }
}
