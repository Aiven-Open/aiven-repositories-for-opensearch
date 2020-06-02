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

package io.aiven.elasticsearch.gcs.storage;

import java.io.IOException;
import java.nio.file.Files;

import io.aiven.elasticsearch.gcs.GcsSettingsProvider;
import io.aiven.elasticsearch.gcs.utils.DummySecureSettings;
import io.aiven.elasticsearch.storage.RsaKeyAwareTest;
import io.aiven.elasticsearch.storage.security.EncryptionKeyProvider;

import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class GcsBlobStoreRepositoryTest extends RsaKeyAwareTest {

    @Mock
    NamedXContentRegistry mockedNamedXContentRegistry;

    @Mock
    ClusterService mockedClusterService;

    @Mock
    ClusterApplierService clusterApplierService;

    @BeforeEach
    void setUp() {
        when(mockedClusterService.getClusterApplierService()).thenReturn(clusterApplierService);
        when(clusterApplierService.threadPool()).thenReturn(mock(ThreadPool.class));
    }

    @Test
    void buildRepositoryWithCustomParameters() throws IOException {
        final var settings = Settings.builder()
                .put(GcsBlobStoreRepository.BUCKET_NAME.getKey(), "some_bucket_name")
                .put(GcsBlobStoreRepository.BASE_PATH.getKey(), "custom/path/in/bucket")
                .put(GcsBlobStoreRepository.CHUNK_SIZE.getKey(), new ByteSizeValue(50, ByteSizeUnit.MB))
                .setSecureSettings(
                        new DummySecureSettings()
                                .setFile(
                                        EncryptionKeyProvider.PUBLIC_KEY_FILE.getKey(),
                                        Files.newInputStream(publicKeyPem)
                                )
                                .setFile(
                                        EncryptionKeyProvider.PRIVATE_KEY_FILE.getKey(),
                                        Files.newInputStream(privateKeyPem)
                                )
                ).build();
        final var gcsSettingsProvider = new GcsSettingsProvider();
        final var repositoryMeta = new RepositoryMetaData("dummy_name", GcsBlobStoreRepository.TYPE, settings);
        final var repository =
                new GcsBlobStoreRepository(
                        repositoryMeta, mockedNamedXContentRegistry, mockedClusterService,
                        gcsSettingsProvider
                );
        assertEquals("some_bucket_name", repository.bucketName());
        assertEquals("custom/path/in/bucket/", repository.basePath().buildAsString());
        assertEquals(new ByteSizeValue(50, ByteSizeUnit.MB), repository.chunkSize());
    }

    @Test
    void throwsExceptionForWrongChunkSize() throws IOException {
        var settings = Settings.builder()
                .put(GcsBlobStoreRepository.BUCKET_NAME.getKey(), "some_bucket_name")
                .put(GcsBlobStoreRepository.CHUNK_SIZE.getKey(), -1, ByteSizeUnit.MB)
                .setSecureSettings(
                        new DummySecureSettings()
                                .setFile(
                                        EncryptionKeyProvider.PUBLIC_KEY_FILE.getKey(),
                                        Files.newInputStream(publicKeyPem)
                                )
                                .setFile(
                                        EncryptionKeyProvider.PRIVATE_KEY_FILE.getKey(),
                                        Files.newInputStream(privateKeyPem)
                                )
                ).build();

        final var gcsSettingsProvider = new GcsSettingsProvider();
        final var repositoryMeta = new RepositoryMetaData("dummy_name", GcsBlobStoreRepository.TYPE, settings);
        final var negativeChunkSizeRepository =
                new GcsBlobStoreRepository(
                        repositoryMeta, mockedNamedXContentRegistry, mockedClusterService,
                        gcsSettingsProvider
                );

        assertThrows(IllegalArgumentException.class, () -> negativeChunkSizeRepository.chunkSize());

        settings = Settings.builder()
                .put(GcsBlobStoreRepository.BUCKET_NAME.getKey(), "some_bucket_name")
                .put(GcsBlobStoreRepository.CHUNK_SIZE.getKey(), 0, ByteSizeUnit.MB)
                .setSecureSettings(
                        new DummySecureSettings()
                                .setFile(
                                        EncryptionKeyProvider.PUBLIC_KEY_FILE.getKey(),
                                        Files.newInputStream(publicKeyPem)
                                )
                                .setFile(
                                        EncryptionKeyProvider.PRIVATE_KEY_FILE.getKey(),
                                        Files.newInputStream(privateKeyPem)
                                )
                ).build();

        final var zeroChunkSizeRepository =
                new GcsBlobStoreRepository(
                        repositoryMeta, mockedNamedXContentRegistry, mockedClusterService,
                        gcsSettingsProvider
                );
        assertThrows(IllegalArgumentException.class, () -> zeroChunkSizeRepository.chunkSize());

        settings = Settings.builder()
                .put(GcsBlobStoreRepository.BUCKET_NAME.getKey(), "some_bucket_name")
                .put(GcsBlobStoreRepository.CHUNK_SIZE.getKey(), 100000, ByteSizeUnit.MB)
                .setSecureSettings(
                        new DummySecureSettings()
                                .setFile(
                                        EncryptionKeyProvider.PUBLIC_KEY_FILE.getKey(),
                                        Files.newInputStream(publicKeyPem)
                                )
                                .setFile(
                                        EncryptionKeyProvider.PRIVATE_KEY_FILE.getKey(),
                                        Files.newInputStream(privateKeyPem)
                                )
                ).build();

        final var biggerThanMaxChunkSizeRepository =
                new GcsBlobStoreRepository(
                        repositoryMeta, mockedNamedXContentRegistry, mockedClusterService,
                        gcsSettingsProvider
                );
        assertThrows(IllegalArgumentException.class, () -> biggerThanMaxChunkSizeRepository.chunkSize());
    }


    @Test
    void buildRepositoryWithDefaultParameters() throws IOException {
        final var settings = Settings.builder()
                .put(GcsBlobStoreRepository.BUCKET_NAME.getKey(), "some_bucket_name")
                .setSecureSettings(
                        new DummySecureSettings()
                                .setFile(
                                        EncryptionKeyProvider.PUBLIC_KEY_FILE.getKey(),
                                        Files.newInputStream(publicKeyPem)
                                )
                                .setFile(
                                        EncryptionKeyProvider.PRIVATE_KEY_FILE.getKey(),
                                        Files.newInputStream(privateKeyPem)
                                )
                ).build();

        final var gcsSettingsProvider = new GcsSettingsProvider();
        final var repositoryMeta = new RepositoryMetaData("dummy_name", GcsBlobStoreRepository.TYPE, settings);
        final var repository =
                new GcsBlobStoreRepository(
                        repositoryMeta, mockedNamedXContentRegistry, mockedClusterService,
                        gcsSettingsProvider
                );

        assertEquals("some_bucket_name", repository.bucketName());
        assertEquals(BlobPath.cleanPath().buildAsString(), repository.basePath().buildAsString());
        assertEquals(new ByteSizeValue(100, ByteSizeUnit.MB), repository.chunkSize());
    }

}
