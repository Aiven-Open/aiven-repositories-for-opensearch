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
import java.nio.file.Files;

import io.aiven.elasticsearch.repositories.DummySecureSettings;
import io.aiven.elasticsearch.repositories.RsaKeyAwareTest;
import io.aiven.elasticsearch.repositories.metadata.EncryptedRepositoryMetadata;
import io.aiven.elasticsearch.repositories.security.EncryptionKeyProvider;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class GcsBlobStoreRepositoryTest extends RsaKeyAwareTest {

    @Mock
    NamedXContentRegistry mockedNamedXContentRegistry;

    @Mock
    ClusterService mockedClusterService;

    @Mock
    ClusterApplierService clusterApplierService;

    @Mock
    Storage storage;

    @Mock
    RecoverySettings mockedRecoverySettings;

    EncryptionKeyProvider encryptionKeyProvider;

    @BeforeEach
    void setUp() throws IOException {
        when(mockedClusterService.getClusterApplierService()).thenReturn(clusterApplierService);
        when(clusterApplierService.threadPool()).thenReturn(mock(ThreadPool.class));

        encryptionKeyProvider =
                EncryptionKeyProvider.of(
                        Files.newInputStream(publicKeyPem),
                        Files.newInputStream(privateKeyPem)
                );
    }

    @Test
    void shouldSaveNewEncryptionKey() throws Exception {
        final var gcsSettingsProvider = mock(GcsSettingsProvider.class);

        when(gcsSettingsProvider.gcsClient()).thenReturn(storage);
        when(gcsSettingsProvider.encryptionKeyProvider())
                .thenReturn(encryptionKeyProvider);

        final var settings = createValidRepositorySettings();
        final var repositoryMeta =
                new RepositoryMetadata("dummy_name", GcsBlobStoreRepository.TYPE, settings);
        final var repository =
                new GcsBlobStoreRepository(
                        repositoryMeta, mockedNamedXContentRegistry, mockedClusterService, mockedRecoverySettings,
                        gcsSettingsProvider
                );

        final var contentCaptor = ArgumentCaptor.forClass(byte[].class);
        final var blobCaptor = ArgumentCaptor.forClass(BlobInfo.class);

        repository.createBlobStore();

        verify(storage).create(blobCaptor.capture(), contentCaptor.capture());

        final var blobInfo = blobCaptor.getValue();
        assertEquals(
                "custom/path/in/bucket/" + GcsBlobStoreRepository.REPOSITORY_METADATA_FILE_NAME,
                blobInfo.getName()
        );
    }

    @Test
    void shouldRestorePreviouslyCreatedEncryptionKey() throws Exception {
        final var repositoryMetadataBlob = mock(Blob.class);
        final var gcsSettingsProvider = mock(GcsSettingsProvider.class);

        when(gcsSettingsProvider.gcsClient()).thenReturn(storage);
        when(gcsSettingsProvider.encryptionKeyProvider())
                .thenReturn(encryptionKeyProvider);
        when(storage.get(any(BlobId.class)))
                .thenReturn(repositoryMetadataBlob);
        when(repositoryMetadataBlob.exists())
                .thenReturn(true);

        final var encKey = encryptionKeyProvider.createKey();
        when(repositoryMetadataBlob.getContent())
                .thenReturn(new EncryptedRepositoryMetadata(encryptionKeyProvider).serialize(encKey));

        final var settings = createValidRepositorySettings();
        final var repositoryMeta =
                new RepositoryMetadata("dummy_name", GcsBlobStoreRepository.TYPE, settings);
        final var repository =
                new GcsBlobStoreRepository(
                        repositoryMeta, mockedNamedXContentRegistry, mockedClusterService, mockedRecoverySettings,
                        gcsSettingsProvider
                );

        final var blobIdCaptor = ArgumentCaptor.forClass(BlobId.class);
        repository.createBlobStore();

        verify(storage).get(blobIdCaptor.capture());

        final var blobId = blobIdCaptor.getValue();
        assertEquals(
                "custom/path/in/bucket/" + GcsBlobStoreRepository.REPOSITORY_METADATA_FILE_NAME,
                blobId.getName()
        );

    }

    private Settings createValidRepositorySettings() {
        return Settings.builder()
                .put(GcsBlobStoreRepository.BUCKET_NAME.getKey(), "some_bucket_name")
                .put(GcsBlobStoreRepository.BASE_PATH.getKey(), "custom/path/in/bucket")
                .build();
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
        final var repositoryMeta = new RepositoryMetadata("dummy_name", GcsBlobStoreRepository.TYPE, settings);
        final var repository =
                new GcsBlobStoreRepository(
                        repositoryMeta, mockedNamedXContentRegistry, mockedClusterService, mockedRecoverySettings,
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
                                        GcsStorageSettings.CREDENTIALS_FILE_SETTING.getKey(),
                                        getClass().getClassLoader().getResourceAsStream("test_gcs_creds.json")
                                )
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
        final var negativeChunkSizeRepository =
                new GcsBlobStoreRepository(
                        new RepositoryMetadata("dummy_name", GcsBlobStoreRepository.TYPE, settings),
                        mockedNamedXContentRegistry, mockedClusterService, mockedRecoverySettings,
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
                        new RepositoryMetadata("dummy_name", GcsBlobStoreRepository.TYPE, settings),
                        mockedNamedXContentRegistry, mockedClusterService, mockedRecoverySettings,
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
                        new RepositoryMetadata("dummy_name", GcsBlobStoreRepository.TYPE, settings),
                        mockedNamedXContentRegistry, mockedClusterService, mockedRecoverySettings,
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
        final var repositoryMeta = new RepositoryMetadata("dummy_name", GcsBlobStoreRepository.TYPE, settings);
        final var repository =
                new GcsBlobStoreRepository(
                        repositoryMeta, mockedNamedXContentRegistry, mockedClusterService, mockedRecoverySettings,
                        gcsSettingsProvider
                );

        assertEquals("some_bucket_name", repository.bucketName());
        assertEquals(BlobPath.cleanPath().buildAsString(), repository.basePath().buildAsString());
        assertEquals(new ByteSizeValue(100, ByteSizeUnit.MB), repository.chunkSize());
    }

}
