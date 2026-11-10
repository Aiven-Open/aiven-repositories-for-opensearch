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

package io.aiven.elasticsearch.repositories;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.opensearch.cluster.metadata.CryptoMetadata;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.service.ClusterApplierService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.repositories.RepositoryException;

import io.aiven.elasticsearch.repositories.io.CryptoIOProvider;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static io.aiven.elasticsearch.repositories.RepositoryStorageIOProvider.REPOSITORY_METADATA_FILE_NAME;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RepositoryStorageIOProviderTest extends RsaKeyAwareTest {

    private static final String BASE_PATH = "base/";

    private static final byte[] CONTENT = "some content".getBytes(StandardCharsets.UTF_8);

    private final Map<String, byte[]> bucket = new ConcurrentHashMap<>();

    private TestClientSettings clientSettings;

    @BeforeEach
    void setUp() throws IOException {
        clientSettings = new TestClientSettings(Files.readAllBytes(publicKeyPem), Files.readAllBytes(privateKeyPem));
    }

    @Test
    void withCryptoSettingsWritesBytesAsIsAndSkipsRepositoryMetadata() throws IOException {
        final var storageIO = new TestStorageIOProvider(false)
            .createStorageIO(BASE_PATH, clientSettings, Settings.EMPTY, false, true);

        storageIO.write(BASE_PATH + "blob", new ByteArrayInputStream(CONTENT), CONTENT.length, false);

        assertFalse(bucket.containsKey(BASE_PATH + REPOSITORY_METADATA_FILE_NAME));
        assertArrayEquals(CONTENT, bucket.get(BASE_PATH + "blob"));
    }

    @Test
    void refusesNewRepositoryWithoutCryptoSettings() {
        final var provider = new TestStorageIOProvider(false);

        assertThrows(IllegalArgumentException.class,
            () -> provider.createStorageIO(BASE_PATH, clientSettings, Settings.EMPTY, false, false));
        assertTrue(bucket.isEmpty());
    }

    @Test
    void createsEncryptionKeyForNewRepositoryWhenAllowed() throws IOException {
        new TestStorageIOProvider(true).createStorageIO(BASE_PATH, clientSettings, Settings.EMPTY, false, false);

        assertTrue(bucket.containsKey(BASE_PATH + REPOSITORY_METADATA_FILE_NAME));
    }

    @Test
    void readsExistingRepositoryWhenKeyCreationIsDisabled() throws IOException {
        final var writer = new TestStorageIOProvider(true)
            .createStorageIO(BASE_PATH, clientSettings, Settings.EMPTY, false, false);
        writer.write(BASE_PATH + "blob", new ByteArrayInputStream(CONTENT), CONTENT.length, false);

        final var reader = new TestStorageIOProvider(false)
            .createStorageIO(BASE_PATH, clientSettings, Settings.EMPTY, false, false);
        try (final var in = reader.read(BASE_PATH + "blob")) {
            assertArrayEquals(CONTENT, in.readAllBytes());
        }
    }

    @Test
    void blobStoreRepositoryRefusesCryptoSettingsWhenTypeDoesNotSupportThem() {
        final var clusterService = mock(ClusterService.class);
        when(clusterService.getClusterApplierService()).thenReturn(mock(ClusterApplierService.class));
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
        when(clusterService.getClusterSettings())
            .thenReturn(new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
        final var metadata = new RepositoryMetadata(
            "repo", "aiven-test", Settings.EMPTY, new CryptoMetadata("default", "aiven-rsa", Settings.EMPTY));

        assertThrows(RepositoryException.class, () -> new BlobStoreRepository<>(
            metadata,
            NamedXContentRegistry.EMPTY,
            clusterService,
            mock(RecoverySettings.class),
            new TestSettingsService()));
    }

    private static final class TestClientSettings implements CommonSettings.ClientSettings {

        private final byte[] publicKey;

        private final byte[] privateKey;

        TestClientSettings(final byte[] publicKey, final byte[] privateKey) {
            this.publicKey = publicKey;
            this.privateKey = privateKey;
        }

        @Override
        public byte[] publicKey() {
            return publicKey;
        }

        @Override
        public byte[] privateKey() {
            return privateKey;
        }
    }

    private static final class TestClientProvider extends ClientProvider<Object, TestClientSettings> {

        @Override
        protected void closeClient() {
        }

        @Override
        protected Object buildClient(final TestClientSettings clientSettings, final Settings repositorySettings) {
            return new Object();
        }
    }

    private final class TestStorageIOProvider extends RepositoryStorageIOProvider<Object, TestClientSettings> {

        private final boolean createsEncryptionKeyIfMissing;

        TestStorageIOProvider(final boolean createsEncryptionKeyIfMissing) {
            super(new TestClientProvider());
            this.createsEncryptionKeyIfMissing = createsEncryptionKeyIfMissing;
        }

        @Override
        protected boolean createsEncryptionKeyIfMissing() {
            return createsEncryptionKeyIfMissing;
        }

        @Override
        protected StorageIO createStorageIOFor(final Object client,
                                               final Settings repositorySettings,
                                               final CryptoIOProvider cryptoIOProvider) {
            return new InMemoryStorageIO(cryptoIOProvider);
        }
    }

    private static final class TestSettingsService extends RepositorySettingsService<Object, TestClientSettings> {

        @Override
        protected Map<String, TestClientSettings> getEffectiveClientSettings(final Settings settings) {
            return Map.of();
        }

        @Override
        protected RepositoryStorageIOProvider<Object, TestClientSettings> createRepositoryStorageIOProvider() {
            throw new UnsupportedOperationException();
        }
    }

    private final class InMemoryStorageIO implements RepositoryStorageIOProvider.StorageIO {

        private final CryptoIOProvider cryptoIOProvider;

        InMemoryStorageIO(final CryptoIOProvider cryptoIOProvider) {
            this.cryptoIOProvider = cryptoIOProvider;
        }

        @Override
        public boolean exists(final String blobName) {
            return bucket.containsKey(blobName);
        }

        @Override
        public InputStream read(final String blobName) throws IOException {
            return cryptoIOProvider.decryptAndDecompress(new ByteArrayInputStream(bucket.get(blobName)));
        }

        @Override
        public void write(final String blobName,
                          final InputStream inputStream,
                          final long blobSize,
                          final boolean failIfAlreadyExists) throws IOException {
            final var out = new ByteArrayOutputStream();
            cryptoIOProvider.compressAndEncrypt(inputStream, out);
            bucket.put(blobName, out.toByteArray());
        }

        @Override
        public Tuple<Integer, Long> deleteDirectories(final String path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteFiles(final List<String> blobNames, final boolean ignoreIfNotExists) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> listDirectories(final String path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, Long> listFiles(final String path, final String prefix) {
            throw new UnsupportedOperationException();
        }
    }
}
