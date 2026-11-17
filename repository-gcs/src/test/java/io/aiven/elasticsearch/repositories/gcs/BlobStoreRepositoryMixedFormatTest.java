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

import javax.crypto.spec.SecretKeySpec;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.Security;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.opensearch.cluster.metadata.CryptoMetadata;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.service.ClusterApplierService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.crypto.MasterKeyProvider;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.crypto.CryptoHandlerRegistry;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.plugins.CryptoKeyProviderPlugin;

import io.aiven.elasticsearch.crypto.cipher.AwsMessageFormatCryptoPlugin;
import io.aiven.elasticsearch.crypto.provider.RsaMasterKeyProvider;
import io.aiven.elasticsearch.repositories.BlobStoreRepository;
import io.aiven.elasticsearch.repositories.CommonSettings;
import io.aiven.elasticsearch.repositories.RepositorySettingsService;
import io.aiven.elasticsearch.repositories.RepositoryStorageIOProvider;
import io.aiven.elasticsearch.repositories.io.CryptoIOProvider;

import org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class BlobStoreRepositoryMixedFormatTest {

    private static final String BASE_PATH = "base/";

    private static final CryptoIOProvider REPOSITORY_KEY_CRYPTO =
        new CryptoIOProvider(new SecretKeySpec(randomBytes(32), "AES"), 8192);

    private static final byte[] BEFORE = randomBytes(1000);

    private static final byte[] AFTER = randomBytes(2000);

    private final Map<String, byte[]> bucket = new HashMap<>();

    @BeforeAll
    static void initCryptoRegistry() throws Exception {
        Security.addProvider(new BouncyCastleFipsProvider());
        CryptoSettingsBlobIdentityTest.generateKeys();
        CryptoHandlerRegistry.initRegistry(
            List.of(new AwsMessageFormatCryptoPlugin()), List.of(new TestKeyProviderPlugin()), Settings.EMPTY);
    }

    @BeforeEach
    void writeBlobWithRepositoryKey() throws IOException {
        new InMemoryStorageIO(REPOSITORY_KEY_CRYPTO)
            .write(BASE_PATH + "before", new ByteArrayInputStream(BEFORE), BEFORE.length, true);
    }

    @Test
    void readsBlobsWrittenWithRepositoryKeyAndWritesThroughCryptoSettings() throws IOException {
        final var container = blobContainer();

        try (final var in = container.readBlob("before")) {
            assertArrayEquals(BEFORE, in.readAllBytes());
        }

        container.writeBlob("after", new ByteArrayInputStream(AFTER), AFTER.length, true);

        try (final var in = CryptoSettingsBlobIdentityTest.newNodeCryptoHandler()
            .createDecryptingStream(new ByteArrayInputStream(bucket.get(BASE_PATH + "after")))) {
            assertArrayEquals(AFTER, in.readAllBytes());
        }
        try (final var in = container.readBlob("after")) {
            assertArrayEquals(AFTER, in.readAllBytes());
        }
    }

    private BlobContainer blobContainer() {
        final var clusterService = mock(ClusterService.class);
        when(clusterService.getClusterApplierService()).thenReturn(mock(ClusterApplierService.class));
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
        when(clusterService.getClusterSettings())
            .thenReturn(new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
        final var metadata = new RepositoryMetadata(
            "repo",
            "aiven-gcs",
            Settings.builder().put(CommonSettings.RepositorySettings.BASE_PATH.getKey(), "base").build(),
            new CryptoMetadata("default", "aiven-rsa", Settings.EMPTY));
        final var repository = new BlobStoreRepository<>(
            metadata, NamedXContentRegistry.EMPTY, clusterService, mock(RecoverySettings.class), new TestSettingsService());
        repository.start();
        return repository.blobStore().blobContainer(repository.basePath());
    }

    private static byte[] randomBytes(final int length) {
        final var bytes = new byte[length];
        new Random(length).nextBytes(bytes);
        return bytes;
    }

    private static final class TestKeyProviderPlugin implements CryptoKeyProviderPlugin {

        @Override
        public MasterKeyProvider createKeyProvider(final CryptoMetadata cryptoMetadata) {
            return new RsaMasterKeyProvider(CryptoSettingsBlobIdentityTest.keyPair);
        }

        @Override
        public String type() {
            return "aiven-rsa";
        }
    }

    private final class TestSettingsService extends RepositorySettingsService<Object, GcsClientSettings> {

        @Override
        public RepositoryStorageIOProvider.StorageIO createStorageIO(final String basePath,
                                                                     final String repositoryName,
                                                                     final Settings repositorySettings,
                                                                     final boolean clear,
                                                                     final boolean hasCryptoSettings) {
            return new InMemoryStorageIO(CryptoIOProvider.UNENCRYPTED);
        }

        @Override
        public RepositoryStorageIOProvider.StorageIO createRepositoryKeyStorageIO(final String basePath,
                                                                                  final String repositoryName,
                                                                                  final Settings repositorySettings) {
            return new InMemoryStorageIO(REPOSITORY_KEY_CRYPTO);
        }

        @Override
        protected boolean supportsCryptoSettings() {
            return true;
        }

        @Override
        protected Map<String, GcsClientSettings> getEffectiveClientSettings(final Settings settings) {
            return Map.of();
        }

        @Override
        protected RepositoryStorageIOProvider<Object, GcsClientSettings> createRepositoryStorageIOProvider() {
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
        public InputStream read(final String blobName, final long position, final long length) throws IOException {
            try (final var in = read(blobName)) {
                in.skipNBytes(position);
                return new ByteArrayInputStream(in.readNBytes(Math.toIntExact(length)));
            }
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
