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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.service.ClusterApplierService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.crypto.CryptoHandler;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.indices.recovery.RecoverySettings;

import io.aiven.elasticsearch.repositories.BlobStoreRepository;
import io.aiven.elasticsearch.repositories.CommonSettings;
import io.aiven.elasticsearch.repositories.RepositoryStorageIOProvider;
import io.aiven.elasticsearch.repositories.io.CryptoIOProvider;

import com.google.cloud.storage.Storage;
import org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static io.aiven.elasticsearch.repositories.RepositoryStorageIOProvider.REPOSITORY_METADATA_FILE_NAME;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class BlobStoreRepositoryV2Test {

    private static final String BASE_PATH = "base/";

    private static final CryptoIOProvider REPOSITORY_KEY_CRYPTO =
        new CryptoIOProvider(new SecretKeySpec(randomBytes(32), "AES"), 8192);

    private static final byte[] BEFORE = randomBytes(1000);

    private static final byte[] AFTER = randomBytes(2 * CryptoSettingsBlobIdentityTest.FRAME_LENGTH + 77);

    private static final int[] RANGE = {CryptoSettingsBlobIdentityTest.FRAME_LENGTH - 3, 40};

    private final Map<String, byte[]> bucket = new HashMap<>();

    @BeforeAll
    static void setUpCrypto() throws Exception {
        Security.addProvider(new BouncyCastleFipsProvider());
        CryptoSettingsBlobIdentityTest.generateKeys();
    }

    @Test
    void flagOffReadsAndWritesVersion1() throws IOException {
        writeVersion1Blob();
        final var container = blobContainer(false);

        assertReads(container, "before", BEFORE);
        container.writeBlob("after", new ByteArrayInputStream(AFTER), AFTER.length, true);

        try (final var in = REPOSITORY_KEY_CRYPTO.decryptAndDecompress(new ByteArrayInputStream(bucket.get(BASE_PATH + "after")))) {
            assertArrayEquals(AFTER, in.readAllBytes());
        }
        assertReads(container, "after", AFTER);
    }

    @Test
    void flagOnReadsVersion1AndWritesFormatV2() throws IOException {
        writeVersion1Blob();
        final var container = blobContainer(true);

        assertReads(container, "before", BEFORE);
        container.writeBlob("after", new ByteArrayInputStream(AFTER), AFTER.length, true);

        try (final var in = AivenBlobContainerV2Test.newNodeCryptoHandler()
            .createDecryptingStream(new ByteArrayInputStream(bucket.get(BASE_PATH + "after")))) {
            assertArrayEquals(AFTER, in.readAllBytes());
        }
        assertReads(container, "after", AFTER);
        assertEquals(
            Map.of("before", (long) bucket.get(BASE_PATH + "before").length, "after", (long) AFTER.length),
            container.listBlobs().entrySet().stream()
                .filter(e -> !e.getKey().equals(REPOSITORY_METADATA_FILE_NAME))
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().length())));
    }

    @Test
    void flagOnWritesFormatV2WithoutRepositoryMetadataOnEmptyPath() throws IOException {
        final var container = blobContainer(true);

        container.writeBlob("after", new ByteArrayInputStream(AFTER), AFTER.length, true);

        assertFalse(bucket.containsKey(BASE_PATH + REPOSITORY_METADATA_FILE_NAME));
        assertArrayEquals(new byte[] {0x02, 0x04, 0x78}, Arrays.copyOf(bucket.get(BASE_PATH + "after"), 3));
        assertReads(container, "after", AFTER);
        assertEquals(AFTER.length, container.listBlobs().get("after").length());
    }

    @Test
    void flagOffReadsFormatV2() throws IOException {
        writeVersion1Blob();
        blobContainer(true).writeBlob("after", new ByteArrayInputStream(AFTER), AFTER.length, true);

        final var container = blobContainer(false);

        assertReads(container, "before", BEFORE);
        assertReads(container, "after", AFTER);
    }

    private void writeVersion1Blob() throws IOException {
        bucket.put(BASE_PATH + REPOSITORY_METADATA_FILE_NAME, new byte[0]);
        new InMemoryStorageIO(REPOSITORY_KEY_CRYPTO)
            .write(BASE_PATH + "before", new ByteArrayInputStream(BEFORE), BEFORE.length, true);
    }

    private static void assertReads(final BlobContainer container, final String name, final byte[] expected)
        throws IOException {
        try (final var in = container.readBlob(name)) {
            assertArrayEquals(expected, in.readAllBytes(), name);
        }
        final var position = Math.min(RANGE[0], expected.length - RANGE[1]);
        try (final var in = container.readBlob(name, position, RANGE[1])) {
            assertArrayEquals(Arrays.copyOfRange(expected, position, position + RANGE[1]), in.readAllBytes(), name + " range");
        }
    }

    private BlobContainer blobContainer(final boolean encryptionFormatV2) {
        final var clusterService = mock(ClusterService.class);
        when(clusterService.getClusterApplierService()).thenReturn(mock(ClusterApplierService.class));
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
        when(clusterService.getClusterSettings())
            .thenReturn(new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
        final var metadata = new RepositoryMetadata(
            "repo",
            "aiven-gcs",
            Settings.builder()
                .put(CommonSettings.RepositorySettings.BASE_PATH.getKey(), "base")
                .put(CommonSettings.RepositorySettings.ENCRYPTION_FORMAT_V2.getKey(), encryptionFormatV2)
                .build());
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

    private final class TestSettingsService extends GcsSettingsProvider {

        @Override
        public RepositoryStorageIOProvider.StorageIO createStorageIO(final String basePath,
                                                                     final String repositoryName,
                                                                     final Settings repositorySettings,
                                                                     final boolean clear) {
            bucket.putIfAbsent(basePath + REPOSITORY_METADATA_FILE_NAME, new byte[0]);
            return new InMemoryStorageIO(REPOSITORY_KEY_CRYPTO);
        }

        @Override
        public RepositoryStorageIOProvider.StorageIO createStoredStorageIO(final String repositoryName,
                                                                           final Settings repositorySettings,
                                                                           final boolean clear) {
            return new InMemoryStorageIO(CryptoIOProvider.UNENCRYPTED);
        }

        @Override
        public RepositoryStorageIOProvider.StorageIO createRepositoryKeyStorageIO(final String basePath,
                                                                                  final String repositoryName,
                                                                                  final Settings repositorySettings) {
            return bucket.containsKey(basePath + REPOSITORY_METADATA_FILE_NAME)
                ? new InMemoryStorageIO(REPOSITORY_KEY_CRYPTO)
                : null;
        }

        @Override
        public CryptoHandler<?, ?> createCryptoHandler(final String repositoryName, final Settings repositorySettings) {
            return createCryptoHandler(
                CryptoSettingsBlobIdentityTest.keyPair, CommonSettings.RepositorySettings.KEY_PROVIDER_NAME.get(repositorySettings));
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
            return bucket.entrySet().stream()
                .filter(e -> e.getKey().startsWith(path + prefix) && e.getKey().indexOf('/', path.length()) < 0)
                .collect(Collectors.toMap(e -> e.getKey().substring(path.length()), e -> (long) e.getValue().length));
        }
    }
}
