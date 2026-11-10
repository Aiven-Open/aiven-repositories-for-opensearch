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

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.EncryptedBlobContainer;
import org.opensearch.common.collect.Tuple;

import io.aiven.elasticsearch.repositories.MixedFormatBlobContainer;
import io.aiven.elasticsearch.repositories.RepositoryBlobContainer;
import io.aiven.elasticsearch.repositories.RepositoryStorageIOProvider;
import io.aiven.elasticsearch.repositories.io.CryptoIOProvider;

import org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static io.aiven.elasticsearch.repositories.gcs.CryptoSettingsBlobIdentityTest.FRAME_LENGTH;
import static io.aiven.elasticsearch.repositories.gcs.CryptoSettingsBlobIdentityTest.PATH;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class MixedFormatBlobContainerTest {

    private static final CryptoIOProvider REPOSITORY_KEY_CRYPTO =
        new CryptoIOProvider(new SecretKeySpec(randomBytes(32), "AES"), 8192);

    private static final Map<String, byte[]> WRITTEN_WITH_REPOSITORY_KEY = Map.of(
        "before-large", randomBytes(2 * FRAME_LENGTH + 99),
        "before-small", randomBytes(10),
        "before-empty", new byte[0]);

    private static final Map<String, byte[]> WRITTEN_WITH_CRYPTO_SETTINGS = Map.of(
        "after-large", randomBytes(2 * FRAME_LENGTH + 4321),
        "after-empty", new byte[0]);

    private final Map<String, byte[]> bucket = new HashMap<>();

    private BlobContainer container;

    @BeforeAll
    static void addBouncyCastleFips() throws Exception {
        Security.addProvider(new BouncyCastleFipsProvider());
        CryptoSettingsBlobIdentityTest.generateKeys();
    }

    @BeforeEach
    void setUp() throws IOException {
        final var repositoryKeyContainer = new RepositoryBlobContainer(PATH, new InMemoryStorageIO(REPOSITORY_KEY_CRYPTO));
        for (final var blob : WRITTEN_WITH_REPOSITORY_KEY.entrySet()) {
            repositoryKeyContainer.writeBlob(
                blob.getKey(), new ByteArrayInputStream(blob.getValue()), blob.getValue().length, true);
        }

        final var storedContainer = new RepositoryBlobContainer(PATH, new InMemoryStorageIO(CryptoIOProvider.UNENCRYPTED));
        final var cryptoHandler = CryptoSettingsBlobIdentityTest.newNodeCryptoHandler();
        container = new MixedFormatBlobContainer(
            new EncryptedBlobContainer<>(storedContainer, cryptoHandler),
            storedContainer,
            repositoryKeyContainer,
            cryptoHandler,
            path -> {
                throw new UnsupportedOperationException();
            });
        for (final var blob : WRITTEN_WITH_CRYPTO_SETTINGS.entrySet()) {
            container.writeBlob(blob.getKey(), new ByteArrayInputStream(blob.getValue()), blob.getValue().length, true);
        }
    }

    @Test
    void readsBlobsOfBothFormats() throws IOException {
        for (final var blob : allBlobs().entrySet()) {
            try (final var in = container.readBlob(blob.getKey())) {
                assertArrayEquals(blob.getValue(), in.readAllBytes(), blob.getKey());
            }
        }
    }

    @Test
    void readsRangesOfBothFormats() throws IOException {
        for (final var name : List.of("before-large", "after-large")) {
            final var blob = allBlobs().get(name);
            for (final var range : List.of(
                new int[] {0, 1},
                new int[] {FRAME_LENGTH - 2, 4},
                new int[] {FRAME_LENGTH + 17, FRAME_LENGTH},
                new int[] {blob.length - 10, 10})) {
                try (final var in = container.readBlob(name, range[0], range[1])) {
                    assertArrayEquals(
                        Arrays.copyOfRange(blob, range[0], range[0] + range[1]),
                        in.readAllBytes(),
                        name + " " + Arrays.toString(range));
                }
            }
        }
    }

    @Test
    void writesOnlyCryptoSettingsFormat() throws IOException {
        final var stored = bucket.get(PATH.buildAsString() + "after-large");

        try (final var in = CryptoSettingsBlobIdentityTest.newNodeCryptoHandler()
            .createDecryptingStream(new ByteArrayInputStream(stored))) {
            assertArrayEquals(WRITTEN_WITH_CRYPTO_SETTINGS.get("after-large"), in.readAllBytes());
        }
    }

    @Test
    void listsPlaintextLengthForCryptoSettingsAndStoredLengthForRepositoryKey() throws IOException {
        final var expected = new HashMap<String, Long>();
        WRITTEN_WITH_CRYPTO_SETTINGS.forEach((name, bytes) -> expected.put(name, (long) bytes.length));
        WRITTEN_WITH_REPOSITORY_KEY.keySet()
            .forEach(name -> expected.put(name, (long) bucket.get(PATH.buildAsString() + name).length));

        final var listed = container.listBlobs().entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().length()));

        assertEquals(expected, listed);
    }

    private static Map<String, byte[]> allBlobs() {
        final var blobs = new HashMap<>(WRITTEN_WITH_REPOSITORY_KEY);
        blobs.putAll(WRITTEN_WITH_CRYPTO_SETTINGS);
        return blobs;
    }

    private static byte[] randomBytes(final int length) {
        final var bytes = new byte[length];
        new Random(length).nextBytes(bytes);
        return bytes;
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
                .filter(e -> e.getKey().startsWith(path + prefix))
                .collect(Collectors.toMap(e -> e.getKey().substring(path.length()), e -> (long) e.getValue().length));
        }
    }
}
