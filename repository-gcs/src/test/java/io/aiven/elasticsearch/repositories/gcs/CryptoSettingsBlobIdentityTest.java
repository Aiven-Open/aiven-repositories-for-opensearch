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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.EncryptedBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.crypto.CryptoHandler;

import io.aiven.elasticsearch.crypto.cipher.AwsMessageFormatCryptoPlugin;
import io.aiven.elasticsearch.crypto.provider.RsaMasterKeyProvider;
import io.aiven.elasticsearch.repositories.RepositoryBlobContainer;
import io.aiven.elasticsearch.repositories.RepositoryStorageIOProvider;
import io.aiven.elasticsearch.repositories.io.CryptoIOProvider;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A blob that core encrypts for a repository with crypto_settings, stored through aiven-gcs, must read back through
 * core over a plain blob container, the way upstream repository-gcs reads it.
 */
class CryptoSettingsBlobIdentityTest {

    static final int FRAME_LENGTH = 1024 * 1024;

    static final BlobPath PATH = BlobPath.cleanPath().add("indices").add("uuid");

    static KeyPair keyPair;

    @TempDir
    Path directory;

    @BeforeAll
    static void generateKeys() throws Exception {
        final var generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        keyPair = generator.generateKeyPair();
    }

    static CryptoHandler<?, ?> newNodeCryptoHandler() {
        return new AwsMessageFormatCryptoPlugin()
            .getOrCreateCryptoHandler(new RsaMasterKeyProvider(keyPair), "default", "aiven-rsa", () -> { });
    }

    @Test
    void blobStoredThroughAivenGcsReadsBackThroughPlainBlobContainer() throws IOException {
        final var blobs = Map.of(
            "large", randomBytes(2 * FRAME_LENGTH + 4321),
            "frames", randomBytes(FRAME_LENGTH),
            "empty", new byte[0]);
        final var bucket = new HashMap<String, byte[]>();
        final BlobContainer aivenContainer = new EncryptedBlobContainer<>(
            new RepositoryBlobContainer(PATH, new InMemoryStorageIO(bucket)), newNodeCryptoHandler());
        for (final var blob : blobs.entrySet()) {
            aivenContainer.writeBlob(blob.getKey(), new ByteArrayInputStream(blob.getValue()), blob.getValue().length, true);
        }

        final var blobDirectory = Files.createDirectories(directory.resolve("indices").resolve("uuid"));
        for (final var stored : bucket.entrySet()) {
            Files.write(blobDirectory.resolve(stored.getKey().substring(PATH.buildAsString().length())), stored.getValue());
        }
        try (final var fsBlobStore = new FsBlobStore(8192, directory, true)) {
            final BlobContainer upstreamContainer =
                new EncryptedBlobContainer<>(fsBlobStore.blobContainer(PATH), newNodeCryptoHandler());

            assertEquals(plaintextLengths(blobs), listedLengths(aivenContainer.listBlobs()));
            assertEquals(plaintextLengths(blobs), listedLengths(upstreamContainer.listBlobs()));
            for (final var blob : blobs.entrySet()) {
                try (final var in = upstreamContainer.readBlob(blob.getKey())) {
                    assertArrayEquals(blob.getValue(), in.readAllBytes(), blob.getKey());
                }
                try (final var in = aivenContainer.readBlob(blob.getKey())) {
                    assertArrayEquals(blob.getValue(), in.readAllBytes(), blob.getKey());
                }
            }
            final var large = blobs.get("large");
            for (final var range : List.of(
                new int[] {0, 1},
                new int[] {FRAME_LENGTH - 2, 4},
                new int[] {FRAME_LENGTH + 17, FRAME_LENGTH},
                new int[] {large.length - 10, 10},
                new int[] {0, large.length})) {
                final var expected = Arrays.copyOfRange(large, range[0], range[0] + range[1]);
                try (final var in = upstreamContainer.readBlob("large", range[0], range[1])) {
                    assertArrayEquals(expected, in.readAllBytes(), "upstream range " + Arrays.toString(range));
                }
                try (final var in = aivenContainer.readBlob("large", range[0], range[1])) {
                    assertArrayEquals(expected, in.readAllBytes(), "aiven-gcs range " + Arrays.toString(range));
                }
            }
        }
    }

    private static byte[] randomBytes(final int length) {
        final var bytes = new byte[length];
        new Random(length).nextBytes(bytes);
        return bytes;
    }

    private static Map<String, Long> plaintextLengths(final Map<String, byte[]> blobs) {
        return blobs.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> (long) e.getValue().length));
    }

    private static Map<String, Long> listedLengths(final Map<String, BlobMetadata> blobs) {
        return blobs.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().length()));
    }

    /**
     * Stores bytes the way {@code GcsStorageIO} does for a repository with crypto_settings: through
     * {@link CryptoIOProvider#UNENCRYPTED}, with ranged reads of the raw object. Every write first runs once and
     * rewinds, as a retried resumable upload does.
     */
    private static final class InMemoryStorageIO implements RepositoryStorageIOProvider.StorageIO {

        private final Map<String, byte[]> bucket;

        InMemoryStorageIO(final Map<String, byte[]> bucket) {
            this.bucket = bucket;
        }

        @Override
        public boolean exists(final String blobName) {
            return bucket.containsKey(blobName);
        }

        @Override
        public InputStream read(final String blobName) throws IOException {
            return CryptoIOProvider.UNENCRYPTED.decryptAndDecompress(new ByteArrayInputStream(bucket.get(blobName)));
        }

        @Override
        public InputStream read(final String blobName, final long position, final long length) {
            return new ByteArrayInputStream(bucket.get(blobName), (int) position, (int) length);
        }

        @Override
        public void write(final String blobName,
                          final InputStream inputStream,
                          final long blobSize,
                          final boolean failIfAlreadyExists) throws IOException {
            inputStream.mark(Integer.MAX_VALUE);
            CryptoIOProvider.UNENCRYPTED.compressAndEncrypt(inputStream, OutputStream.nullOutputStream());
            inputStream.reset();
            final var out = new ByteArrayOutputStream();
            CryptoIOProvider.UNENCRYPTED.compressAndEncrypt(inputStream, out);
            assertEquals(blobSize, out.size(), blobName);
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
