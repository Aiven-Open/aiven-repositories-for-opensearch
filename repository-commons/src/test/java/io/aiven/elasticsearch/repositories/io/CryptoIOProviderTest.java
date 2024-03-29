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

package io.aiven.elasticsearch.repositories.io;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

import org.opensearch.core.internal.io.Streams;

import io.aiven.elasticsearch.repositories.RsaKeyAwareTest;
import io.aiven.elasticsearch.repositories.security.EncryptionKeyProvider;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

public class CryptoIOProviderTest extends RsaKeyAwareTest {

    private static final int BUFFER_SIZE = 8_192;

    private static final int MESSAGE_AMOUNT = 1_000;

    private EncryptionKeyProvider encProvider;

    private CryptoIOProvider cryptoIOProvider;

    @BeforeEach
    public void setUpKey() throws Exception {
        encProvider =
                EncryptionKeyProvider.of(
                        Files.newInputStream(publicKeyPem).readAllBytes(),
                        Files.newInputStream(privateKeyPem).readAllBytes()
                );
        final var encryptionKey = encProvider.createKey();
        cryptoIOProvider = new CryptoIOProvider(encryptionKey, BUFFER_SIZE);
    }

    @Test
    public void compressAndEncryptStream(@TempDir final Path tmpFolder) throws Exception {

        final var random = new Random();

        final var testFile = tmpFolder.resolve("original_file");
        final var encryptedFile = tmpFolder.resolve("encrypted_file");
        final var decryptedFile = tmpFolder.resolve("decrypted_file");

        final var message = new byte[BUFFER_SIZE];

        final var expectedBytes = ByteBuffer.allocate(BUFFER_SIZE * MESSAGE_AMOUNT);
        try (final var fin = Files.newOutputStream(testFile)) {
            for (int i = 0; i < MESSAGE_AMOUNT; i++) {
                random.nextBytes(message);
                fin.write(message);
                expectedBytes.put(message);
            }
            fin.flush();
        }

        try (final var in = Files.newInputStream(testFile);
             final var encryptedFileOut = Files.newOutputStream(encryptedFile)) {
            cryptoIOProvider.compressAndEncrypt(in, encryptedFileOut);
        }

        try (final var encryptedFileStream = Files.newInputStream(encryptedFile);
             final var out = Files.newOutputStream(decryptedFile)) {
            Streams.copy(cryptoIOProvider.decryptAndDecompress(encryptedFileStream), out);
        }

        final var decryptedBytes = ByteBuffer.allocate(BUFFER_SIZE * MESSAGE_AMOUNT);
        try (final var in = Files.newInputStream(decryptedFile)) {
            final var buffer = new byte[8192];
            while (in.read(buffer) != -1) {
                decryptedBytes.put(buffer);
            }
        }

        assertArrayEquals(expectedBytes.array(), decryptedBytes.array());
    }

}
