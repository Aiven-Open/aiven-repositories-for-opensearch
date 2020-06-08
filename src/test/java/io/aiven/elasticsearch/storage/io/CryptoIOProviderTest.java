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

package io.aiven.elasticsearch.storage.io;

import java.io.ByteArrayInputStream;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.zip.CRC32;

import io.aiven.elasticsearch.storage.RsaKeyAwareTest;
import io.aiven.elasticsearch.storage.security.Encryption;
import io.aiven.elasticsearch.storage.security.EncryptionKeyProvider;

import com.google.common.io.ByteStreams;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class CryptoIOProviderTest extends RsaKeyAwareTest implements Encryption {

    private static final int BUFFER_SIZE = 8192;

    private EncryptionKeyProvider encProvider;

    private CryptoIOProvider cryptoIOProvider;

    private SecureRandom secureRandom;

    @BeforeEach
    public void setUpKey() throws Exception {
        encProvider =
                EncryptionKeyProvider.of(
                        Files.newInputStream(publicKeyPem),
                        Files.newInputStream(privateKeyPem)
                );
        final var encryptionKey = encProvider.createKey();
        cryptoIOProvider = new CryptoIOProvider(encryptionKey);
        secureRandom = SecureRandom.getInstanceStrong();
    }

    @Test
    public void compressAndEncryptSmallStream() throws Exception {
        final var message = new byte[BUFFER_SIZE];
        secureRandom.nextBytes(message);

        final var compressedAndEncryptedBytes =
                cryptoIOProvider.compressAndEncrypt(new ByteArrayInputStream(message));

        try (final var decompressedAndDecryptedMessage =
                     cryptoIOProvider.decryptAndDecompress(compressedAndEncryptedBytes)) {
            final var decryptedAndDecompressedStream = decompressedAndDecryptedMessage.readAllBytes();
            assertArrayEquals(
                    message,
                    decryptedAndDecompressedStream
            );
        }
    }

    @Test
    public void compressAndEncryptLargeStream(@TempDir final Path tmpFolder) throws Exception {
        final var testFile = tmpFolder.resolve("original_file");
        final var encryptedFile = tmpFolder.resolve("encrypted_file");
        final var decryptedFile = tmpFolder.resolve("decrypted_file");

        final var message = new byte[BUFFER_SIZE];

        final var expectedChecksum = new CRC32();
        try (final var fin = Files.newOutputStream(testFile)) {
            for (int i = 0; i < 1_000; i++) {
                secureRandom.nextBytes(message);
                fin.write(message);
                expectedChecksum.update(message);
            }
            fin.flush();
        }

        try (final var in = Files.newInputStream(testFile);
             final var encryptedFileOut = Files.newOutputStream(encryptedFile);
             final var writeChannel = Channels.newChannel(encryptedFileOut)) {
            cryptoIOProvider.compressAndEncrypt(in, writeChannel);
        }

        try (final var encryptedFileStream = Files.newInputStream(encryptedFile);
             final var readChannel = Channels.newChannel(encryptedFileStream);
             final var out = Files.newOutputStream(decryptedFile)) {
            ByteStreams.copy(cryptoIOProvider.decryptAndDecompress(readChannel), out);
        }

        final var resultCrc32 = new CRC32();
        try (final var in = Files.newInputStream(decryptedFile)) {
            final var buffer = new byte[8192];
            while (in.read(buffer) != -1) {
                resultCrc32.update(buffer);
            }
        }

        assertEquals(expectedChecksum.getValue(), resultCrc32.getValue());
    }

}
