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

import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.security.SecureRandom;
import java.util.Arrays;

import io.aiven.elasticsearch.storage.RsaKeyAwareTest;
import io.aiven.elasticsearch.storage.security.Decryption;
import io.aiven.elasticsearch.storage.security.Encryption;
import io.aiven.elasticsearch.storage.security.EncryptionKeyProvider;

import com.google.common.hash.Hashing;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class EncryptionOutputStreamTest
        extends RsaKeyAwareTest
        implements Decryption {

    private EncryptionKeyProvider encProvider;

    private SecureRandom secureRandom;

    @BeforeEach
    public void setRsaUpKey() throws Exception {
        encProvider =
                EncryptionKeyProvider.of(
                        Files.newInputStream(publicKeyPem),
                        Files.newInputStream(privateKeyPem)
                );
        secureRandom = SecureRandom.getInstanceStrong();
    }

    @Test
    public void skipZeroBytes() throws Exception {
        final var encKey = encProvider.createKey();
        final var message = new byte[0];
        secureRandom.nextBytes(message);
        final var outBytes = encryptMessage(encKey, message);

        assertEquals(0, outBytes.length);
    }

    @ParameterizedTest
    @ValueSource(ints = {500, 10, 100, 1000})
    public void encryptMessages(final int messageSize) throws Exception {
        final var encKey = encProvider.createKey();
        final var message = new byte[messageSize];
        secureRandom.nextBytes(message);

        final var outBytes = encryptMessage(encKey, message);
        assertArrayEquals(message, extractMessage(encKey, outBytes));
    }

    @Test
    void encryptMessageSmallerThanBufferSize() throws Exception {
        final var smallMessageSize = 10;
        final var encKey = encProvider.createKey();

        final var message = new byte[EncryptionOutputStream.encryptedPackageSize()];
        secureRandom.nextBytes(message);

        final var smallMessage = new byte[message.length];
        Arrays.fill(smallMessage, (byte) 0);
        System.arraycopy(message, 0, smallMessage, 0, smallMessageSize);

        try (final var outBytes = new ByteArrayOutputStream()) {
            try (final var writer = new EncryptionOutputStream(outBytes, encKey)) {
                writer.write(smallMessage, 0, smallMessageSize);
            }
            assertArrayEquals(
                    Arrays.copyOfRange(smallMessage, 0, smallMessageSize),
                    extractMessage(encKey, outBytes.toByteArray())
            );
        }

    }

    @Test
    void writeRemainingBytes() throws Exception {
        final var encKey = encProvider.createKey();

        final var message = new byte[EncryptionOutputStream.ENCRYPTION_CHUNK_SIZE + 100];
        secureRandom.nextBytes(message);

        final var outBytes = encryptMessage(encKey, message);

        final var encryptedBuffer = ByteBuffer.wrap(outBytes).order(ByteOrder.LITTLE_ENDIAN);
        final var encryptedChunkSize = EncryptionOutputStream.encryptedPackageSize();

        final var encryptedMessage = new byte[encryptedChunkSize];
        final var decryptedByeBuffer = ByteBuffer.allocate(EncryptionOutputStream.ENCRYPTION_CHUNK_SIZE + 100);
        encryptedBuffer.get(encryptedMessage);
        decryptedByeBuffer.put(
                extractMessage(
                        encKey,
                        ByteBuffer.wrap(encryptedMessage).order(ByteOrder.LITTLE_ENDIAN)
                )
        );

        final var remainingMessage = new byte[encryptedBuffer.remaining()];
        encryptedBuffer.get(remainingMessage);
        decryptedByeBuffer.put(
                extractMessage(
                        encKey,
                        ByteBuffer.wrap(remainingMessage).order(ByteOrder.LITTLE_ENDIAN)
                )
        );

        assertFalse(encryptedBuffer.hasRemaining());
        assertArrayEquals(message, decryptedByeBuffer.array());
    }

    private byte[] encryptMessage(final SecretKey encKey,
                                  final byte[] message) throws IOException {
        final var outBytes = new ByteArrayOutputStream();
        try (final var writer = new EncryptionOutputStream(outBytes, encKey)) {
            writer.write(message, 0, message.length);
        }
        return outBytes.toByteArray();
    }

    private byte[] extractMessage(final SecretKey key,
                                  final byte[] encryptedMessage) throws Exception {
        return extractMessage(key, ByteBuffer.wrap(encryptedMessage).order(ByteOrder.LITTLE_ENDIAN));
    }

    private byte[] extractMessage(final SecretKey key,
                                  final ByteBuffer encryptedBuffer) throws Exception {
        final var messageHash = encryptedBuffer.getLong();
        final var iv = new byte[Encryption.NONCE_LENGTH];
        encryptedBuffer.get(iv);

        final var encryptedMessage = new byte[encryptedBuffer.remaining()];
        encryptedBuffer.get(encryptedMessage);

        assertFalse(encryptedBuffer.hasRemaining());

        final var cipher =
                createDecryptingCipher(
                        key,
                        new IvParameterSpec(iv),
                        EncryptionOutputStream.CIPHER_TRANSFORMATION
                );

        final var decrypted = cipher.doFinal(encryptedMessage);
        final var expectedHash =
                Hashing.murmur3_128()
                        .hashBytes(
                                ByteBuffer.allocate(decrypted.length + iv.length)
                                        .put(decrypted)
                                        .put(iv)
                                        .flip()
                        ).asLong();
        assertEquals(expectedHash, messageHash);

        return decrypted;
    }

}
