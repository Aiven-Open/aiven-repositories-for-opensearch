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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.security.SecureRandom;

import io.aiven.elasticsearch.storage.RsaKeyAwareTest;
import io.aiven.elasticsearch.storage.security.Encryption;
import io.aiven.elasticsearch.storage.security.EncryptionKeyProvider;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

class DecryptionInputStreamTest
        extends RsaKeyAwareTest
        implements Encryption {

    private static final int CHUNK_SIZE = 1024 * 100 * 2;
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
    public void decryptMessage() throws Exception {
        final var encKey = encProvider.createKey();

        final var message = newMessage(CHUNK_SIZE);
        final var encryptedMessage = encryptMessage(encKey, message);

        assertArrayEquals(
                message,
                decryptMessage(
                        encKey,
                        encryptedMessage
                )
        );
    }

    @Test
    public void decryptSmallMessage() throws Exception {
        final var encKey = encProvider.createKey();

        final var message = newMessage(40);
        final var encryptedMessage = encryptMessage(encKey, message);

        assertArrayEquals(
                message,
                decryptMessage(
                        encKey,
                        encryptedMessage
                )
        );
    }

    @Test
    public void decryptLargeMessage() throws Exception {
        final var encKey = encProvider.createKey();

        final var message = newMessage(CHUNK_SIZE * 3);
        secureRandom.nextBytes(message);
        final var encryptedMessage = encryptMessage(encKey, message);

        assertArrayEquals(
                message,
                decryptMessage(
                        encKey,
                        encryptedMessage
                )
        );
    }

    private byte[] newMessage(final int size) {
        final var message = new byte[size];
        secureRandom.nextBytes(message);
        return message;
    }

    private byte[] encryptMessage(final SecretKey encKey,
                                  final byte[] message) throws IOException {
        final var outBytes = new ByteArrayOutputStream();
        try (final var writer = new EncryptionOutputStream(outBytes, encKey)) {
            writer.write(message, 0, message.length);
        }
        return outBytes.toByteArray();
    }

    private byte[] decryptMessage(final SecretKey encKey,
                                  final byte[] encryptedMessage) throws IOException {
        try (final var reader = new DecryptionInputStream(new ByteArrayInputStream(encryptedMessage), encKey)) {
            return reader.readAllBytes();
        }
    }

}
