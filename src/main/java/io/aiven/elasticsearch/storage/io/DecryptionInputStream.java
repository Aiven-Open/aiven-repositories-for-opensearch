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

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import io.aiven.elasticsearch.storage.security.Decryption;
import io.aiven.elasticsearch.storage.security.Encryption;

import com.google.common.hash.Hashing;

public class DecryptionInputStream
        extends FilterInputStream
        implements Decryption {

    private final SecretKey encryptionKey;

    private final ByteBuffer buffer;

    public DecryptionInputStream(final InputStream in, final SecretKey encryptionKey) {
        super(in);
        this.encryptionKey = encryptionKey;
        buffer = ByteBuffer.allocate(EncryptionOutputStream.encryptedPackageSize());
    }

    @Override
    public boolean markSupported() {
        return false;
    }

    @Override
    public int available() throws IOException {
        return in.available() + buffer.remaining();
    }

    @Override
    public int read() throws IOException {
        readAndDecryptChunk();
        return buffer.get() & 0xFF;
    }

    @Override
    public int read(final byte[] bytes,
                    final int off,
                    final int len) throws IOException {
        if (len == 0) {
            return 0;
        }

        readAndDecryptChunk();
        final var length = Math.min(len, buffer.remaining());
        buffer.get(bytes, off, length);
        return length == 0 ? -1 : length;
    }

    private void readAndDecryptChunk() throws IOException {
        if (!buffer.hasRemaining() || buffer.position() == 0) {
            final var encryptedBytes = new byte[EncryptionOutputStream.encryptedPackageSize()];
            final var n = in.read(encryptedBytes, 0, encryptedBytes.length);
            if (n > 0) {
                final var encryptedChunk =
                        ByteBuffer.wrap(encryptedBytes, 0, n)
                                .order(ByteOrder.LITTLE_ENDIAN);

                final var chunkHash = encryptedChunk.getLong();
                final var iv = extractIV(encryptedChunk);
                final var encrypted = extractEncryptedMessage(encryptedChunk);

                final var decrypted = decrypt(encrypted, iv);

                assertHash(chunkHash, decrypted, iv);

                buffer.clear().put(decrypted).flip();
            }
        }
    }

    private byte[] extractIV(final ByteBuffer encryptedChunk) {
        final var iv = new byte[Encryption.NONCE_LENGTH];
        encryptedChunk.get(iv);
        return iv;
    }

    private byte[] extractEncryptedMessage(final ByteBuffer encryptedChunk) {
        final var encrypted = new byte[encryptedChunk.remaining()];
        encryptedChunk.get(encrypted);
        return encrypted;
    }

    private void assertHash(final long chunkHash, final byte[] encrypted, final byte[] iv) throws IOException {
        final var hashBuffer = ByteBuffer.allocate(encrypted.length + Encryption.NONCE_LENGTH);
        hashBuffer.put(encrypted).put(iv).flip();
        if (chunkHash != Hashing.murmur3_128().hashBytes(hashBuffer).asLong()) {
            throw new IOException("Broken encrypted package chunk");
        }
    }

    private byte[] decrypt(final byte[] encrypted, final byte[] iv) throws IOException {
        try {
            final var cipher =
                    createDecryptingCipher(
                            encryptionKey,
                            new IvParameterSpec(iv),
                            EncryptionOutputStream.CIPHER_TRANSFORMATION
                    );
            return cipher.doFinal(encrypted);
        } catch (final IllegalBlockSizeException | BadPaddingException e) {
            throw new IOException("Couldn't decrypt message", e);
        }
    }

    @Override
    public byte[] readAllBytes() throws IOException {
        return readNBytes(Integer.MAX_VALUE);
    }
}
