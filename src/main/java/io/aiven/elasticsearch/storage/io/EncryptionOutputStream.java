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
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.SecretKey;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import io.aiven.elasticsearch.storage.security.Encryption;

import com.google.common.hash.Hashing;

public class EncryptionOutputStream
        extends FilterOutputStream
        implements Encryption {

    static final String CIPHER_TRANSFORMATION = "AES/CTR/NoPadding";

    private final ByteBuffer buffer;

    private final SecretKey encryptionKey;

    public EncryptionOutputStream(final OutputStream out,
                                  final SecretKey encryptionKey) {
        super(out);
        this.buffer = ByteBuffer.allocate(ENCRYPTION_CHUNK_SIZE);
        this.encryptionKey = encryptionKey;
    }

    public static int encryptedPackageSize() {
        return ENCRYPTION_CHUNK_SIZE + NONCE_LENGTH + Long.BYTES;
    }

    @Override
    public void write(final int b) throws IOException {
        write(new byte[]{(byte) b});
    }

    @Override
    public void write(final byte[] bytes,
                      final int off,
                      final int len) throws IOException {
        final var source = ByteBuffer.wrap(bytes, off, len);
        while (source.hasRemaining()) {
            final var transferred = Math.min(buffer.remaining(), source.remaining());
            final var offset = source.arrayOffset() + source.position();
            buffer.put(source.array(), offset, transferred);
            source.position(source.position() + transferred);
            if (!buffer.hasRemaining()) {
                flushBuffer();
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (ByteBufferUtils.nonEmpty(buffer)) {
            flushBuffer();
        }
        out.close();
    }

    private void flushBuffer() throws IOException {
        try {
            final var encryptedChunk = encryptionChunk();
            out.write(encryptedChunk.array());
        } finally {
            buffer.clear();
        }
    }

    private ByteBuffer encryptionChunk() throws IOException {
        final var cipher = createEncryptingCipher(encryptionKey, CIPHER_TRANSFORMATION);

        final var chunkToSign = ByteBufferUtils.loadBytes(buffer);

        final var encrypted = encrypt(chunkToSign, cipher);
        final var hash = makeHash(chunkToSign, cipher.getIV());

        return ByteBuffer
                .allocate(encrypted.length + NONCE_LENGTH + Long.BYTES)
                .order(ByteOrder.LITTLE_ENDIAN)
                .putLong(hash)
                .put(cipher.getIV())
                .put(encrypted);
    }

    private byte[] encrypt(final byte[] chunkToSign, final Cipher cipher) throws IOException {
        try {
            return cipher.doFinal(chunkToSign);
        } catch (final IllegalBlockSizeException | BadPaddingException e) {
            throw new IOException("Couldn't encrypt data", e);
        } finally {
            buffer.clear();
        }
    }

    private long makeHash(final byte[] chunkToSign, final byte[] iv) {
        final ByteBuffer hashBuffer = ByteBuffer.allocate(chunkToSign.length + NONCE_LENGTH);
        hashBuffer.put(chunkToSign).put(iv).flip();
        return Hashing.murmur3_128().hashBytes(hashBuffer).asLong();
    }

}
