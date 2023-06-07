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

import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.opensearch.common.util.io.Streams;

import io.aiven.elasticsearch.repositories.security.Decryption;
import io.aiven.elasticsearch.repositories.security.Encryption;

import com.github.luben.zstd.RecyclingBufferPool;
import com.github.luben.zstd.ZstdInputStreamNoFinalizer;
import com.github.luben.zstd.ZstdOutputStreamNoFinalizer;

public class CryptoIOProvider implements Encryption, Decryption {

    static final String CIPHER_TRANSFORMATION = "AES/CTR/NoPadding";

    static final int NONCE_LENGTH = 16;

    static final int COMPRESSION_LEVEL = 3;

    static final int BUFFER_SIZE = 16 * 1024;

    private final SecretKey encryptionKey;

    private final int bufferSize;

    public CryptoIOProvider(final SecretKey encryptionKey, final int bufferSize) {
        this.encryptionKey = encryptionKey;
        this.bufferSize = bufferSize;
    }

    public long compressAndEncrypt(final InputStream in,
                                   final OutputStream out) throws IOException {
        final var cipher = createEncryptingCipher(encryptionKey, CIPHER_TRANSFORMATION);
        out.write(cipher.getIV());
        return Streams.copy(
                in,
                newBufferedOutputStream(
                        new ZstdOutputStreamNoFinalizer(
                                new CipherOutputStream(out, cipher),
                                RecyclingBufferPool.INSTANCE,
                                COMPRESSION_LEVEL)
                ),
                new byte[bufferSize]
        );
    }

    private OutputStream newBufferedOutputStream(final OutputStream out) {
        return new BufferedOutputStream(out, BUFFER_SIZE);
    }

    public InputStream decryptAndDecompress(final InputStream in) throws IOException {
        final var cipher = createDecryptingCipher(
                encryptionKey,
                new IvParameterSpec(in.readNBytes(NONCE_LENGTH)),
                CIPHER_TRANSFORMATION);
        return newBufferedInputStream(
                new ZstdInputStreamNoFinalizer(
                        new CipherInputStream(in, cipher),
                        RecyclingBufferPool.INSTANCE
                )
        );
    }

    private InputStream newBufferedInputStream(final InputStream in) {
        return new BufferedInputStream(in, BUFFER_SIZE);
    }

}
