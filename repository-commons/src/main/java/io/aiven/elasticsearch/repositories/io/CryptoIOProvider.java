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

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import io.aiven.elasticsearch.repositories.security.Decryption;
import io.aiven.elasticsearch.repositories.security.Encryption;

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import org.elasticsearch.common.io.Streams;


public class CryptoIOProvider implements Encryption, Decryption {

    static final String CIPHER_TRANSFORMATION = "AES/CTR/NoPadding";

    static final int ENCODED_PARAMETERS_LENGTH = 16;

    private final SecretKey encryptionKey;

    public CryptoIOProvider(final SecretKey encryptionKey) {
        this.encryptionKey = encryptionKey;
    }

    public void compressAndEncrypt(final InputStream chunk,
                                   final WritableByteChannel writer) throws IOException {
        final var out = Channels.newOutputStream(writer);
        Streams.copy(
                chunk,
                new ZstdOutputStream(
                        new CipherOutputStream(
                                out,
                                createEncryptingCipherAndWriteIv(out)
                        )
                )
        );
    }

    private Cipher createEncryptingCipherAndWriteIv(final OutputStream out) throws IOException {
        final var cipher = createEncryptingCipher(encryptionKey, CIPHER_TRANSFORMATION);
        out.write(cipher.getIV());
        return cipher;
    }

    public InputStream decryptAndDecompress(final ReadableByteChannel reader) throws IOException {
        final var in = Channels.newInputStream(reader);
        return new ZstdInputStream(
                new CipherInputStream(
                        in,
                        readIvAndCreateDecryptingCipher(in)));
    }

    private Cipher readIvAndCreateDecryptingCipher(final InputStream in) throws IOException {
        return createDecryptingCipher(
                encryptionKey,
                new IvParameterSpec(in.readNBytes(ENCODED_PARAMETERS_LENGTH)),
                CIPHER_TRANSFORMATION
        );
    }

}
