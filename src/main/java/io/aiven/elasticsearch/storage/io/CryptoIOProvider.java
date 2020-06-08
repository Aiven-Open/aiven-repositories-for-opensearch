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
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import org.elasticsearch.common.io.Streams;

public class CryptoIOProvider {

    private final SecretKey encryptionKey;

    public CryptoIOProvider(final SecretKey encryptionKey) {
        this.encryptionKey = encryptionKey;
    }

    public byte[] compressAndEncrypt(final InputStream in) throws IOException {
        final var out = new ByteArrayOutputStream();
        Streams.copy(
                in,
                new ZstdOutputStream(
                        new EncryptionOutputStream(out, encryptionKey))
        );
        return out.toByteArray();
    }

    public void compressAndEncrypt(final InputStream chunk,
                                   final WritableByteChannel writer) throws IOException {
        Streams.copy(
                chunk,
                new ZstdOutputStream(
                        new EncryptionOutputStream(
                                Channels.newOutputStream(writer),
                                encryptionKey))
        );
    }

    public InputStream decryptAndDecompress(final byte[] inBytes) throws IOException {
        return new ZstdInputStream(
                new DecryptionInputStream(
                        new ByteArrayInputStream(inBytes),
                        encryptionKey));
    }

    public InputStream decryptAndDecompress(final ReadableByteChannel reader) throws IOException {
        return new ZstdInputStream(
                new DecryptionInputStream(
                        Channels.newInputStream(reader),
                        encryptionKey));
    }

}
