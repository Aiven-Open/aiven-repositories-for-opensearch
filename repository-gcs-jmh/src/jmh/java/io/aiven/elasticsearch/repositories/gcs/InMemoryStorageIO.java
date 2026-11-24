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
import java.nio.file.NoSuchFileException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.opensearch.common.collect.Tuple;

import io.aiven.elasticsearch.repositories.RepositoryStorageIOProvider.StorageIO;
import io.aiven.elasticsearch.repositories.io.CryptoIOProvider;

import com.google.common.io.ByteStreams;

final class InMemoryStorageIO implements StorageIO {

    private final Map<String, byte[]> bucket;

    private final CryptoIOProvider cryptoIOProvider;

    private final boolean discardWrites;

    InMemoryStorageIO(final Map<String, byte[]> bucket, final CryptoIOProvider cryptoIOProvider, final boolean discardWrites) {
        this.bucket = bucket;
        this.cryptoIOProvider = cryptoIOProvider;
        this.discardWrites = discardWrites;
    }

    @Override
    public boolean exists(final String blobName) {
        return bucket.containsKey(blobName);
    }

    @Override
    public InputStream read(final String blobName) throws IOException {
        return cryptoIOProvider.decryptAndDecompress(new ByteArrayInputStream(blob(blobName)));
    }

    @Override
    public InputStream read(final String blobName, final long position, final long length) throws IOException {
        final var bytes = blob(blobName);
        if (cryptoIOProvider == CryptoIOProvider.UNENCRYPTED) {
            final var start = Math.toIntExact(Math.min(position, bytes.length));
            return new ByteArrayInputStream(bytes, start, Math.toIntExact(Math.min(length, bytes.length - start)));
        }
        final var in = cryptoIOProvider.decryptAndDecompress(new ByteArrayInputStream(bytes));
        in.skipNBytes(position);
        return ByteStreams.limit(in, length);
    }

    @Override
    public void write(final String blobName,
                      final InputStream inputStream,
                      final long blobSize,
                      final boolean failIfAlreadyExists) throws IOException {
        if (discardWrites) {
            cryptoIOProvider.compressAndEncrypt(inputStream, OutputStream.nullOutputStream());
            return;
        }
        final var out = new ByteArrayOutputStream(Math.toIntExact(blobSize));
        cryptoIOProvider.compressAndEncrypt(inputStream, out);
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

    private byte[] blob(final String blobName) throws NoSuchFileException {
        final var bytes = bucket.get(blobName);
        if (bytes == null) {
            throw new NoSuchFileException(blobName);
        }
        return bytes;
    }
}
