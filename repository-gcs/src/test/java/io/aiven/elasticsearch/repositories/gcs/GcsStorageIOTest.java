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

import javax.crypto.spec.SecretKeySpec;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.Security;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import io.aiven.elasticsearch.repositories.io.CryptoIOProvider;

import com.google.cloud.NoCredentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class GcsStorageIOTest {

    private static final BlobId BLOB_ID = BlobId.of("bucket", "blob");

    private static final byte[] BLOB = new byte[3 * 1024 * 1024];

    static {
        new Random(42).nextBytes(BLOB);
    }

    @Mock
    private Storage storage;

    @Mock
    private ReadChannel channel;

    @Mock
    private WriteChannel writeChannel;

    @BeforeEach
    void setUp() {
        when(storage.getOptions()).thenReturn(StorageOptions.newBuilder()
            .setProjectId("test")
            .setCredentials(NoCredentials.getInstance())
            .build());
        lenient().when(storage.reader(BLOB_ID)).thenReturn(channel);
    }

    @Test
    void writeSizesUploadChunkToBlob() throws IOException {
        final var written = captureWrites();
        final var storageIO = new GcsRepositoryStorageIOProvider.GcsStorageIO(
            storage, BLOB_ID.getBucket(), CryptoIOProvider.NO_OP_CRYPTO_PROVIDER);

        storageIO.write(BLOB_ID.getName(), new ByteArrayInputStream(BLOB), BLOB.length, false);

        verify(writeChannel).setChunkSize(BLOB.length);
        assertArrayEquals(BLOB, written.toByteArray());
    }

    @Test
    void writeCapsUploadChunkAtConfiguredSize() throws IOException {
        captureWrites();
        final var uploadChunkSize = 3 * 1024 * 1024 + 768 * 1024;
        final var storageIO = new GcsRepositoryStorageIOProvider.GcsStorageIO(
            storage, BLOB_ID.getBucket(), CryptoIOProvider.NO_OP_CRYPTO_PROVIDER, uploadChunkSize);

        storageIO.write(BLOB_ID.getName(), new ByteArrayInputStream(BLOB), 64L * 1024 * 1024, false);

        verify(writeChannel).setChunkSize(uploadChunkSize);
    }

    @Test
    void readWithPositionSeeksAndStopsAfterLength() throws IOException {
        storeObject(BLOB);
        final var storageIO = new GcsRepositoryStorageIOProvider.GcsStorageIO(
            storage, BLOB_ID.getBucket(), CryptoIOProvider.NO_OP_CRYPTO_PROVIDER);

        try (final var in = storageIO.read(BLOB_ID.getName(), 1_000_000, 500)) {
            assertArrayEquals(Arrays.copyOfRange(BLOB, 1_000_000, 1_000_500), in.readAllBytes());
        }
        verify(channel).seek(1_000_000);
    }

    @Test
    void readWithPositionDecryptsBlobEncryptedWithRepositoryKey() throws IOException {
        Security.addProvider(new BouncyCastleFipsProvider());
        final var cryptoIOProvider = new CryptoIOProvider(new SecretKeySpec(Arrays.copyOf(BLOB, 32), "AES"), 1024);
        final var stored = new ByteArrayOutputStream();
        cryptoIOProvider.compressAndEncrypt(new ByteArrayInputStream(BLOB), stored);
        storeObject(stored.toByteArray());
        final var storageIO = new GcsRepositoryStorageIOProvider.GcsStorageIO(
            storage, BLOB_ID.getBucket(), cryptoIOProvider);

        try (final var in = storageIO.read(BLOB_ID.getName(), 1_000_000, 500)) {
            assertArrayEquals(Arrays.copyOfRange(BLOB, 1_000_000, 1_000_500), in.readAllBytes());
        }
    }

    private ByteArrayOutputStream captureWrites() throws IOException {
        final var written = new ByteArrayOutputStream();
        when(storage.writer(any(BlobInfo.class))).thenReturn(writeChannel);
        when(writeChannel.write(any(ByteBuffer.class))).thenAnswer(invocation -> {
            final ByteBuffer src = invocation.getArgument(0);
            final int length = src.remaining();
            final var bytes = new byte[length];
            src.get(bytes);
            written.write(bytes);
            return length;
        });
        return written;
    }

    private void storeObject(final byte[] object) throws IOException {
        final var position = new AtomicInteger();
        lenient().doAnswer(invocation -> {
            position.set(Math.toIntExact(invocation.<Long>getArgument(0)));
            return null;
        }).when(channel).seek(anyLong());
        when(channel.read(any(ByteBuffer.class))).thenAnswer(invocation -> {
            final ByteBuffer dst = invocation.getArgument(0);
            final int length = Math.min(dst.remaining(), object.length - position.get());
            if (length <= 0) {
                return -1;
            }
            dst.put(object, position.getAndAdd(length), length);
            return length;
        });
    }
}
