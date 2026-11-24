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
import com.google.cloud.storage.BlobId;
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

    @BeforeEach
    void setUp() {
        when(storage.getOptions()).thenReturn(StorageOptions.newBuilder()
            .setProjectId("test")
            .setCredentials(NoCredentials.getInstance())
            .build());
        when(storage.reader(BLOB_ID)).thenReturn(channel);
    }

    @Test
    void readWithPositionSeeksAndStopsAfterLength() throws IOException {
        storeObject(BLOB);
        final var storageIO = new GcsRepositoryStorageIOProvider.GcsStorageIO(
            storage, BLOB_ID.getBucket(), CryptoIOProvider.UNENCRYPTED);

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
