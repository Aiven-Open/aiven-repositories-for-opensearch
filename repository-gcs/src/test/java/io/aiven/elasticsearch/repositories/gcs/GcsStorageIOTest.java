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

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;

import io.aiven.elasticsearch.repositories.io.CryptoIOProvider;

import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.Tuple;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.spi.StorageRpcFactory;
import com.google.cloud.storage.spi.v1.StorageRpc;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class GcsStorageIOTest {

    private static final Map<StorageRpc.Option, ?> EMPTY_RPC_OPTIONS = ImmutableMap.of();

    private static final byte[] BLOB = new byte[3 * 1024 * 1024];

    static {
        new Random(42).nextBytes(BLOB);
    }

    @Mock
    private StorageRpcFactory rpcFactoryMock;

    @Mock
    private StorageRpc storageRpcMock;

    private Storage storage;

    @BeforeEach
    void setUp() {
        when(rpcFactoryMock.create(any(StorageOptions.class))).thenReturn(storageRpcMock);
        storage = new StorageOptions.DefaultStorageFactory()
            .create(StorageOptions.newBuilder().setServiceRpcFactory(rpcFactoryMock).build());
    }

    @Test
    void readWithPositionSeeksAndStopsAfterLength() throws IOException {
        when(storageRpcMock.read(any(StorageObject.class), eq(EMPTY_RPC_OPTIONS), anyLong(), anyInt()))
            .thenAnswer(invocation -> {
                final int position = Math.toIntExact((long) invocation.getArgument(2));
                final int chunkSize = invocation.getArgument(3);
                return Tuple.of("etag", Arrays.copyOfRange(BLOB, position, Math.min(BLOB.length, position + chunkSize)));
            });
        final var storageIO = new GcsRepositoryStorageIOProvider.GcsStorageIO(
            storage, "bucket", CryptoIOProvider.UNENCRYPTED);

        try (final var in = storageIO.read("blob", 1_000_000, 500)) {
            assertArrayEquals(Arrays.copyOfRange(BLOB, 1_000_000, 1_000_500), in.readAllBytes());
        }
        verify(storageRpcMock).read(any(StorageObject.class), eq(EMPTY_RPC_OPTIONS), eq(1_000_000L), anyInt());
    }

    @Test
    void readWithPositionIsRefusedForRepositoryWithoutCryptoSettings() {
        final var cryptoIOProvider = new CryptoIOProvider(new SecretKeySpec(new byte[32], "AES"), 1024);
        final var storageIO = new GcsRepositoryStorageIOProvider.GcsStorageIO(storage, "bucket", cryptoIOProvider);

        assertThrows(UnsupportedOperationException.class, () -> storageIO.read("blob", 0, 10));
    }
}
