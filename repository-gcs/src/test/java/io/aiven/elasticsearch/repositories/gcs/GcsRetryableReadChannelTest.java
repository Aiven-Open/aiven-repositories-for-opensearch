/*
 * Copyright 2021 Aiven Oy
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

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.Tuple;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.StorageOptions.DefaultStorageFactory;
import com.google.cloud.storage.spi.StorageRpcFactory;
import com.google.cloud.storage.spi.v1.StorageRpc;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class GcsRetryableReadChannelTest {
    private static final Map<StorageRpc.Option, ?> EMPTY_RPC_OPTIONS = ImmutableMap.of();
    private static final int DEFAULT_CHUNK_SIZE = 2 * 1024 * 1024;
    private static final int CUSTOM_CHUNK_SIZE = 2 * 1024 * 1024;
    private static final Random RANDOM = new Random();

    private StorageOptions options;
    @Mock private StorageRpcFactory rpcFactoryMock;
    @Mock private StorageRpc storageRpcMock;
    private Storage storage;

    @BeforeEach
    public void setUp() {
        when(rpcFactoryMock.create(any(StorageOptions.class))).thenReturn(storageRpcMock);

        options = StorageOptions
            .newBuilder()
            .setServiceRpcFactory(rpcFactoryMock)
            .build();

        storage = new DefaultStorageFactory().create(options);
    }

    @Test
    public void testReaderIsRestoredWhenChunkReadFails() throws IOException {
        final BlobId blobId = BlobId.of("container", "test");
        final var reader = new GcsRetryableReadChannel(storage.reader(blobId), blobId, 3);
        
        final byte[] result = randomByteArray(2 * DEFAULT_CHUNK_SIZE);
        final ByteBuffer buffer = ByteBuffer.allocate(DEFAULT_CHUNK_SIZE);
        
        final AtomicInteger chunk = new AtomicInteger();
        final AtomicInteger retries = new AtomicInteger(options.getRetrySettings().getMaxAttempts() + 1);
        when(storageRpcMock.read(any(StorageObject.class), eq(EMPTY_RPC_OPTIONS), anyLong(), eq(CUSTOM_CHUNK_SIZE)))
            .thenAnswer(invocation -> {
                final int position = Math.toIntExact((long) invocation.getArgument(2));
                if (position == 0) {
                    return Tuple.of("etag-" + chunk.get(), Arrays.copyOfRange(result, 0, DEFAULT_CHUNK_SIZE));
                } else if (retries.decrementAndGet() > 0) {
                    throw new StorageException(new SocketTimeoutException());
                } else {
                    return Tuple.of("etag-" + chunk.getAndIncrement(), Arrays.copyOfRange(result, position, 
                        Math.min(result.length, position + CUSTOM_CHUNK_SIZE)));
                }
            });
        
        // clean read, first chunk
        assertEquals(DEFAULT_CHUNK_SIZE, reader.read(buffer));
        assertArrayEquals(Arrays.copyOfRange(result, 0, DEFAULT_CHUNK_SIZE), buffer.array());
        
        // SocketTimeoutException, up to max retry attempts + 1
        assertEquals(DEFAULT_CHUNK_SIZE, reader.read(buffer.clear()));
        assertArrayEquals(Arrays.copyOfRange(result, DEFAULT_CHUNK_SIZE, 2 * DEFAULT_CHUNK_SIZE), buffer.array());

        // clean read, last chunk
        assertEquals(-1, reader.read(buffer.clear()));
    }
    
    @Test
    public void testReaderIsNotRetryingWhenBlobIsGone() throws IOException {
        final BlobId blobId = BlobId.of("container", "test");
        final var reader = new GcsRetryableReadChannel(storage.reader(blobId), blobId, 3);
        final ByteBuffer buffer = ByteBuffer.allocate(DEFAULT_CHUNK_SIZE);

        when(storageRpcMock.read(any(StorageObject.class), eq(EMPTY_RPC_OPTIONS), anyLong(), eq(CUSTOM_CHUNK_SIZE)))
            .thenThrow(new StorageException(404, "HTTP 1.1 NOT FOUND", "Blob is gone", new IOException()));
        
        assertThrows(NoSuchFileException.class, () -> reader.read(buffer));
    }
    
    private static byte[] randomByteArray(final int size) {
        final byte[] byteArray = new byte[size];
        RANDOM.nextBytes(byteArray);
        return byteArray;
    }
}
