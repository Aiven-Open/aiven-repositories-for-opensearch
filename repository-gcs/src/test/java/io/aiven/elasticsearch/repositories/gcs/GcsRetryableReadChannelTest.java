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
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.Random;

import com.google.cloud.ReadChannel;
import com.google.cloud.RestorableState;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.StorageException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class GcsRetryableReadChannelTest {
    private static final BlobId BLOB_ID = BlobId.of("container", "test");
    private static final int CHUNK_SIZE = 1024;
    private static final int MAX_ATTEMPTS = 3;

    @Mock private ReadChannel channel;
    @Mock private ReadChannel restoredChannel;
    @Mock private RestorableState<ReadChannel> state;

    @Test
    public void testReaderIsRestoredWhenReadFails() throws IOException {
        final byte[] blob = new byte[2 * CHUNK_SIZE];
        new Random(42).nextBytes(blob);
        // The client wraps the failure that exhausted its own retries into an IOException.
        when(channel.read(any(ByteBuffer.class)))
            .thenAnswer(readFrom(Arrays.copyOfRange(blob, 0, CHUNK_SIZE)))
            .thenThrow(new IOException(new StorageException(503, "Service Unavailable")));
        when(channel.capture()).thenReturn(state);
        when(state.restore()).thenReturn(restoredChannel);
        when(restoredChannel.read(any(ByteBuffer.class)))
            .thenAnswer(readFrom(Arrays.copyOfRange(blob, CHUNK_SIZE, blob.length)))
            .thenReturn(-1);

        final var reader = new GcsRetryableReadChannel(channel, BLOB_ID, MAX_ATTEMPTS);

        assertArrayEquals(blob, Channels.newInputStream(reader).readAllBytes());
        verify(channel).close();
    }

    @Test
    public void testReaderFailsWhenAllAttemptsAreExhausted() throws IOException {
        when(channel.read(any(ByteBuffer.class))).thenAnswer(invocation -> {
            throw new IOException(new StorageException(503, "Service Unavailable"));
        });
        when(channel.capture()).thenReturn(state);
        when(state.restore()).thenReturn(channel);

        final var reader = new GcsRetryableReadChannel(channel, BLOB_ID, MAX_ATTEMPTS);

        final var e = assertThrows(StorageException.class, () -> reader.read(ByteBuffer.allocate(CHUNK_SIZE)));
        assertEquals(503, e.getCode());
        assertEquals(MAX_ATTEMPTS - 1, e.getSuppressed().length);
        verify(channel, times(MAX_ATTEMPTS)).read(any(ByteBuffer.class));
    }

    @Test
    public void testReaderIsNotRetryingWhenBlobIsGone() throws IOException {
        when(channel.read(any(ByteBuffer.class)))
            .thenThrow(new IOException(new StorageException(404, "Not Found")));

        final var reader = new GcsRetryableReadChannel(channel, BLOB_ID, MAX_ATTEMPTS);

        assertThrows(NoSuchFileException.class, () -> reader.read(ByteBuffer.allocate(CHUNK_SIZE)));
        verify(channel, never()).capture();
    }

    private static Answer<Integer> readFrom(final byte[] chunk) {
        return invocation -> {
            invocation.<ByteBuffer>getArgument(0).put(chunk);
            return chunk.length;
        };
    }
}
