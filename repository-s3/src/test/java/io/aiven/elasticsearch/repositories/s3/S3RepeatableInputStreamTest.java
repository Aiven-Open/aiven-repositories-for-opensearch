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

package io.aiven.elasticsearch.repositories.s3;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class S3RepeatableInputStreamTest {

    static final String BUCKET_NAME = "some_bucket";

    static final String FILE_KEY = "some_key";

    @Mock
    AmazonS3 mockedAmazonS3;

    @Mock
    S3Object mockedS3Object;

    @Mock
    S3ObjectInputStream mockedS3InputStream;

    @Captor
    ArgumentCaptor<GetObjectRequest> getObjectRequestArgumentCaptor;

    @BeforeEach
    void setup() {
        when(mockedS3Object.getObjectContent()).thenReturn(mockedS3InputStream);
        when(mockedAmazonS3.getObject(getObjectRequestArgumentCaptor.capture())).thenReturn(mockedS3Object);
    }

    @Test
    void testReadRetryingWhenBlobIsGone() throws IOException {
        final var content = 42;

        final var invocationCounter = new AtomicInteger(0);
        when(mockedS3InputStream.read()).thenAnswer(invocation -> {
            if (invocationCounter.get() == 2) {
                return content;
            } else {
                invocationCounter.incrementAndGet();
                throw new IOException("boo");
            }
        });
        try (final var in = new S3RepeatableInputStream(mockedAmazonS3, BUCKET_NAME, FILE_KEY, 2)) {
            assertEquals(content, in.read());
            assertEquals(2, in.attempt);
        }
    }

    @Test
    void testReadWithBytesRetryingWhenBlobIsGone() throws IOException {
        final var content = randomBytes(42);

        final var invocationCounter = new AtomicInteger(0);
        when(mockedS3InputStream.read(any(byte[].class), any(int.class), any(int.class))).thenAnswer(invocation -> {
            if (invocationCounter.get() == 2) {
                final var b = (byte[]) invocation.getArgument(0);
                System.arraycopy(content, 0, b, 0, b.length);
                return b.length;
            } else {
                invocationCounter.incrementAndGet();
                throw new IOException("boo");
            }
        });
        try (final var in = new S3RepeatableInputStream(mockedAmazonS3, BUCKET_NAME, FILE_KEY, 2)) {
            final var bytes = new byte[42];
            in.read(bytes);
            assertEquals(content.length, bytes.length);
            assertArrayEquals(content, bytes);
            assertEquals(2, in.attempt);
        }
    }

    @Test
    void testRestoredWhenChunkReadFails() throws IOException {
        final var content = randomBytes(84);
        final var invocationCounter = new AtomicInteger(0);
        final var readBytes = new AtomicInteger(0);
        when(mockedS3InputStream.read(any(byte[].class), any(int.class), any(int.class))).thenAnswer(invocation -> {
            if (invocationCounter.get() == 0 && readBytes.get() == 0) {
                final var b = (byte[]) invocation.getArgument(0);
                System.arraycopy(content, 0, b, 0, b.length);
                readBytes.addAndGet(b.length);
                return readBytes.get();
            } else if (invocationCounter.get() == 2 && readBytes.get() == 42) {
                final var b = (byte[]) invocation.getArgument(0);
                System.arraycopy(content, readBytes.get(), b, 0, b.length);
                readBytes.addAndGet(b.length);
                return 42;
            } else {
                invocationCounter.incrementAndGet();
                throw new IOException("boo");
            }
        });
        final var buffer = ByteBuffer.allocate(84);
        try (final var in = new S3RepeatableInputStream(mockedAmazonS3, BUCKET_NAME, FILE_KEY, 2)) {
            final var bytes = new byte[42];
            in.read(bytes);
            buffer.put(bytes);
            in.read(bytes);
            buffer.put(bytes);
            assertEquals(2, in.attempt);
        }
        assertArrayEquals(content, buffer.array());
        assertEquals(42, getObjectRequestArgumentCaptor.getValue().getRange()[0]);
    }

    private byte[] randomBytes(final int size) {
        final var bytes = new byte[size];
        new Random().nextBytes(bytes);
        return bytes;
    }

}
