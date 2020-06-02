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

package io.aiven.elasticsearch.gcs.storage;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.WritableByteChannel;

import io.aiven.elasticsearch.gcs.GcsSettingsProvider;
import io.aiven.elasticsearch.storage.RsaKeyAwareTest;
import io.aiven.elasticsearch.storage.io.CryptoIOProvider;

import com.google.cloud.ReadChannel;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import org.elasticsearch.common.blobstore.BlobPath;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class GcsBlobContainerTest extends RsaKeyAwareTest {

    static final String TEST_BUCKET = "bucket_42";

    static final String BASE_PATH = "foo_bar";

    @Mock
    GcsSettingsProvider gcsSettingsProvider;

    @Mock
    Storage storage;

    @Mock
    CryptoIOProvider cryptoIOProvider;

    @InjectMocks
    GcsBlobStore gcsBlobStore;

    GcsBlobContainer gcsBlobContainer;

    @BeforeEach
    void setUp() throws IOException {
        when(gcsSettingsProvider.gcsClient()).thenReturn(storage);
        gcsBlobContainer =
                new GcsBlobContainer(
                        BlobPath.cleanPath().add(BASE_PATH),
                        gcsBlobStore,
                        TEST_BUCKET
                );
    }

    @Test
    void writeBlobFully() throws IOException {
        when(cryptoIOProvider.compressAndEncrypt(any(InputStream.class)))
                .thenReturn(new byte[0]);

        gcsBlobContainer.writeBlob(
                "small_file",
                mock(InputStream.class),
                GcsBlobContainer.MAX_BLOB_SIZE / 2,
                false);

        final ArgumentCaptor<BlobInfo> blobInfoCaptor =
                ArgumentCaptor.forClass(BlobInfo.class);

        verify(cryptoIOProvider).compressAndEncrypt(any(InputStream.class));
        verify(storage)
                .create(blobInfoCaptor.capture(), any(byte[].class));

        final var blobInfo = blobInfoCaptor.getValue();

        assertEquals(TEST_BUCKET, blobInfo.getBucket());
        assertEquals("foo_bar/small_file", blobInfo.getName());
    }

    @Test
    void writeBlobFullyWithDoesNotExistOption() throws IOException {
        when(cryptoIOProvider.compressAndEncrypt(any(InputStream.class)))
                .thenReturn(new byte[0]);

        gcsBlobContainer.writeBlob(
                "small_file",
                mock(InputStream.class),
                GcsBlobContainer.MAX_BLOB_SIZE / 2,
                true);

        final ArgumentCaptor<BlobInfo> blobInfoCaptor =
                ArgumentCaptor.forClass(BlobInfo.class);
        final ArgumentCaptor<Storage.BlobTargetOption[]> blobTargetOptionCaptor =
                ArgumentCaptor.forClass(Storage.BlobTargetOption[].class);

        verify(cryptoIOProvider)
                .compressAndEncrypt(any(InputStream.class));
        verify(storage)
                .create(blobInfoCaptor.capture(),
                        any(byte[].class),
                        blobTargetOptionCaptor.capture());

        final var blobInfo = blobInfoCaptor.getValue();

        assertEquals(TEST_BUCKET, blobInfo.getBucket());
        assertEquals(BASE_PATH + "/small_file", blobInfo.getName());
        assertEquals(
                Storage.BlobTargetOption.doesNotExist(),
                blobTargetOptionCaptor.getValue()
        );
    }

    @Test
    void writeBlobResumeable() throws IOException {
        when(storage.writer(any(BlobInfo.class)))
                .thenReturn(mock(WriteChannel.class));

        gcsBlobContainer.writeBlob(
                "large_file",
                mock(InputStream.class),
                GcsBlobContainer.MAX_BLOB_SIZE + 100,
                false);

        final ArgumentCaptor<BlobInfo> blobInfoCaptor =
                ArgumentCaptor.forClass(BlobInfo.class);

        verify(cryptoIOProvider)
                .compressAndEncrypt(any(InputStream.class), any(WritableByteChannel.class));
        verify(storage)
                .writer(blobInfoCaptor.capture());

        final var blobInfo = blobInfoCaptor.getValue();

        assertEquals(TEST_BUCKET, blobInfo.getBucket());
        assertEquals(BASE_PATH + "/large_file", blobInfo.getName());
    }

    @Test
    void writeBlobResumeableWithDoesNotExistOption() throws IOException {
        when(storage.writer(any(BlobInfo.class), any()))
                .thenReturn(mock(WriteChannel.class));

        gcsBlobContainer.writeBlob(
                "large_file",
                mock(InputStream.class),
                GcsBlobContainer.MAX_BLOB_SIZE + 100,
                true);

        final ArgumentCaptor<BlobInfo> blobInfoCaptor =
                ArgumentCaptor.forClass(BlobInfo.class);
        final ArgumentCaptor<Storage.BlobWriteOption[]> blobTargetOptionCaptor =
                ArgumentCaptor.forClass(Storage.BlobWriteOption[].class);

        verify(cryptoIOProvider)
                .compressAndEncrypt(any(InputStream.class), any(WritableByteChannel.class));
        verify(storage)
                .writer(blobInfoCaptor.capture(),
                        blobTargetOptionCaptor.capture());

        final var blobInfo = blobInfoCaptor.getValue();

        assertEquals(TEST_BUCKET, blobInfo.getBucket());
        assertEquals(BASE_PATH + "/large_file", blobInfo.getName());
        assertEquals(
                Storage.BlobWriteOption.doesNotExist(),
                blobTargetOptionCaptor.getValue()
        );
    }

    @Test
    void readBlobFully() throws IOException {
        final var bucket = mock(Bucket.class);
        final var blob = mock(Blob.class);
        when(storage.get(TEST_BUCKET)).thenReturn(bucket);
        when(bucket.get(BASE_PATH + "/small_file")).thenReturn(blob);
        when(blob.getSize()).thenReturn(Long.valueOf(GcsBlobContainer.MAX_BLOB_SIZE / 2));
        when(blob.getContent()).thenReturn(new byte[0]);

        gcsBlobContainer.readBlob("small_file");

        verify(bucket).get(BASE_PATH + "/small_file");
        verify(blob).getContent();
        verify(cryptoIOProvider).decryptAndDecompress(any(byte[].class));
    }

    @Test
    void readBlobResumeable() throws IOException {
        final var bucket = mock(Bucket.class);
        final var blob = mock(Blob.class);
        when(storage.get(TEST_BUCKET)).thenReturn(bucket);
        when(bucket.get(BASE_PATH + "/large_file")).thenReturn(blob);
        when(blob.getSize()).thenReturn(Long.valueOf(GcsBlobContainer.MAX_BLOB_SIZE + 100));
        when(blob.reader()).thenReturn(mock(ReadChannel.class));

        gcsBlobContainer.readBlob("large_file");

        verify(bucket).get(BASE_PATH + "/large_file");
        verify(blob).reader();
        verify(cryptoIOProvider).decryptAndDecompress(any(ReadChannel.class));
    }

}
