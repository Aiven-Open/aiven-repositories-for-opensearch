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
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
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
    void writeBlob() throws IOException {
        when(storage.writer(any(BlobInfo.class)))
                .thenReturn(mock(WriteChannel.class));

        gcsBlobContainer.writeBlob(
                "large_file",
                mock(InputStream.class),
                1000,
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
    void writeBlobWithDoesNotExistOption() throws IOException {
        when(storage.writer(any(BlobInfo.class), any()))
                .thenReturn(mock(WriteChannel.class));

        gcsBlobContainer.writeBlob(
                "large_file",
                mock(InputStream.class),
                1000,
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
    void readBlob() throws IOException {
        when(storage.reader(any(BlobId.class))).thenReturn(mock(ReadChannel.class));

        final ArgumentCaptor<BlobId> blobIdCaptor =
                ArgumentCaptor.forClass(BlobId.class);

        gcsBlobContainer.readBlob("a_file");

        verify(storage).reader(blobIdCaptor.capture());

        final var blobId = blobIdCaptor.getValue();

        assertEquals(TEST_BUCKET, blobId.getBucket());
        assertEquals(BASE_PATH + "/a_file", blobId.getName());

        verify(cryptoIOProvider).decryptAndDecompress(any(ReadChannel.class));
    }

}
