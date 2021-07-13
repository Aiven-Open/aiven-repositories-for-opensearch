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

import java.nio.file.Files;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.aiven.elasticsearch.repositories.RsaKeyAwareTest;
import io.aiven.elasticsearch.repositories.io.CryptoIOProvider;
import io.aiven.elasticsearch.repositories.security.EncryptionKeyProvider;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.opensearch.common.settings.Settings;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class S3StorageIOTest extends RsaKeyAwareTest {

    @Mock
    AmazonS3 mockedAmazonS3;

    @Captor
    ArgumentCaptor<DeleteObjectsRequest> deleteObjectsRequestArgumentCaptor;

    @Test
    void deleteFilesUsingBulk() throws Exception {

        when(mockedAmazonS3.deleteObjects(deleteObjectsRequestArgumentCaptor.capture()))
                .thenReturn(mock(DeleteObjectsResult.class));


        final var encProvider =
                EncryptionKeyProvider.of(
                        Files.newInputStream(publicKeyPem),
                        Files.newInputStream(privateKeyPem));

        final var s3StorageIO =
                new S3RepositoryStorageIOProvider(mockedAmazonS3, encProvider)
                        .createStorageIOFor(
                                Settings.builder()
                                        .put(S3RepositoryStorageIOProvider.BUCKET_NAME.getKey(), "some_bucket")
                                        .build(),
                                new CryptoIOProvider(null, 0)
                        );

        final var hugeListOfFiles =
                Stream.generate(this::generateRandomString)
                        .limit(10_000 * 3)
                        .collect(Collectors.toList());
        s3StorageIO.deleteFiles(hugeListOfFiles, true);

        final var deleteObjectsRequests =
                deleteObjectsRequestArgumentCaptor.getAllValues();

        deleteObjectsRequests.forEach(r -> {
            assertEquals("some_bucket", r.getBucketName());
            assertEquals(10_000, r.getKeys().size());
        });

        verify(mockedAmazonS3, times(3))
                .deleteObjects(deleteObjectsRequestArgumentCaptor.capture());
    }

    @Test
    void deleteDirectoriesUsingBulk() throws Exception {
        final var encProvider =
                EncryptionKeyProvider.of(
                        Files.newInputStream(publicKeyPem),
                        Files.newInputStream(privateKeyPem));

        final var objectSummaries =
                Stream.generate(() -> {
                    final var objectSummary = new S3ObjectSummary();
                    objectSummary.setKey(generateRandomString());
                    objectSummary.setSize(10L);
                    return objectSummary;
                }).limit(10_000 + 2).collect(Collectors.toList());

        final var listingResult = mock(ListObjectsV2Result.class);

        when(listingResult.getObjectSummaries()).thenReturn(objectSummaries);
        when(mockedAmazonS3.listObjectsV2(any(ListObjectsV2Request.class)))
                .thenReturn(listingResult);
        when(mockedAmazonS3.deleteObjects(deleteObjectsRequestArgumentCaptor.capture()))
                .thenReturn(mock(DeleteObjectsResult.class));

        final var s3StorageIO =
                new S3RepositoryStorageIOProvider(mockedAmazonS3, encProvider)
                        .createStorageIOFor(
                                Settings.builder()
                                        .put(S3RepositoryStorageIOProvider.BUCKET_NAME.getKey(), "some_bucket")
                                        .build(),
                                new CryptoIOProvider(null, 0)
                        );

        final var result = s3StorageIO.deleteDirectories("/dome/path");

        assertEquals(10_002, result.v1());
        assertEquals(10_002 * 10L, result.v2());

        final var deleteObjectRequests = deleteObjectsRequestArgumentCaptor.getAllValues();

        assertEquals(10_000, deleteObjectRequests.get(0).getKeys().size());
        assertEquals(2, deleteObjectRequests.get(1).getKeys().size());

        verify(mockedAmazonS3, times(2))
                .deleteObjects(deleteObjectsRequestArgumentCaptor.capture());
    }

    private String generateRandomString() {
        final var chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
        final StringBuilder sb = new StringBuilder();
        final Random random = new Random();
        for (int i = 0; i < 10; i++) {
            sb.append(chars.charAt(random.nextInt(chars.length())));
        }
        return sb.toString();
    }

}
