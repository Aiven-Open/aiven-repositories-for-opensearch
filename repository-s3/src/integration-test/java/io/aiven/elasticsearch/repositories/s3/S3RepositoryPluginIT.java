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
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import io.aiven.elasticsearch.repositories.AbstractRepositoryPluginIT;
import io.aiven.elasticsearch.repositories.DummySecureSettings;
import io.aiven.elasticsearch.repositories.RepositoryStorageIOProvider;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class S3RepositoryPluginIT extends AbstractRepositoryPluginIT {

    static String bucketName;

    static AmazonS3 amazonS3;

    @BeforeAll
    static void setUp() throws Exception {
        bucketName = System.getProperty("integration-test.s3.bucket.name");
        final var awsSecretAccessKey = System.getProperty("integration-test.aws.secret.key");
        final var awsAccessKeyId = System.getProperty("integration-test.aws.secret.id");
        final var awsEndpoint = System.getProperty("integration-test.aws.endpoint");
        configureAndStartCluster(
                S3RepositoryPlugin.class, settingsBuilder ->
                        settingsBuilder.put(S3StorageSettings.ENDPOINT.getKey(), awsEndpoint)
                                .setSecureSettings(new DummySecureSettings()
                                        .setString(
                                                S3StorageSettings.AWS_ACCESS_KEY_ID.getKey(),
                                                awsAccessKeyId)
                                        .setString(
                                                S3StorageSettings.AWS_SECRET_ACCESS_KEY.getKey(),
                                                awsSecretAccessKey)
                                        .setString(
                                                S3StorageSettings.ENDPOINT.getKey(),
                                                awsEndpoint)
                                        .setFile(
                                                S3StorageSettings.PUBLIC_KEY_FILE.getKey(),
                                                Files.newInputStream(publicKeyPem))
                                        .setFile(
                                                S3StorageSettings.PRIVATE_KEY_FILE.getKey(),
                                                Files.newInputStream(privateKeyPem))
                                ));

        amazonS3 = AmazonS3ClientBuilder.standard()
                .withCredentials(
                        new AWSStaticCredentialsProvider(
                                new BasicAWSCredentials(awsAccessKeyId, awsSecretAccessKey)
                        )
                ).withEndpointConfiguration(
                        new AwsClientBuilder.EndpointConfiguration(
                                awsEndpoint, null)
                ).build();


    }

    @AfterAll
    static void tearDown() throws IOException {
        try {
            shutdownCluster();
        } finally {
            listOfFileForBackupFolder(listObjectsRequestFor(BASE_PATH))
                    .forEach(f ->
                            amazonS3.deleteObject(bucketName, f));
        }
    }

    @Override
    public String registerRepositoryJson() {
        return "{ \"type\": \""
                + S3RepositoryPlugin.REPOSITORY_TYPE + "\", "
                + "\"settings\": { \"bucket_name\": \""
                + bucketName + "\", \"base_path\": \"" + BASE_PATH + "\" } "
                + "}";
    }

    @Override
    protected void assertRegisterRepository(final String responseContent) {
        assertEquals(
                "{backup={settings={bucket_name=" + bucketName + ", base_path=" + BASE_PATH + "}, "
                        + "type=" + S3RepositoryPlugin.REPOSITORY_TYPE + "}}", responseContent
        );
    }

    @Override
    public void assertCreatedSnapshot() {
        final var metadataBlob = amazonS3.doesObjectExist(bucketName,
                BASE_PATH + "/" + RepositoryStorageIOProvider.REPOSITORY_METADATA_FILE_NAME);
        assertTrue(metadataBlob);

        final var listOfFilesIterator = listOfFileForBackupFolder(listObjectsRequestFor(BASE_PATH));
        assertNotNull(listOfFilesIterator);
        assertTrue(countElementsInBackup(listOfFilesIterator) > 0);
    }

    @Override
    public void assertDeletionOfRepository() {
        //The repository with metadata files themselves are left untouched and in place
        final var indices =
                listOfFileForBackupFolder(listObjectsRequestFor(BASE_PATH + "/indices/"));
        assertNotNull(indices);
        assertFalse(countElementsInBackup(indices) > 0);
    }

    private static ListObjectsV2Request listObjectsRequestFor(final String path) {
        return new ListObjectsV2Request()
                .withBucketName(bucketName)
                .withPrefix(path);
    }

    private static List<String> listOfFileForBackupFolder(final ListObjectsV2Request listObjectsRequest) {
        return listFor(listObjectsRequest).stream()
                .map(S3ObjectSummary::getKey)
                .collect(Collectors.toList());
    }

    private static List<S3ObjectSummary> listFor(final ListObjectsV2Request listObjectRequest) {
        final var objectSummariesList = new ArrayList<S3ObjectSummary>();
        ListObjectsV2Result listing;
        do {
            listing = amazonS3.listObjectsV2(listObjectRequest);
            final var objectSummaries = listing.getObjectSummaries();
            objectSummariesList.addAll(objectSummaries);
            final var nextToken = listing.getNextContinuationToken();
            listObjectRequest.setContinuationToken(nextToken);
        } while (listing.isTruncated());
        return objectSummariesList;
    }

    private int countElementsInBackup(final Iterable<String> listIterator) {
        var elemCounter = 0;
        for (final var b : listIterator) {
            elemCounter++;
        }
        return elemCounter;
    }

}
