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

package io.aiven.elasticsearch.repositories.gcs;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import io.aiven.elasticsearch.repositories.AbstractRepositoryPluginIT;
import io.aiven.elasticsearch.repositories.DummySecureSettings;
import io.aiven.elasticsearch.repositories.RepositoryStorageIOProvider;

import com.google.auth.oauth2.UserCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GcsRepositoryPluginIT extends AbstractRepositoryPluginIT {

    private static String bucketName;

    private static Storage storage;

    @BeforeAll
    static void setUp() throws IOException {
        final var gcsCredentialsPath = Paths.get(System.getProperty("integration-test.gcs.credentials.path"));
        bucketName = System.getProperty("integration-test.gcs.bucket.name");
        final var credentials =
                UserCredentials.fromStream(Files.newInputStream(gcsCredentialsPath));

        storage = StorageOptions.newBuilder()
                .setCredentials(credentials)
                .build()
                .getService();

        try {
            configureAndStartCluster(GcsRepositoryPlugin.class, settingsBuilder ->
                    settingsBuilder.setSecureSettings(
                            new DummySecureSettings()
                                    .setFile(
                                            GcsClientSettings.CREDENTIALS_FILE_SETTING.getConcreteSettingForNamespace("default").getKey(),
                                            Files.newInputStream(gcsCredentialsPath))
                                    .setFile(
                                            GcsClientSettings.PUBLIC_KEY_FILE.getConcreteSettingForNamespace("default").getKey(),
                                            Files.newInputStream(publicKeyPem))
                                    .setFile(
                                            GcsClientSettings.PRIVATE_KEY_FILE.getConcreteSettingForNamespace("default").getKey(),
                                            Files.newInputStream(privateKeyPem))));
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    @AfterAll
    static void tearDown() throws IOException {
        try {
            shutdownCluster();
        } finally {
            clearBucket();
        }
    }

    static void clearBucket() {
        final var batch = storage.batch();
        listOfFileForBackupFolder()
                .forEach(blob -> batch.delete(blob.getBlobId()));
        batch.submit();
    }

    private static Iterable<Blob> listOfFileForBackupFolder() {
        return storage.get(bucketName)
                .list(Storage.BlobListOption.prefix(BASE_PATH + "/"))
                .iterateAll();
    }

    @Override
    public String registerRepositoryJson() {
        return "{ \"type\": \""
                + GcsRepositoryPlugin.REPOSITORY_TYPE + "\", "
                + "\"settings\": { \"bucket_name\": \""
                + bucketName + "\", \"base_path\": \"" + BASE_PATH + "\" } "
                + "}";
    }

    @Override
    protected void assertRegisterRepository(final String responseContent) {
        assertEquals(
                "{backup={settings={bucket_name=" + bucketName + ", base_path=" + BASE_PATH + "}, "
                        + "type=" + GcsRepositoryPlugin.REPOSITORY_TYPE + "}}", responseContent
        );
    }

    @Override
    public void assertCreatedSnapshot() {
        final var metadataBlob =
                storage.get(
                        BlobId.of(
                                bucketName,
                                BASE_PATH + "/" + RepositoryStorageIOProvider.REPOSITORY_METADATA_FILE_NAME)
                );
        assertNotNull(metadataBlob);
        assertTrue(metadataBlob.exists());

        final var listOfFilesIterator = listOfFileForBackupFolder();
        assertNotNull(listOfFilesIterator);
        assertTrue(countElementsInBackup(listOfFilesIterator) > 0);
    }

    @Override
    public void assertDeletionOfRepository() {
        //The repository with metadata files themselves are left untouched and in place
        final var indices = storage.get(bucketName)
                .list(Storage.BlobListOption.prefix(BASE_PATH + "/indices/"))
                .iterateAll();


        assertNotNull(indices);
        assertFalse(countElementsInBackup(indices) > 0);
    }

    private int countElementsInBackup(final Iterable<Blob> blobIterable) {
        var elemCounter = 0;
        for (final var b : blobIterable) {
            elemCounter++;
        }
        return elemCounter;
    }

}
