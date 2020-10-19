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

package io.aiven.elasticsearch.repositories.azure;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.stream.Collectors;

import io.aiven.elasticsearch.repositories.AbstractRepositoryPluginIT;
import io.aiven.elasticsearch.repositories.DummySecureSettings;
import io.aiven.elasticsearch.repositories.RepositoryStorageIOProvider;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.ListBlobsOptions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AzureRepositoryPluginIT extends AbstractRepositoryPluginIT {

    static String containerName;

    static BlobContainerClient blobContainerClient;

    @BeforeAll
    static void setUp() throws Exception {
        containerName = System.getProperty("integration-test.azure.container.name");
        final var azureAccount = System.getProperty("integration-test.azure.account");
        final var azureAccountKey = System.getProperty("integration-test.azure.account.key");
        try {
            configureAndStartCluster(
                    AzureRepositoryPlugin.class, settingsBuilder ->
                            settingsBuilder
                                    .setSecureSettings(new DummySecureSettings()
                                            .setString(
                                                    AzureStorageSettings.AZURE_ACCOUNT.getKey(),
                                                    azureAccount)
                                            .setString(
                                                    AzureStorageSettings.AZURE_ACCOUNT_KEY.getKey(),
                                                    azureAccountKey)
                                            .setFile(
                                                    AzureStorageSettings.PUBLIC_KEY_FILE.getKey(),
                                                    Files.newInputStream(publicKeyPem))
                                            .setFile(
                                                    AzureStorageSettings.PRIVATE_KEY_FILE.getKey(),
                                                    Files.newInputStream(privateKeyPem))
                                    ));
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }

        final var blobServiceClient =
                new BlobServiceClientBuilder()
                        .connectionString(
                                String.format(
                                        AzureStorageSettings.AZURE_CONNECTION_STRING_TEMPLATE,
                                        azureAccount,
                                        azureAccountKey)
                        ).buildClient();
        blobContainerClient = blobServiceClient.getBlobContainerClient(containerName);
    }

    @AfterAll
    static void tearDown() throws IOException {
        try {
            shutdownCluster();
        } finally {
            listOfFileForBackupFolder(BASE_PATH)
                    .forEach(f -> blobContainerClient.getBlobClient(f).delete());
        }
    }

    @Override
    public String registerRepositoryJson() {
        return "{ \"type\": \""
                + AzureRepositoryPlugin.REPOSITORY_TYPE + "\", "
                + "\"settings\": { \"container_name\": \""
                + containerName + "\", \"base_path\": \"" + BASE_PATH + "\" } "
                + "}";
    }

    @Override
    protected void assertRegisterRepository(final String responseContent) {
        assertEquals(
                "{backup={settings={container_name=" + containerName + ", base_path=" + BASE_PATH + "}, "
                        + "type=" + AzureRepositoryPlugin.REPOSITORY_TYPE + "}}", responseContent
        );
    }

    @Override
    public void assertCreatedSnapshot() {
        final var metadataFile = BASE_PATH + "/" + RepositoryStorageIOProvider.REPOSITORY_METADATA_FILE_NAME;
        final var metadataBlob = blobContainerClient
                .getBlobClient(metadataFile)
                .exists();
        assertTrue(metadataBlob);

        final var listOfFilesIterator = listOfFileForBackupFolder(BASE_PATH);
        assertNotNull(listOfFilesIterator);
        assertTrue(countElementsInBackup(listOfFilesIterator) > 0);
    }

    @Override
    public void assertDeletionOfRepository() {
        //The repository with metadata files themselves are left untouched and in place
        final var indices =
                listOfFileForBackupFolder(BASE_PATH + "/indices/");

        assertNotNull(indices);
        assertFalse(countElementsInBackup(indices) > 0);
    }

    private static List<String> listOfFileForBackupFolder(final String path) {
        return blobContainerClient.listBlobsByHierarchy(null, new ListBlobsOptions().setPrefix(path), null)
                .stream()
                .map(BlobItem::getName)
                .collect(Collectors.toList());
    }

    private int countElementsInBackup(final Iterable<String> listIterator) {
        var elemCounter = 0;
        for (final var b : listIterator) {
            elemCounter++;
        }
        return elemCounter;
    }

}
