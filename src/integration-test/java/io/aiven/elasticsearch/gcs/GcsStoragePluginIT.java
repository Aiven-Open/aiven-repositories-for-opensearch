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

package io.aiven.elasticsearch.gcs;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import io.aiven.elasticsearch.gcs.storage.GcsBlobStoreRepository;
import io.aiven.elasticsearch.gcs.utils.DummySecureSettings;
import io.aiven.elasticsearch.storage.RsaKeyAwareTest;
import io.aiven.elasticsearch.storage.security.EncryptionKeyProvider;

import com.google.auth.oauth2.UserCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.net.HttpHeaders;
import com.google.common.net.MediaType;
import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner;
import org.codelibs.elasticsearch.runner.net.EcrCurl;
import org.elasticsearch.common.settings.Settings;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import static org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner.newConfigs;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class GcsStoragePluginIT extends RsaKeyAwareTest {

    static final String INDEX = "index_42";

    static final String BASE_PATH = "test_backup";

    static ElasticsearchClusterRunner clusterRunner;

    private static Path gcsCredentialsPath;

    private static String bucketName;

    @BeforeAll
    static void setUp() {
        gcsCredentialsPath = Paths.get(System.getProperty("integration-test.gcs.credentials.path"));
        bucketName = System.getProperty("integration-test.gcs.bucket.name");

        clusterRunner = new ElasticsearchClusterRunner();
        clusterRunner.onBuild((index, builder) -> {
            final var securitySettings = new DummySecureSettings();
            try {
                securitySettings
                        .setFile(GcsStorageSettings.CREDENTIALS_FILE_SETTING.getKey(),
                                Files.newInputStream(gcsCredentialsPath))
                        .setFile(EncryptionKeyProvider.PUBLIC_KEY_FILE.getKey(), Files.newInputStream(publicKeyPem))
                        .setFile(EncryptionKeyProvider.PRIVATE_KEY_FILE.getKey(), Files.newInputStream(privateKeyPem));
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
            builder.setSecureSettings(securitySettings);
        }).build(
                newConfigs()
                        .clusterName("TestCluster")
                        .numOfNode(1)
                        .pluginTypes("io.aiven.elasticsearch.gcs.GcsStoragePlugin")
        );
        clusterRunner.ensureYellow();
        clusterRunner.createIndex(INDEX, (Settings) null);
        clusterRunner.ensureYellow(INDEX);
    }

    @AfterAll
    static void tearDown() throws IOException {
        try {
            clusterRunner.close();
            clusterRunner.clean();
        } finally {
            clearBucket();
        }
    }

    static void clearBucket() throws IOException {

        final var credentials =
                UserCredentials.fromStream(
                        Files.newInputStream(gcsCredentialsPath)
                );

        final var storage = StorageOptions.newBuilder()
                .setCredentials(credentials)
                .build()
                .getService();

        final var batch = storage.batch();
        storage.get(bucketName)
                .list(Storage.BlobListOption.prefix(BASE_PATH + "/"))
                .iterateAll()
                .forEach(blob -> batch.delete(blob.getBlobId()));
        batch.submit();
    }


    @Test
    @Order(1)
    void registerRepository() throws Exception {
        final var node = clusterRunner.getNode(0);

        final var enableRepositoryRequest =
                "{ \"type\": \""
                        + GcsBlobStoreRepository.TYPE + "\", "
                        + "\"settings\": { \"bucket_name\": \""
                        + bucketName + "\", \"base_path\": \"test_backup\" } "
                        + "}";
        try (final var response = EcrCurl.put(node, "/_snapshot/backup")
                .header(HttpHeaders.CONTENT_TYPE, MediaType.JSON_UTF_8.toString())
                .body(enableRepositoryRequest)
                .execute()) {
            assertEquals(200, response.getHttpStatusCode());
        }

        try (final var response = EcrCurl.get(node, "_snapshot/backup").execute()) {
            assertEquals(200, response.getHttpStatusCode());
            final Map<String, Object> content = response.getContent(EcrCurl.jsonParser());
            assertEquals(
                    "{backup={settings={bucket_name=ples-test, base_path=" + BASE_PATH + "}, type=gcs-encrypted}}",
                    content.toString()
            );
        }

    }

    @Test
    @Order(2)
    void createSnapshot() throws Exception {
        final var node = clusterRunner.getNode(0);

        for (int i = 0; i < 10_000; i++) {
            try (final var curlResponse = EcrCurl.post(node, "/" + INDEX + "/_doc/")
                    .header("Content-Type", "application/json")
                    .body("{\"id\":\"200" + i + "\",\"msg\":\"test 200" + i + "\"}")
                    .execute()) {
                final Map<String, Object> content = curlResponse.getContent(EcrCurl.jsonParser());
                assertNotNull(content);
                assertEquals("created", content.get("result"));
            }
        }

        try (final var response =
                     EcrCurl.put(node, "/_snapshot/backup/snapshot_1?wait_for_completion=true")
                             .execute()) {
            assertEquals(200, response.getHttpStatusCode());
        }
    }

    @Test
    @Order(3)
    void restoreSnapshot() throws Exception {
        final var node = clusterRunner.getNode(0);
        try (final var response = EcrCurl.get(node, "/_snapshot/backup/snapshot_1").execute()) {
            assertEquals(200, response.getHttpStatusCode());
        }
    }

    @Test
    @Order(4)
    void deleteRepository() throws Exception {
        final var node = clusterRunner.getNode(0);

        try (final var response = EcrCurl.delete(node, "_snapshot/backup").execute()) {
            assertEquals(200, response.getHttpStatusCode());
        }
    }


}
