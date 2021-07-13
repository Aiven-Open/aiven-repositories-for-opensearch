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

package io.aiven.elasticsearch.repositories;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

import org.apache.http.HttpHeaders;
import org.apache.http.entity.ContentType;
import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner;
import org.codelibs.elasticsearch.runner.net.EcrCurl;
import org.opensearch.common.settings.Settings;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner.newConfigs;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public abstract class AbstractRepositoryPluginIT extends RsaKeyAwareTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRepositoryPluginIT.class);

    static final String INDEX = "index_42";

    public static final String BASE_PATH = "test_backup";

    static ElasticsearchClusterRunner clusterRunner;

    protected static Path credentialsPath;

    @FunctionalInterface
    public interface SettingsBuilder {

        void accept(final Settings.Builder builder) throws IOException;

    }

    public static <T> void configureAndStartCluster(final Class<? extends AbstractRepositoryPlugin<T>> pluginClass,
                                                    final SettingsBuilder settingsHandler) throws Exception {
        clusterRunner = new ElasticsearchClusterRunner();
        clusterRunner.onBuild((index, builder) -> {
            try {
                settingsHandler.accept(builder);
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        }).build(
                newConfigs()
                        .clusterName("TestCluster")
                        .numOfNode(3)
                        .pluginTypes(pluginClass.getCanonicalName())
        );
        clusterRunner.ensureYellow();
        clusterRunner.createIndex(INDEX, (Settings) null);
        clusterRunner.ensureYellow(INDEX);
    }

    public static void shutdownCluster() throws IOException {
        clusterRunner.close();
        clusterRunner.clean();
    }

    @Test
    @Order(1)
    void registerRepository() throws Exception {
        final var enableRepositoryRequest = registerRepositoryJson();
        LOGGER.info("Register repository with request {}", enableRepositoryRequest);
        try (final var response = EcrCurl.put(clusterRunner.masterNode(), "/_snapshot/backup")
                .header(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType())
                .body(enableRepositoryRequest)
                .execute()) {
            assertEquals(200, response.getHttpStatusCode());
        }

        try (final var response = EcrCurl.get(clusterRunner.masterNode(), "_snapshot/backup").execute()) {
            assertEquals(200, response.getHttpStatusCode());
            assertRegisterRepository(response.getContent(EcrCurl.jsonParser()).toString());
        }
    }


    protected abstract void assertRegisterRepository(final String responseContent);

    @Test
    @Order(2)
    void createSnapshot() throws Exception {
        LOGGER.info("Create snapshot for index {}", INDEX);
        for (int i = 0; i < 10; i++) {
            try (final var curlResponse = EcrCurl.post(clusterRunner.masterNode(), "/" + INDEX + "/_doc/")
                    .header("Content-Type", ContentType.APPLICATION_JSON.getMimeType())
                    .body("{\"id\":\"200" + i + "\",\"msg\":\"test 200" + i + "\"}")
                    .execute()) {
                final Map<String, Object> content = curlResponse.getContent(EcrCurl.jsonParser());
                assertNotNull(content);
                assertEquals("created", content.get("result"));
            }
        }
        try (final var response =
                     EcrCurl.put(clusterRunner.masterNode(), "/_snapshot/backup/snapshot_1?wait_for_completion=true")
                             .execute()) {
            assertEquals(200, response.getHttpStatusCode());
        }
        assertCreatedSnapshot();
    }

    @Test
    @Order(3)
    void restoreSnapshot() throws Exception {
        LOGGER.info("Restore snapshot for index {}", INDEX);
        clusterRunner.deleteIndex(INDEX);

        assertFalse(clusterRunner.indexExists(INDEX));

        try (final var response =
                     EcrCurl.post(
                             clusterRunner.masterNode(),
                             "/_snapshot/backup/snapshot_1/_restore?wait_for_completion=true"
                     ).execute()) {
            assertEquals(200, response.getHttpStatusCode());
        }

        assertTrue(clusterRunner.indexExists(INDEX));
    }

    @Test
    @Order(4)
    void deleteRepositorySnapshot() throws Exception {
        LOGGER.info("Delete snapshot for index {}", INDEX);
        //should remove all indexes data for the snapshot_1
        try (final var response =
                     EcrCurl.delete(
                             clusterRunner.masterNode(),
                             "_snapshot/backup/snapshot_1")
                             .execute()) {

            assertEquals(200, response.getHttpStatusCode());
        }

        assertDeletionOfRepository();
    }

    public abstract String registerRepositoryJson();

    public abstract void assertCreatedSnapshot();

    public abstract void assertDeletionOfRepository();

}
