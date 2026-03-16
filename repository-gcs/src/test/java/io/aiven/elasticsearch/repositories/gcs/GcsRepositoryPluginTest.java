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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.Settings;

import io.aiven.elasticsearch.repositories.CommonSettings;
import io.aiven.elasticsearch.repositories.DummySecureSettings;
import io.aiven.elasticsearch.repositories.RepositorySettingsService;
import io.aiven.elasticsearch.repositories.RepositoryStorageIOProvider;
import io.aiven.elasticsearch.repositories.RsaKeyAwareTest;
import io.aiven.elasticsearch.repositories.io.CryptoIOProvider;

import com.google.cloud.storage.Storage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static io.aiven.elasticsearch.repositories.gcs.GcsClientSettingsTest.createSecureSettings;
import static io.aiven.elasticsearch.repositories.gcs.GcsClientSettingsTest.createSettings;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GcsRepositoryPluginTest extends RsaKeyAwareTest {
    private static final String CREDENTIALS_JSON = "test_gcs_creds.json";

    private GcsRepositoryPlugin plugin;
    private final String projectIdDefault = "test_project_default";
    private final String projectIdBtar = "test_project_btar";

    @TempDir
    private java.nio.file.Path tempDir;

    @BeforeEach
    public void setUp() throws Exception {
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (plugin != null) {
            plugin.close();
        }
        Files.deleteIfExists(tempDir.resolve(CREDENTIALS_JSON));
    }

    @Test
    public void testClientsAreNotSharedAcrossRepositories() throws Exception {
        final DummySecureSettings secureSettings = new DummySecureSettings();
        createSecureSettings(secureSettings, "btar",
                             new ByteArrayInputStream(serviceAccountFileContent(projectIdBtar)),
                             Files.newInputStream(publicKeyPem), Files.newInputStream(privateKeyPem));
        createSecureSettings(secureSettings, "default",
                             new ByteArrayInputStream(serviceAccountFileContent(projectIdDefault)),
                             Files.newInputStream(publicKeyPem), Files.newInputStream(privateKeyPem));

        final Settings.Builder raw = Settings.builder();
        createSettings(raw, "btar", projectIdBtar);
        createSettings(raw, "default", projectIdDefault);
        final Settings settings = raw.setSecureSettings(secureSettings).build();

        final Settings repositoryRepo1Settings = Settings.builder()
                                                         .put(CommonSettings.RepositorySettings.BASE_PATH.getKey(), "base_path/")
                                                         .put(CommonSettings.RepositorySettings.CLIENT_NAME.getKey(), "default")
                                                         .put(GcsRepositoryStorageIOProvider.BUCKET_NAME.getKey(), "bucket/name")
                                                         .build();
        final Settings repositoryRepo2Settings = Settings.builder()
                                                         .put(CommonSettings.RepositorySettings.BASE_PATH.getKey(), "base_path/")
                                                         .put(CommonSettings.RepositorySettings.CLIENT_NAME.getKey(), "default")
                                                         .put(GcsRepositoryStorageIOProvider.BUCKET_NAME.getKey(), "bucket/name")
                                                         .build();

        plugin = new TestGcsRepositoryPlugin(settings);

        final RepositorySettingsService<?, ?> service = plugin.getRepositorySettingsProvider();
        final var firstIo = service.createStorageIO("/", "repo1", repositoryRepo1Settings);
        final var secondIo = service.createStorageIO("/", "repo2", repositoryRepo2Settings);
        final var repoFirstIoSame = service.createStorageIO("/", "repo1", repositoryRepo1Settings);

        assertInstanceOf(TestStorageIO.class, firstIo);
        assertInstanceOf(TestStorageIO.class, secondIo);

        // Assert that the underlying clients are the same for the same repository, and different across repositories.
        assertNotSame(((TestStorageIO) firstIo).storage, ((TestStorageIO) secondIo).storage);
        assertSame(((TestStorageIO) firstIo).storage, ((TestStorageIO) repoFirstIoSame).storage);
    }

    @Test
    public void testRepositoryResourcesAreClosedOnPluginClose() throws Exception {
        final var secureSettings = new DummySecureSettings();
        createSecureSettings(secureSettings,
                             "default",
                             new ByteArrayInputStream(serviceAccountFileContent(projectIdDefault)),
                             Files.newInputStream(publicKeyPem),
                             Files.newInputStream(privateKeyPem));

        final var settings = createSettings("default").setSecureSettings(secureSettings).build();

        final var repositoryRepo1Settings = Settings.builder()
                                                    .put(CommonSettings.RepositorySettings.BASE_PATH.getKey(), "base_path/")
                                                    .put(CommonSettings.RepositorySettings.CLIENT_NAME.getKey(), "default")
                                                    .put(GcsRepositoryStorageIOProvider.BUCKET_NAME.getKey(), "bucket/name")
                                                    .build();

        plugin = new TestGcsRepositoryPlugin(settings);

        final RepositorySettingsService<?, ?> service = plugin.getRepositorySettingsProvider();
        service.createStorageIO("/", "repo1", repositoryRepo1Settings);
        service.createStorageIO("/", "repo1", repositoryRepo1Settings);

        plugin.close();
        // Further closing should be idempotent and not throw exceptions
        plugin.close();

        assertTrue(plugin.getRepositorySettingsProvider().getRepositoryStorages().isEmpty());
    }

    @Test
    public void testRepositorySettingsBackwardsCompatibility() throws Exception {
        // All settings must fall back to the "default" client if not explicitly set otherwise.
        final Path credentialsFile = tempDir.resolve(CREDENTIALS_JSON);
        Files.write(credentialsFile, serviceAccountFileContent(projectIdDefault));

        final var settings = Settings.builder()
                                     .put("aiven.gcs.client.default.project_id", projectIdDefault)
                                     .put("aiven.gcs.client.default.connection_timeout", 1000)
                                     .put("aiven.gcs.client.default.read_timeout", 10)
                                     .put("aiven.gcs.client.default.proxy.host", "localhost")
                                     .put("aiven.gcs.client.default.proxy.port", 8080)
                                     .setSecureSettings(new DummySecureSettings()
                                                            .setFile("aiven.gcs.client.default.private_key_file",
                                                                     Files.newInputStream(privateKeyPem))
                                                            .setFile("aiven.gcs.client.default.public_key_file",
                                                                     Files.newInputStream(publicKeyPem))
                                                            .setFile("aiven.gcs.client.default.credentials_file",
                                                                     Files.newInputStream(credentialsFile))
                                                            .setString("aiven.gcs.client.default.proxy.user_name", "usr")
                                                            .setString("aiven.gcs.client.default.proxy.user_password", "pwd"))
                                     .build();
        plugin = new TestGcsRepositoryPlugin(settings);
        assertNotNull(plugin); // This mean that plugin initialized without issues.

        final var gcsClientSettings = plugin.getRepositorySettingsProvider()
                                            .getClientsSettings()
                                            .get("default");
        assertNotNull(gcsClientSettings);
        assertEquals(projectIdDefault, gcsClientSettings.projectId());
        // The default value is -1, so if we get 1000, it means that the setting was picked up correctly.
        assertEquals(1000, gcsClientSettings.connectionTimeout());
        assertEquals(10, gcsClientSettings.readTimeout());
        assertEquals("usr", gcsClientSettings.getProxyUsername());
        assertEquals("pwd", new String(gcsClientSettings.getProxyUserPassword()));
    }

    private static byte[] serviceAccountFileContent(final String projectId) {
        try {
            final var keyPairGenerator = java.security.KeyPairGenerator.getInstance("RSA");
            keyPairGenerator.initialize(1024);
            final var keyPair = keyPairGenerator.generateKeyPair();
            final var encodedKey = Base64.getEncoder().encodeToString(keyPair.getPrivate().getEncoded());
            
            final var serviceAccountJson = String.format(
                "{\n"
                + "  \"type\": \"service_account\",\n"
                + "  \"project_id\": \"%s\",\n"
                + "  \"private_key_id\": \"%s\",\n"
                + "  \"private_key\": \"-----BEGIN PRIVATE KEY-----\\n%s\\n-----END PRIVATE KEY-----\\n\",\n"
                + "  \"client_email\": \"integration_test@appspot.gserviceaccount.com\",\n"
                + "  \"client_id\": \"client_id\"\n"
                + "}",
                projectId,
                UUID.randomUUID(),
                encodedKey
            );
            
            return serviceAccountJson.getBytes();
        } catch (final Exception e) {
            throw new RuntimeException("Failed to create service account content", e);
        }
    }

    private static class TestGcsRepositoryPlugin extends GcsRepositoryPlugin {
        public TestGcsRepositoryPlugin(final Settings settings) {
            super(settings);
        }

        protected RepositorySettingsService<Storage, GcsClientSettings> createRepositorySettingsService() {
            return new TestGcsSettingsProvider();
        }

    }

    private static class TestGcsSettingsProvider extends GcsSettingsProvider {
        @Override
        protected RepositoryStorageIOProvider<Storage, GcsClientSettings> createRepositoryStorageIOProvider() {
            return new TestGcsRepositoryStorageIOProvider();
        }
    }

    private static class TestGcsRepositoryStorageIOProvider extends GcsRepositoryStorageIOProvider {
        @Override
        protected StorageIO createStorageIOFor(final Storage storage,
                                               final Settings repositorySettings,
                                               final CryptoIOProvider cryptoIOProvider) {
            return new TestStorageIO(storage);
        }
    }

    private static class TestStorageIO implements RepositoryStorageIOProvider.StorageIO {
        private final Storage storage;

        public TestStorageIO(final Storage storage) {
            this.storage = storage;
        }

        @Override
        public boolean exists(final String blobName) throws IOException {
            return false;
        }

        @Override
        public InputStream read(final String blobName) throws IOException {
            return null;
        }

        @Override
        public void write(final String blobName,
                          final InputStream inputStream,
                          final long blobSize,
                          final boolean failIfAlreadyExists) throws IOException {
        }

        @Override
        public Tuple<Integer, Long> deleteDirectories(final String path) throws IOException {
            return null;
        }

        @Override
        public void deleteFiles(final List<String> blobNames, final boolean ignoreIfNotExists) throws IOException {

        }

        @Override
        public List<String> listDirectories(final String path) throws IOException {
            return List.of();
        }

        @Override
        public Map<String, Long> listFiles(final String path, final String prefix) throws IOException {
            return Map.of();
        }
    }
}
