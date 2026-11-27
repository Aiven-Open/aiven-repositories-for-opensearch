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
import java.io.InputStream;
import java.nio.file.Files;

import org.opensearch.common.settings.SecureSettings;
import org.opensearch.common.settings.Settings;

import io.aiven.elasticsearch.repositories.DummySecureSettings;
import io.aiven.elasticsearch.repositories.RsaKeyAwareTest;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GcsClientSettingsTest extends RsaKeyAwareTest {

    @Test
    void loadSettings() throws Exception {
        final var settings = createSettings("default")
                .setSecureSettings(
                        createSecureSettings(
                                "default",
                                getClass().getClassLoader().getResourceAsStream("test_gcs_creds.json"),
                                Files.newInputStream(publicKeyPem),
                                Files.newInputStream(privateKeyPem)
                        )
                ).build();

        final var gcsClientSettings = GcsClientSettings.create(settings).get("default");
        assertEquals("some_project", gcsClientSettings.projectId());
        assertEquals(1, gcsClientSettings.connectionTimeout());
        assertEquals(2, gcsClientSettings.readTimeout());

        assertNotNull(gcsClientSettings.gcsCredentials());
    }

    @Test
    void loadNonDefaultClientSettings() throws IOException {
        final var settings = createSettings("some_client")
                .setSecureSettings(
                        createSecureSettings(
                                "some_client",
                                getClass().getClassLoader().getResourceAsStream("test_gcs_creds.json"),
                                Files.newInputStream(publicKeyPem),
                                Files.newInputStream(privateKeyPem)
                        )
                ).build();

        final var gcsClientSettings = GcsClientSettings.create(settings).get("some_client");
        assertEquals("some_project", gcsClientSettings.projectId());
        assertEquals(1, gcsClientSettings.connectionTimeout());
        assertEquals(2, gcsClientSettings.readTimeout());

        assertNotNull(gcsClientSettings.gcsCredentials());
    }

    @Test
    void uploadChunkSizeStaysUnderHalfOfLargeRegions() {
        assertEquals(mib(3.75), GcsClientSettings.uploadChunkSizeFor(mib(8)));
        assertEquals(mib(7.75), GcsClientSettings.uploadChunkSizeFor(mib(16)));
        assertEquals(mib(15.75), GcsClientSettings.uploadChunkSizeFor(mib(32)));
        assertEquals(mib(16), GcsClientSettings.uploadChunkSizeFor(mib(64)));
    }

    @Test
    void uploadChunkSizeUsesFloorForSmallRegions() {
        assertEquals(mib(3.75), GcsClientSettings.uploadChunkSizeFor(mib(1)));
        assertEquals(mib(3.75), GcsClientSettings.uploadChunkSizeFor(mib(2)));
        assertEquals(mib(3.75), GcsClientSettings.uploadChunkSizeFor(mib(4)));
    }

    @Test
    void uploadChunkSizeDefaultsToSdkDefaultWithoutG1() {
        assertEquals(mib(16), GcsClientSettings.uploadChunkSizeFor(0));
    }

    @Test
    void uploadChunkSizeDefaultsToRuntimeValue() throws IOException {
        final var settings = createSettings("default")
                .setSecureSettings(
                        createSecureSettings(
                                "default",
                                getClass().getClassLoader().getResourceAsStream("test_gcs_creds.json"),
                                Files.newInputStream(publicKeyPem),
                                Files.newInputStream(privateKeyPem)
                        )
                ).build();

        final var uploadChunkSize = GcsClientSettings.create(settings).get("default").uploadChunkSize();

        assertEquals(GcsClientSettings.UPLOAD_CHUNK_SIZE.getConcreteSettingForNamespace("default").get(Settings.EMPTY)
                .getBytes(), uploadChunkSize);
        assertTrue(uploadChunkSize >= GcsClientSettings.MIN_UPLOAD_CHUNK_SIZE);
        assertTrue(uploadChunkSize <= GcsClientSettings.MAX_UPLOAD_CHUNK_SIZE);
    }

    @Test
    void uploadChunkSizeCanBeSet() throws IOException {
        final var settings = createSettings("default")
                .put(GcsClientSettings.UPLOAD_CHUNK_SIZE.getConcreteSettingForNamespace("default").getKey(), "8mb")
                .setSecureSettings(
                        createSecureSettings(
                                "default",
                                getClass().getClassLoader().getResourceAsStream("test_gcs_creds.json"),
                                Files.newInputStream(publicKeyPem),
                                Files.newInputStream(privateKeyPem)
                        )
                ).build();

        assertEquals(mib(8), GcsClientSettings.create(settings).get("default").uploadChunkSize());
    }

    private static int mib(final double mib) {
        return (int) (mib * 1024 * 1024);
    }

    @Test
    void throwsIllegalArgumentExceptionForEmptyCredentials() {
        final var e =
                assertThrows(
                        IllegalArgumentException.class, () -> GcsClientSettings.create(Settings.EMPTY));

        assertEquals(
                "Settings for GC storage hasn't been set",
                e.getMessage()
        );
    }

    public static Settings.Builder createSettings(final String clientName) {
        return createSettings(Settings.builder(), clientName, "some_project");
    }

    public static Settings.Builder createSettings(final Settings.Builder builder, final String clientName, final String projectId) {
        return builder
                .put(GcsClientSettings.CONNECTION_TIMEOUT.getConcreteSettingForNamespace(clientName).getKey(), 1)
                .put(GcsClientSettings.READ_TIMEOUT.getConcreteSettingForNamespace(clientName).getKey(), 2)
                .put(GcsClientSettings.PROJECT_ID.getConcreteSettingForNamespace(clientName).getKey(), projectId);
    }

    public static SecureSettings createSecureSettings(final String clientName,
                                        final InputStream googleCredential,
                                        final InputStream publicKey,
                                        final InputStream privateKey) throws IOException {
        return createSecureSettings(new DummySecureSettings(), clientName, googleCredential, publicKey, privateKey);
    }

    public static SecureSettings createSecureSettings(final DummySecureSettings settings,
                                                      final String clientName,
                                                      final InputStream googleCredential,
                                                      final InputStream publicKey,
                                                      final InputStream privateKey) throws IOException {
        return settings.setFile(GcsClientSettings.CREDENTIALS_FILE_SETTING.getConcreteSettingForNamespace(clientName).getKey(),
                                googleCredential)
                       .setFile(GcsClientSettings.PUBLIC_KEY_FILE.getConcreteSettingForNamespace(clientName).getKey(),
                                publicKey)
                       .setFile(GcsClientSettings.PRIVATE_KEY_FILE.getConcreteSettingForNamespace(clientName).getKey(),
                                privateKey);
    }

}
