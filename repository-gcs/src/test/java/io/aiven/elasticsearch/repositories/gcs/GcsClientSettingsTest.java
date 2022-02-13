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

import io.aiven.elasticsearch.repositories.DummySecureSettings;
import io.aiven.elasticsearch.repositories.RsaKeyAwareTest;

import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
    void throwsIllegalArgumentExceptionForEmptyCredentials() {
        final var e =
                assertThrows(
                        IllegalArgumentException.class, () -> GcsClientSettings.create(Settings.EMPTY));

        assertEquals(
                "Settings for GC storage hasn't been set",
                e.getMessage()
        );
    }

    Settings.Builder createSettings(final String clientName) {
        return Settings.builder()
                .put(GcsClientSettings.CONNECTION_TIMEOUT.getConcreteSettingForNamespace(clientName).getKey(), 1)
                .put(GcsClientSettings.READ_TIMEOUT.getConcreteSettingForNamespace(clientName).getKey(), 2)
                .put(GcsClientSettings.PROJECT_ID.getConcreteSettingForNamespace(clientName).getKey(), "some_project");
    }

    SecureSettings createSecureSettings(final String clientName,
                                        final InputStream googleCredential,
                                        final InputStream publicKey,
                                        final InputStream privateKey) throws IOException {
        return new DummySecureSettings()
                .setFile(
                        GcsClientSettings.CREDENTIALS_FILE_SETTING.getConcreteSettingForNamespace(clientName).getKey(),
                        googleCredential
                ).setFile(
                        GcsClientSettings.PUBLIC_KEY_FILE.getConcreteSettingForNamespace(clientName).getKey(),
                        publicKey
                ).setFile(
                        GcsClientSettings.PRIVATE_KEY_FILE.getConcreteSettingForNamespace(clientName).getKey(),
                        privateKey
                );

    }

}
