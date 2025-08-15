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

import org.opensearch.common.settings.SecureSettings;
import org.opensearch.common.settings.Settings;

import io.aiven.elasticsearch.repositories.DummySecureSettings;
import io.aiven.elasticsearch.repositories.RsaKeyAwareTest;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class GcsClientSettingsClientSpecificTest extends RsaKeyAwareTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(GcsClientSettingsClientSpecificTest.class);

    @Test
    void testClientSpecificSettings() throws Exception {
        // Create settings with client-specific configuration
        // This test should use client-specific keystore keys if available, fall back to default
        final var settings = Settings.builder()
                .put("client", "myclient")
                .put(GcsClientSettings.PROJECT_ID.getKey(), "myclient_project")
                .put(GcsClientSettings.CONNECTION_TIMEOUT.getKey(), "5000")
                .put(GcsClientSettings.READ_TIMEOUT.getKey(), "10000")
                .setSecureSettings(
                        createSecureSettingsWithFallback("myclient")
                ).build();

        final var gcsClientSettings = GcsClientSettings.create(settings);
        
        // Verify that client-specific settings are used
        assertEquals("myclient", gcsClientSettings.getClientName());
        assertEquals("myclient_project", gcsClientSettings.projectId());
        assertEquals(5000, gcsClientSettings.connectionTimeout());
        assertEquals(10000, gcsClientSettings.readTimeout());
        
        // Verify that credentials are loaded
        assertNotNull(gcsClientSettings.gcsCredentials());
    }

    @Test
    void testDefaultClientSettings() throws Exception {
        // Create settings without client specification (should use default)
        final var settings = Settings.builder()
                .put(GcsClientSettings.PROJECT_ID.getKey(), "default_project")
                .put(GcsClientSettings.CONNECTION_TIMEOUT.getKey(), "3000")
                .put(GcsClientSettings.READ_TIMEOUT.getKey(), "8000")
                .setSecureSettings(
                        createSecureSettings(
                                getClass().getClassLoader().getResourceAsStream("test_gcs_creds.json"),
                                java.nio.file.Files.newInputStream(publicKeyPem),
                                java.nio.file.Files.newInputStream(privateKeyPem)
                        )
                ).build();

        final var gcsClientSettings = GcsClientSettings.create(settings);
        
        // Verify that default settings are used
        assertEquals("default", gcsClientSettings.getClientName());
        assertEquals("default_project", gcsClientSettings.projectId());
        assertEquals(3000, gcsClientSettings.connectionTimeout());
        assertEquals(8000, gcsClientSettings.readTimeout());
        
        // Verify that credentials are loaded
        assertNotNull(gcsClientSettings.gcsCredentials());
    }

    @Test
    void testClientSpecificKeystoreKeys() throws Exception {
        // Create settings with client-specific keystore keys
        // This should now work with the dynamic client-specific keystore functionality
        final var settings = Settings.builder()
                .put("client", "testclient")
                .put(GcsClientSettings.PROJECT_ID.getKey(), "myclient_project")
                .put(GcsClientSettings.CONNECTION_TIMEOUT.getKey(), "5000")
                .put(GcsClientSettings.READ_TIMEOUT.getKey(), "10000")
                .setSecureSettings(
                        createClientSpecificSecureSettings("testclient")
                ).build();

        final var gcsClientSettings = GcsClientSettings.create(settings);
        
        // Verify that client-specific settings are used
        assertEquals("testclient", gcsClientSettings.getClientName());
        assertEquals("myclient_project", gcsClientSettings.projectId());
        assertEquals(5000, gcsClientSettings.connectionTimeout());
        assertEquals(10000, gcsClientSettings.readTimeout());
        
        // Verify that credentials are loaded from client-specific keystore keys
        assertNotNull(gcsClientSettings.gcsCredentials());
    }

    @Test
    void testClientParameterExtraction() throws Exception {
        // Test that the client parameter is correctly extracted from repository settings
        final var settings = Settings.builder()
                .put("client", "testclient")
                .put(GcsClientSettings.PROJECT_ID.getKey(), "myclient_project")
                .build();

        final var clientName = GcsClientSettings.CLIENT_NAME.get(settings);
        assertEquals("testclient", clientName);
        
        // Verify that this matches what the plugin would see
        LOGGER.info("Client name extracted: {}", clientName);
    }

    SecureSettings createSecureSettings(final java.io.InputStream googleCredential,
                                        final java.io.InputStream publicKey,
                                        final java.io.InputStream privateKey) throws IOException {
        return new DummySecureSettings()
                .setFile(GcsClientSettings.CREDENTIALS_FILE_SETTING.getKey(), googleCredential)
                .setFile(GcsClientSettings.PUBLIC_KEY_FILE.getKey(), publicKey)
                .setFile(GcsClientSettings.PRIVATE_KEY_FILE.getKey(), privateKey);
    }

    SecureSettings createClientSpecificSecureSettings(final String clientName) throws IOException {
        final String clientPrefix = "aiven.gcs.client." + clientName + ".";
        
        return new DummySecureSettings()
                .setFile(clientPrefix + "credentials_file", 
                         getClass().getClassLoader().getResourceAsStream("test_gcs_creds.json"))
                .setFile(clientPrefix + "public_key_file", 
                         java.nio.file.Files.newInputStream(publicKeyPem))
                .setFile(clientPrefix + "private_key_file", 
                         java.nio.file.Files.newInputStream(privateKeyPem));
    }
    
    SecureSettings createSecureSettingsWithFallback(final String clientName) throws IOException {
        final String clientPrefix = "aiven.gcs.client." + clientName + ".";
        
        return new DummySecureSettings()
                // Client-specific keystore keys
                .setFile(clientPrefix + "credentials_file", 
                         getClass().getClassLoader().getResourceAsStream("test_gcs_creds.json"))
                .setFile(clientPrefix + "public_key_file", 
                         java.nio.file.Files.newInputStream(publicKeyPem))
                .setFile(clientPrefix + "private_key_file", 
                         java.nio.file.Files.newInputStream(privateKeyPem))
                // Default keystore keys as fallback
                .setFile(GcsClientSettings.CREDENTIALS_FILE_SETTING.getKey(), 
                         getClass().getClassLoader().getResourceAsStream("test_gcs_creds.json"))
                .setFile(GcsClientSettings.PUBLIC_KEY_FILE.getKey(), 
                         java.nio.file.Files.newInputStream(publicKeyPem))
                .setFile(GcsClientSettings.PRIVATE_KEY_FILE.getKey(), 
                         java.nio.file.Files.newInputStream(privateKeyPem));
    }
}
