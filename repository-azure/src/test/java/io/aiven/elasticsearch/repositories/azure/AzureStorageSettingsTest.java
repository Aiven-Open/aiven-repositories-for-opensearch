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

import io.aiven.elasticsearch.repositories.DummySecureSettings;
import io.aiven.elasticsearch.repositories.RsaKeyAwareTest;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AzureStorageSettingsTest extends RsaKeyAwareTest {

    @Test
    void failsForEmptyPublicKey() throws IOException {
        final var settings =
                Settings.builder()
                        .setSecureSettings(
                                new DummySecureSettings()
                                        .setString(AzureStorageSettings.AZURE_ACCOUNT.getKey(), "some_account")
                                        .setString(AzureStorageSettings.AZURE_ACCOUNT_KEY.getKey(), "some_account_key")
                                        .setFile(
                                                AzureStorageSettings.PRIVATE_KEY_FILE.getKey(),
                                                Files.newInputStream(privateKeyPem)
                                        )
                        ).build();

        final var t = assertThrows(
                IllegalArgumentException.class, () -> AzureStorageSettings.create(settings));
        assertEquals("Settings with name aiven.azure.public_key_file hasn't been set", t.getMessage());
    }

    @Test
    void failsForEmptyPrivateKey() throws IOException {
        final var settings =
                Settings.builder()
                        .setSecureSettings(
                                new DummySecureSettings()
                                        .setString(AzureStorageSettings.AZURE_ACCOUNT.getKey(), "some_account")
                                        .setString(
                                                AzureStorageSettings.AZURE_ACCOUNT_KEY.getKey(),
                                                "some_account_key"
                                        )
                                        .setFile(
                                                AzureStorageSettings.PUBLIC_KEY_FILE.getKey(),
                                                Files.newInputStream(publicKeyPem)
                                        )
                        ).build();

        final var t = assertThrows(
                IllegalArgumentException.class, () -> AzureStorageSettings.create(settings));
        assertEquals("Settings with name aiven.azure.private_key_file hasn't been set", t.getMessage());
    }

    @Test
    void failsForEmptyAzureAccount() {
        final var secureSettings =
                new DummySecureSettings()
                        .setString(AzureStorageSettings.AZURE_ACCOUNT_KEY.getKey(), "some_key");

        final var t =
                assertThrows(IllegalArgumentException.class, () ->
                        AzureStorageSettings.create(Settings.builder()
                                .setSecureSettings(secureSettings).build()));

        assertEquals("Settings with name aiven.azure.client.account hasn't been set", t.getMessage());
    }

    @Test
    void failsForEmptyAzureAccountKey() {

        final var secureSettings =
                new DummySecureSettings()
                        .setString(AzureStorageSettings.AZURE_ACCOUNT.getKey(), "some_account");

        final var t =
                assertThrows(IllegalArgumentException.class, () ->
                        AzureStorageSettings.create(Settings.builder()
                                .setSecureSettings(secureSettings).build()));

        assertEquals("Settings with name aiven.azure.client.account.key hasn't been set", t.getMessage());
    }

    @Test
    void loadDefaultSettings() throws IOException {
        final var secureSettings =
                new DummySecureSettings()
                        .setString(AzureStorageSettings.AZURE_ACCOUNT.getKey(), "some_account")
                        .setString(AzureStorageSettings.AZURE_ACCOUNT_KEY.getKey(), "some_account_key")
                        .setFile(
                                AzureStorageSettings.PUBLIC_KEY_FILE.getKey(),
                                Files.newInputStream(publicKeyPem)
                        )
                        .setFile(
                                AzureStorageSettings.PRIVATE_KEY_FILE.getKey(),
                                Files.newInputStream(privateKeyPem)
                        );

        final var azureStorageSettings =
                AzureStorageSettings.create(Settings.builder().setSecureSettings(secureSettings).build());

        assertEquals("some_account", azureStorageSettings.azureAccount());
        assertEquals("some_account_key", azureStorageSettings.azureAccountKey());
        assertEquals(
                String.format(
                        AzureStorageSettings.AZURE_CONNECTION_STRING_TEMPLATE,
                        azureStorageSettings.azureAccount(),
                        azureStorageSettings.azureAccountKey()
                ), azureStorageSettings.azureConnectionString());
        assertEquals(3, azureStorageSettings.maxRetries());

        final var httpThreadPoolSettings = azureStorageSettings.httpThreadPoolSettings();
        assertEquals(1, httpThreadPoolSettings.minThreads());
        assertEquals(Runtime.getRuntime().availableProcessors() * 2 - 1, httpThreadPoolSettings.maxThreads());
        assertEquals(TimeValue.timeValueSeconds(30L).getMillis(), httpThreadPoolSettings.keepAlive());
    }

    @Test
    void overrideDefaultSettings() throws IOException {
        final var settingsBuilder = Settings.builder();
        settingsBuilder.setSecureSettings(
                new DummySecureSettings()
                        .setString(AzureStorageSettings.AZURE_ACCOUNT.getKey(), "some_account")
                        .setString(AzureStorageSettings.AZURE_ACCOUNT_KEY.getKey(), "some_account_key")
                        .setFile(
                                AzureStorageSettings.PUBLIC_KEY_FILE.getKey(),
                                Files.newInputStream(publicKeyPem)
                        )
                        .setFile(
                                AzureStorageSettings.PRIVATE_KEY_FILE.getKey(),
                                Files.newInputStream(privateKeyPem)
                        )
        );

        settingsBuilder.put(AzureStorageSettings.MAX_RETRIES.getKey(), 42);

        settingsBuilder.put(AzureStorageSettings.AZURE_HTTP_POOL_MIN_THREADS.getKey(), 10);
        settingsBuilder.put(AzureStorageSettings.AZURE_HTTP_POOL_MAX_THREADS.getKey(), 32);
        settingsBuilder.put(AzureStorageSettings.AZURE_HTTP_POOL_KEEP_ALIVE.getKey(), TimeValue.timeValueMillis(1000L));


        final var azureStorageSettings =
                AzureStorageSettings.create(settingsBuilder.build());

        assertEquals("some_account", azureStorageSettings.azureAccount());
        assertEquals("some_account_key", azureStorageSettings.azureAccountKey());
        assertEquals(
                String.format(
                        AzureStorageSettings.AZURE_CONNECTION_STRING_TEMPLATE,
                        azureStorageSettings.azureAccount(),
                        azureStorageSettings.azureAccountKey()
                ), azureStorageSettings.azureConnectionString());
        assertEquals(42, azureStorageSettings.maxRetries());

        final var httpThreadPoolSettings = azureStorageSettings.httpThreadPoolSettings();
        assertEquals(10, httpThreadPoolSettings.minThreads());
        assertEquals(32, httpThreadPoolSettings.maxThreads());
        assertEquals(TimeValue.timeValueMillis(1000L).getMillis(), httpThreadPoolSettings.keepAlive());
    }

}
