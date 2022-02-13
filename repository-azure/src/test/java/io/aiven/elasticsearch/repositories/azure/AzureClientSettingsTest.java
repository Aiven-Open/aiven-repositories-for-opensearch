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

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.junit.jupiter.api.Test;

import static io.aiven.elasticsearch.repositories.azure.AzureClientSettings.AZURE_ACCOUNT;
import static io.aiven.elasticsearch.repositories.azure.AzureClientSettings.AZURE_ACCOUNT_KEY;
import static io.aiven.elasticsearch.repositories.azure.AzureClientSettings.AZURE_HTTP_POOL_KEEP_ALIVE;
import static io.aiven.elasticsearch.repositories.azure.AzureClientSettings.AZURE_HTTP_POOL_MAX_THREADS;
import static io.aiven.elasticsearch.repositories.azure.AzureClientSettings.AZURE_HTTP_POOL_MIN_THREADS;
import static io.aiven.elasticsearch.repositories.azure.AzureClientSettings.MAX_RETRIES;
import static io.aiven.elasticsearch.repositories.azure.AzureClientSettings.PRIVATE_KEY_FILE;
import static io.aiven.elasticsearch.repositories.azure.AzureClientSettings.PUBLIC_KEY_FILE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AzureClientSettingsTest extends RsaKeyAwareTest {

    @Test
    void failsForEmptyPublicKey() throws IOException {
        final var securitySettings =
                new DummySecureSettings()
                        .setString(
                                AZURE_ACCOUNT.getConcreteSettingForNamespace("default").getKey(),
                                "some_account"
                        ).setString(
                                AZURE_ACCOUNT_KEY.getConcreteSettingForNamespace("default").getKey(),
                                "some_account_key")
                        .setFile(
                                PRIVATE_KEY_FILE.getConcreteSettingForNamespace("default").getKey(),
                                Files.newInputStream(privateKeyPem)
                        );
        final var settings = Settings.builder().setSecureSettings(securitySettings).build();

        final var t = assertThrows(
                IllegalArgumentException.class, () -> AzureClientSettings.create(settings));
        assertEquals("Settings with name aiven.azure.default.public_key_file hasn't been set",
                t.getMessage());
    }

    @Test
    void failsForEmptyPrivateKey() throws IOException {
        final var securitySettings =
                new DummySecureSettings()
                        .setString(
                                AZURE_ACCOUNT.getConcreteSettingForNamespace("default").getKey(),
                                "some_account"
                        ).setString(
                                AZURE_ACCOUNT_KEY.getConcreteSettingForNamespace("default").getKey(),
                                "some_account_key"
                        ).setFile(
                                PUBLIC_KEY_FILE.getConcreteSettingForNamespace("default").getKey(),
                                Files.newInputStream(publicKeyPem)
                        );
        final var settings = Settings.builder().setSecureSettings(securitySettings).build();

        final var t = assertThrows(
                IllegalArgumentException.class, () -> AzureClientSettings.create(settings));
        assertEquals("Settings with name aiven.azure.default.private_key_file hasn't been set", t.getMessage());
    }

    @Test
    void failsForEmptyAzureAccount() {
        final var secureSettings =
                new DummySecureSettings()
                        .setString(
                                AZURE_ACCOUNT_KEY.getConcreteSettingForNamespace("default").getKey(),
                                "some_key"
                        );

        final var t =
                assertThrows(IllegalArgumentException.class, () ->
                        AzureClientSettings.create(Settings.builder()
                                .setSecureSettings(secureSettings).build()));

        assertEquals("Settings with name aiven.azure.default.client.account hasn't been set", t.getMessage());
    }

    @Test
    void failsForEmptyAzureAccountKey() {

        final var secureSettings =
                new DummySecureSettings()
                        .setString(
                                AZURE_ACCOUNT.getConcreteSettingForNamespace("default").getKey(),
                                "some_account"
                        );

        final var t =
                assertThrows(IllegalArgumentException.class, () ->
                        AzureClientSettings.create(Settings.builder()
                                .setSecureSettings(secureSettings).build()));

        assertEquals("Settings with name aiven.azure.default.client.account.key hasn't been set",
                t.getMessage());
    }

    @Test
    void loadDefaultSettings() throws IOException {
        final var secureSettings =
                new DummySecureSettings()
                        .setString(
                                AZURE_ACCOUNT.getConcreteSettingForNamespace("default").getKey(),
                                "some_account"
                        ).setString(
                                AZURE_ACCOUNT_KEY.getConcreteSettingForNamespace("default").getKey(),
                                "some_account_key")
                        .setFile(
                                PUBLIC_KEY_FILE.getConcreteSettingForNamespace("default").getKey(),
                                Files.newInputStream(publicKeyPem)
                        )
                        .setFile(
                                PRIVATE_KEY_FILE.getConcreteSettingForNamespace("default").getKey(),
                                Files.newInputStream(privateKeyPem)
                        );

        final var azureClientSettings =
                AzureClientSettings.create(Settings.builder().setSecureSettings(secureSettings).build()).get("default");

        assertEquals("some_account", azureClientSettings.azureAccount());
        assertEquals("some_account_key", azureClientSettings.azureAccountKey());
        assertEquals(
                String.format(
                        AzureClientSettings.AZURE_CONNECTION_STRING_TEMPLATE,
                        azureClientSettings.azureAccount(),
                        azureClientSettings.azureAccountKey()
                ), azureClientSettings.azureConnectionString());
        assertEquals(3, azureClientSettings.maxRetries());

        final var httpThreadPoolSettings = azureClientSettings.httpThreadPoolSettings();
        assertEquals(Runtime.getRuntime().availableProcessors() * 2 - 1, httpThreadPoolSettings.minThreads());
        assertEquals(Runtime.getRuntime().availableProcessors() * 2 - 1, httpThreadPoolSettings.maxThreads());
        assertEquals(TimeValue.timeValueSeconds(30L).getMillis(), httpThreadPoolSettings.keepAlive());
    }

    @Test
    void overrideDefaultSettings() throws IOException {
        final var securitySettings =
                new DummySecureSettings()
                        .setString(
                             AZURE_ACCOUNT.getConcreteSettingForNamespace("default").getKey(),
                             "some_account"
                        ).setString(
                             AZURE_ACCOUNT_KEY.getConcreteSettingForNamespace("default").getKey(),
                             "some_account_key")
                        .setFile(
                             PUBLIC_KEY_FILE.getConcreteSettingForNamespace("default").getKey(),
                             Files.newInputStream(publicKeyPem)
                        )
                        .setFile(
                             PRIVATE_KEY_FILE.getConcreteSettingForNamespace("default").getKey(),
                             Files.newInputStream(privateKeyPem)
                        );
        final var settingsBuilder = Settings.builder();
        settingsBuilder.setSecureSettings(securitySettings);
        settingsBuilder.put(
                MAX_RETRIES.getConcreteSettingForNamespace("default").getKey(),
                42
        ).put(
                AZURE_HTTP_POOL_MIN_THREADS.getConcreteSettingForNamespace("default").getKey(),
                10
        ).put(
                AZURE_HTTP_POOL_MAX_THREADS.getConcreteSettingForNamespace("default").getKey(),
                32
        ).put(
                AZURE_HTTP_POOL_KEEP_ALIVE.getConcreteSettingForNamespace("default").getKey(),
                TimeValue.timeValueMillis(1000L)
        );


        final var azureClientSettings =
                AzureClientSettings.create(settingsBuilder.build()).get("default");

        assertEquals("some_account", azureClientSettings.azureAccount());
        assertEquals("some_account_key", azureClientSettings.azureAccountKey());
        assertEquals(
                String.format(
                        AzureClientSettings.AZURE_CONNECTION_STRING_TEMPLATE,
                        azureClientSettings.azureAccount(),
                        azureClientSettings.azureAccountKey()
                ), azureClientSettings.azureConnectionString());
        assertEquals(42, azureClientSettings.maxRetries());

        final var httpThreadPoolSettings = azureClientSettings.httpThreadPoolSettings();
        assertEquals(10, httpThreadPoolSettings.minThreads());
        assertEquals(32, httpThreadPoolSettings.maxThreads());
        assertEquals(TimeValue.timeValueMillis(1000L).getMillis(), httpThreadPoolSettings.keepAlive());
    }

}
