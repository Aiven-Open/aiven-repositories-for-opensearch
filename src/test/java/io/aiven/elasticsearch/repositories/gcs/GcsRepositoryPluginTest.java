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

import io.aiven.elasticsearch.repositories.DummySecureSettings;
import io.aiven.elasticsearch.repositories.RsaKeyAwareTest;
import io.aiven.elasticsearch.repositories.security.EncryptionKeyProvider;

import org.elasticsearch.common.settings.Settings;
import org.junit.jupiter.api.Test;


import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GcsRepositoryPluginTest extends RsaKeyAwareTest {

    @Test
    void skipNonAivenSettings() throws Exception {
        var plugin = new GcsRepositoryPlugin(Settings.EMPTY);
        assertThrows(IOException.class, plugin.gcsSettingsProvider::encryptionKeyProvider);
        assertThrows(IOException.class, plugin.gcsSettingsProvider::gcsClient);

        plugin = new GcsRepositoryPlugin(
                Settings.builder()
                        .put("some_prop#0", "some_value#0")
                        .put("some_prop#1", "some_value#1")
                        .put("some_prop#2", "some_value#2")
                        .build());
        assertThrows(IOException.class, plugin.gcsSettingsProvider::encryptionKeyProvider);
        assertThrows(IOException.class, plugin.gcsSettingsProvider::gcsClient);
    }

    @Test
    void applyAivenSettings() throws Exception {

        final var plugin =
                new GcsRepositoryPlugin(Settings.builder()
                        .setSecureSettings(createFullSecureSettings()).build());

        assertNotNull(plugin.gcsSettingsProvider.encryptionKeyProvider());
        assertNotNull(plugin.gcsSettingsProvider.gcsClient());
    }

    private DummySecureSettings createFullSecureSettings() throws IOException {
        return new DummySecureSettings()
                .setFile(
                        GcsStorageSettings.CREDENTIALS_FILE_SETTING.getKey(),
                        getClass().getClassLoader().getResourceAsStream("test_gcs_creds.json"))
                .setFile(EncryptionKeyProvider.PRIVATE_KEY_FILE.getKey(), Files.newInputStream(privateKeyPem))
                .setFile(EncryptionKeyProvider.PUBLIC_KEY_FILE.getKey(), Files.newInputStream(publicKeyPem));
    }


}
