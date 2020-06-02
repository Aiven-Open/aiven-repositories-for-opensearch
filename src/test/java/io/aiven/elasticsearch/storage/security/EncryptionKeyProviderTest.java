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

package io.aiven.elasticsearch.storage.security;

import java.io.IOException;
import java.nio.file.Files;

import io.aiven.elasticsearch.gcs.utils.DummySecureSettings;
import io.aiven.elasticsearch.storage.RsaKeyAwareTest;

import org.elasticsearch.common.settings.Settings;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class EncryptionKeyProviderTest extends RsaKeyAwareTest {

    @Test
    void alwaysGeneratesNewKey() throws IOException {
        final var ekp =
                EncryptionKeyProvider.of(
                        Files.newInputStream(publicKeyPem),
                        Files.newInputStream(privateKeyPem)
                );

        final var key1 = ekp.createKey();
        final var key2 = ekp.createKey();

        assertNotEquals(key1, key2);
    }

    @Test
    void decryptGeneratedKey() throws IOException {
        final var ekProvider =
                EncryptionKeyProvider.of(
                        Files.newInputStream(publicKeyPem),
                        Files.newInputStream(privateKeyPem)
                );
        final var secretKey = ekProvider.createKey();
        final var encryptedKey = ekProvider.encryptKey(secretKey);
        final var restoredKey = ekProvider.decryptKey(encryptedKey);

        assertEquals(secretKey, restoredKey);
    }

    @Test
    public void canBeBuildFromElasticSettings() throws IOException {
        final var settings = Settings.builder().setSecureSettings(
                new DummySecureSettings()
                        .setFile(EncryptionKeyProvider.PUBLIC_KEY_FILE.getKey(), Files.newInputStream(publicKeyPem))
                        .setFile(EncryptionKeyProvider.PRIVATE_KEY_FILE.getKey(), Files.newInputStream(privateKeyPem))
        ).build();

        assertNotNull(EncryptionKeyProvider.of(settings));
    }

}
