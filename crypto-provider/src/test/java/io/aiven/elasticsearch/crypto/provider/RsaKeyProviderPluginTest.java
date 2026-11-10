/*
 * Copyright 2026 Aiven Oy
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

package io.aiven.elasticsearch.crypto.provider;

import java.nio.charset.StandardCharsets;
import java.security.Key;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.Base64;

import org.opensearch.cluster.metadata.CryptoMetadata;
import org.opensearch.common.settings.Settings;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RsaKeyProviderPluginTest {

    static KeyPair keyPair;

    static KeyPair otherKeyPair;

    @BeforeAll
    static void generateKeys() throws Exception {
        final var generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        keyPair = generator.generateKeyPair();
        otherKeyPair = generator.generateKeyPair();
    }

    static byte[] pem(final String type, final Key key) {
        final var body = Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.US_ASCII)).encodeToString(key.getEncoded());
        return ("-----BEGIN " + type + "-----\n" + body + "\n-----END " + type + "-----\n").getBytes(StandardCharsets.US_ASCII);
    }

    static Settings keystore(final String name, final KeyPair keyPair) {
        return Settings.builder()
            .setSecureSettings(new TestSecureSettings()
                .setFile("aiven.crypto." + name + ".public_key_file", pem("PUBLIC KEY", keyPair.getPublic()))
                .setFile("aiven.crypto." + name + ".private_key_file", pem("PRIVATE KEY", keyPair.getPrivate())))
            .build();
    }

    static CryptoMetadata cryptoMetadata(final String name) {
        return new CryptoMetadata(name, RsaKeyProviderPlugin.TYPE, Settings.EMPTY);
    }

    @Test
    void createsKeyProviderForKeyPairInKeystore() {
        final var plugin = new RsaKeyProviderPlugin(keystore("default", keyPair));

        assertEquals("aiven-rsa", plugin.type());
        assertEquals(RsaMasterKeyProvider.fingerprint(keyPair.getPublic()),
            plugin.createKeyProvider(cryptoMetadata("default")).getKeyId());
    }

    @Test
    void keyProviderUnwrapsKeysWrappedWithKeystoreKeyPair() {
        final var dataKeyPair = new RsaMasterKeyProvider(keyPair).generateDataPair();
        final var provider = new RsaKeyProviderPlugin(keystore("default", keyPair)).createKeyProvider(cryptoMetadata("default"));

        assertArrayEquals(dataKeyPair.getRawKey(), provider.decryptKey(dataKeyPair.getEncryptedKey()));
    }

    @Test
    void reloadReplacesKeyPairs() {
        final var plugin = new RsaKeyProviderPlugin(keystore("default", keyPair));
        final var before = plugin.createKeyProvider(cryptoMetadata("default"));

        plugin.reload(keystore("default", otherKeyPair));

        assertEquals(RsaMasterKeyProvider.fingerprint(otherKeyPair.getPublic()),
            plugin.createKeyProvider(cryptoMetadata("default")).getKeyId());
        assertEquals(RsaMasterKeyProvider.fingerprint(keyPair.getPublic()), before.getKeyId());
    }

    @Test
    void unknownKeyProviderNameIsRefused() {
        final var plugin = new RsaKeyProviderPlugin(keystore("default", keyPair));

        final var e = assertThrows(IllegalArgumentException.class, () -> plugin.createKeyProvider(cryptoMetadata("other")));
        assertTrue(e.getMessage().contains("aiven.crypto.other.public_key_file"), e.getMessage());
    }

    @Test
    void keyProviderSettingsAreRefused() {
        final var plugin = new RsaKeyProviderPlugin(keystore("default", keyPair));
        final var metadata = new CryptoMetadata("default", RsaKeyProviderPlugin.TYPE,
            Settings.builder().put("key", "value").build());

        assertThrows(IllegalArgumentException.class, () -> plugin.createKeyProvider(metadata));
    }

    @Test
    void keyPairWithoutPrivateKeyIsRefused() {
        final var settings = Settings.builder()
            .setSecureSettings(new TestSecureSettings()
                .setFile("aiven.crypto.default.public_key_file", pem("PUBLIC KEY", keyPair.getPublic())))
            .build();

        final var e = assertThrows(IllegalArgumentException.class, () -> new RsaKeyProviderPlugin(settings));
        assertTrue(e.getMessage().contains("aiven.crypto.default.private_key_file"), e.getMessage());
    }

    @Test
    void startsWithEmptyKeystore() {
        final var plugin = new RsaKeyProviderPlugin(Settings.EMPTY);

        assertThrows(IllegalArgumentException.class, () -> plugin.createKeyProvider(cryptoMetadata("default")));
    }
}
