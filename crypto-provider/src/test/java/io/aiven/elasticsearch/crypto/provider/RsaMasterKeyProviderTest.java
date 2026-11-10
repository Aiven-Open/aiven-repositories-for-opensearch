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

import javax.crypto.Cipher;
import javax.crypto.spec.OAEPParameterSpec;
import javax.crypto.spec.PSource;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.spec.MGF1ParameterSpec;
import java.util.HexFormat;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RsaMasterKeyProviderTest {

    static KeyPair keyPair;

    static KeyPair otherKeyPair;

    @BeforeAll
    static void generateKeys() throws Exception {
        final var generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        keyPair = generator.generateKeyPair();
        otherKeyPair = generator.generateKeyPair();
    }

    @Test
    void generatedDataKeyUnwrapsToRawKey() {
        final var provider = new RsaMasterKeyProvider(keyPair);
        final var dataKeyPair = provider.generateDataPair();

        assertEquals(32, dataKeyPair.getRawKey().length);
        assertEquals(256, dataKeyPair.getEncryptedKey().length);
        assertArrayEquals(dataKeyPair.getRawKey(), provider.decryptKey(dataKeyPair.getEncryptedKey()));
    }

    @Test
    void wrappedKeyIsRsaOaepWithSha256AndMgf1Sha256() throws Exception {
        final var dataKeyPair = new RsaMasterKeyProvider(keyPair).generateDataPair();

        final var cipher = Cipher.getInstance("RSA/ECB/OAEPPadding");
        cipher.init(Cipher.DECRYPT_MODE, keyPair.getPrivate(),
            new OAEPParameterSpec("SHA-256", "MGF1", MGF1ParameterSpec.SHA256, PSource.PSpecified.DEFAULT));
        assertArrayEquals(dataKeyPair.getRawKey(), cipher.doFinal(dataKeyPair.getEncryptedKey()));

        final var sha1Mgf = Cipher.getInstance("RSA/ECB/OAEPPadding");
        sha1Mgf.init(Cipher.DECRYPT_MODE, keyPair.getPrivate(),
            new OAEPParameterSpec("SHA-256", "MGF1", MGF1ParameterSpec.SHA1, PSource.PSpecified.DEFAULT));
        assertThrows(Exception.class, () -> sha1Mgf.doFinal(dataKeyPair.getEncryptedKey()));
    }

    @Test
    void keyIdIsSha256OfPublicKey() throws Exception {
        final var expected = HexFormat.of()
            .formatHex(MessageDigest.getInstance("SHA-256").digest(keyPair.getPublic().getEncoded()));

        assertEquals(expected, new RsaMasterKeyProvider(keyPair).getKeyId());
        assertEquals(expected, new RsaMasterKeyProvider(keyPair).getKeyId());
        assertNotEquals(expected, new RsaMasterKeyProvider(otherKeyPair).getKeyId());
        assertTrue(new RsaMasterKeyProvider(keyPair).getEncryptionContext().isEmpty());
    }

    @Test
    void keyWrappedForAnotherKeyPairIsRefused() {
        final var encryptedKey = new RsaMasterKeyProvider(otherKeyPair).generateDataPair().getEncryptedKey();

        assertThrows(IllegalStateException.class, () -> new RsaMasterKeyProvider(keyPair).decryptKey(encryptedKey));
    }
}
