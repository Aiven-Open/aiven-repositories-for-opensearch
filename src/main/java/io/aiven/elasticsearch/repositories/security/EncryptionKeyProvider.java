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

package io.aiven.elasticsearch.repositories.security;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import java.io.InputStream;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Objects;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class EncryptionKeyProvider
        implements Encryption, Decryption {

    private static final Logger LOGGER = LoggerFactory.getLogger(EncryptionKeyProvider.class);

    public static final int KEY_SIZE = 256;

    public static final Setting<InputStream> PUBLIC_KEY_FILE =
            SecureSetting.secureFile("aiven.public_key_file", null);

    public static final Setting<InputStream> PRIVATE_KEY_FILE =
            SecureSetting.secureFile("aiven.private_key_file", null);

    private static final String CIPHER_TRANSFORMATION = "RSA/NONE/OAEPWithSHA3-512AndMGF1Padding";

    private final KeyGenerator aesKeyGenerator;

    private final KeyPair rsaKeyPair;

    private EncryptionKeyProvider(final KeyPair rsaKeyPair,
                                  final KeyGenerator aesKeyGenerator) {
        this.rsaKeyPair = rsaKeyPair;
        this.aesKeyGenerator = aesKeyGenerator;
    }

    public static EncryptionKeyProvider of(final Settings settings) {
        Objects.requireNonNull(settings, "settings hasn't been set");
        if (!PUBLIC_KEY_FILE.exists(settings)) {
            throw new IllegalArgumentException("Settings with name " + PUBLIC_KEY_FILE.getKey() + " hasn't been set");
        }
        if (!PRIVATE_KEY_FILE.exists(settings)) {
            throw new IllegalArgumentException("Settings with name " + PRIVATE_KEY_FILE.getKey() + " hasn't been set");
        }
        return of(PUBLIC_KEY_FILE.get(settings), PRIVATE_KEY_FILE.get(settings));
    }

    public static EncryptionKeyProvider of(final InputStream rsaPublicKey,
                                           final InputStream rsaPrivateKey) {
        LOGGER.info("Read RSA keys");
        Objects.requireNonNull(rsaPublicKey, "rsaPublicKey hasn't been set");
        Objects.requireNonNull(rsaPrivateKey, "rsaPrivateKey hasn't been set");
        try {
            final var rsaKeyPair = RsaKeysReader.readRsaKeyPair(rsaPublicKey, rsaPrivateKey);
            final var kg = KeyGenerator.getInstance("AES", new BouncyCastleProvider());
            kg.init(KEY_SIZE, SecureRandom.getInstanceStrong());
            return new EncryptionKeyProvider(rsaKeyPair, kg);
        } catch (final NoSuchAlgorithmException e) {
            throw new RuntimeException("Couldn't create encrypt key provider", e);
        }
    }

    public SecretKey createKey() {
        return aesKeyGenerator.generateKey();
    }

    public byte[] encryptKey(final SecretKey secretKey) {
        try {
            final var cipher = createEncryptingCipher(rsaKeyPair.getPublic(), CIPHER_TRANSFORMATION);
            return cipher.doFinal(secretKey.getEncoded());
        } catch (final IllegalBlockSizeException | BadPaddingException e) {
            throw new RuntimeException("Couldn't encrypt AES key", e);
        }
    }

    public SecretKey decryptKey(final byte[] bytes) {
        try {
            final var cipher = createDecryptingCipher(rsaKeyPair.getPrivate(), CIPHER_TRANSFORMATION);
            return new SecretKeySpec(cipher.doFinal(bytes), "AES");
        } catch (final IllegalBlockSizeException | BadPaddingException e) {
            throw new RuntimeException("Couldn't encrypt AES key", e);
        }
    }

}
