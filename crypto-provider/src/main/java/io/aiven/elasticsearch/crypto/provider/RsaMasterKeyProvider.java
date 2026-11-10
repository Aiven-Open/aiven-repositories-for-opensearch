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

import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.MessageDigest;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.spec.MGF1ParameterSpec;
import java.util.HexFormat;
import java.util.Map;

import org.opensearch.common.crypto.DataKeyPair;
import org.opensearch.common.crypto.MasterKeyProvider;

/**
 * Generates AES-256 data keys and wraps them with an RSA key pair using RSA-OAEP with SHA-256.
 */
public final class RsaMasterKeyProvider implements MasterKeyProvider {

    static final String WRAPPING_TRANSFORMATION = "RSA/ECB/OAEPPadding";

    // JCE defaults MGF1 to SHA-1 for OAEP with SHA-256. The AWS Encryption SDK raw RSA keyring uses SHA-256.
    static final OAEPParameterSpec OAEP_SHA256 =
        new OAEPParameterSpec("SHA-256", "MGF1", MGF1ParameterSpec.SHA256, PSource.PSpecified.DEFAULT);

    static final int DATA_KEY_LENGTH = 32;

    private final KeyPair keyPair;

    private final String keyId;

    private final SecureRandom random = new SecureRandom();

    public RsaMasterKeyProvider(final KeyPair keyPair) {
        this.keyPair = keyPair;
        this.keyId = fingerprint(keyPair.getPublic());
    }

    /** Hex SHA-256 of the DER encoded public key. */
    static String fingerprint(final PublicKey publicKey) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(publicKey.getEncoded()));
        } catch (final GeneralSecurityException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }

    @Override
    public DataKeyPair generateDataPair() {
        final byte[] rawKey = new byte[DATA_KEY_LENGTH];
        random.nextBytes(rawKey);
        return new DataKeyPair(rawKey, crypt(Cipher.ENCRYPT_MODE, rawKey));
    }

    @Override
    public byte[] decryptKey(final byte[] encryptedKey) {
        return crypt(Cipher.DECRYPT_MODE, encryptedKey);
    }

    @Override
    public String getKeyId() {
        return keyId;
    }

    @Override
    public Map<String, String> getEncryptionContext() {
        return Map.of();
    }

    @Override
    public void close() {
    }

    private byte[] crypt(final int mode, final byte[] input) {
        try {
            final var cipher = Cipher.getInstance(WRAPPING_TRANSFORMATION);
            cipher.init(mode, mode == Cipher.ENCRYPT_MODE ? keyPair.getPublic() : keyPair.getPrivate(), OAEP_SHA256);
            return cipher.doFinal(input);
        } catch (final GeneralSecurityException e) {
            throw new IllegalStateException(
                mode == Cipher.ENCRYPT_MODE ? "Couldn't wrap data key" : "Couldn't unwrap data key", e);
        }
    }
}
