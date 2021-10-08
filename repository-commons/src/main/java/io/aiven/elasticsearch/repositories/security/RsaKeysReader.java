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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Objects;

import org.bouncycastle.util.io.pem.PemReader;

class RsaKeysReader {

    static KeyPair readRsaKeyPair(final byte[] publicKeyBytes,
                                  final byte[] privateKeyBytes) {
        try {
            final var publicKey = readPublicKey(publicKeyBytes);
            final var privateKey = readPrivateKey(privateKeyBytes);
            return new KeyPair(publicKey, privateKey);
        } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw new IllegalArgumentException("Couldn't generate RSA key pair", e);
        }
    }

    private static PublicKey readPublicKey(final byte[] bytes)
            throws NoSuchAlgorithmException, InvalidKeySpecException {
        final var pemContent = readPemContent(new InputStreamReader(new ByteArrayInputStream(bytes)));
        final var keySpec = new X509EncodedKeySpec(pemContent);
        final var kf = KeyFactory.getInstance("RSA");
        return kf.generatePublic(keySpec);
    }

    private static PrivateKey readPrivateKey(final byte[] bytes)
            throws NoSuchAlgorithmException, InvalidKeySpecException {
        final var pemContent = readPemContent(new InputStreamReader(new ByteArrayInputStream(bytes)));
        final var keySpec = new PKCS8EncodedKeySpec(pemContent);
        final var kf = KeyFactory.getInstance("RSA");
        return kf.generatePrivate(keySpec);
    }

    private static byte[] readPemContent(final Reader reader) {
        try (final var pemReader = new PemReader(reader)) {
            final var pemObject = pemReader.readPemObject();
            if (Objects.isNull(pemObject)) {
                throw new IllegalArgumentException("Couldn't read PEM file");
            }
            return pemObject.getContent();
        } catch (final IOException e) {
            throw new IllegalArgumentException("Couldn't read PEM file", e);
        }
    }

}
