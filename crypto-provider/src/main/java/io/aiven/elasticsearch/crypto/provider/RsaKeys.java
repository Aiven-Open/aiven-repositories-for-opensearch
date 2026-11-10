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
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import java.util.stream.Collectors;

/**
 * Reads an RSA key pair from PEM files: an X.509 public key and a PKCS#8 private key.
 */
final class RsaKeys {

    private RsaKeys() {
    }

    static KeyPair readKeyPair(final byte[] publicKeyPem, final byte[] privateKeyPem) {
        try {
            final var keyFactory = KeyFactory.getInstance("RSA");
            return new KeyPair(
                keyFactory.generatePublic(new X509EncodedKeySpec(pemContent(publicKeyPem))),
                keyFactory.generatePrivate(new PKCS8EncodedKeySpec(pemContent(privateKeyPem))));
        } catch (final GeneralSecurityException e) {
            throw new IllegalArgumentException("Couldn't read RSA key pair", e);
        }
    }

    static byte[] pemContent(final byte[] pem) {
        final var base64 = new String(pem, StandardCharsets.US_ASCII)
            .lines()
            .map(String::strip)
            .filter(line -> !line.isEmpty() && !line.startsWith("-----"))
            .collect(Collectors.joining());
        if (base64.isEmpty()) {
            throw new IllegalArgumentException("Couldn't read PEM file");
        }
        return Base64.getDecoder().decode(base64);
    }
}
