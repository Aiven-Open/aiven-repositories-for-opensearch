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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyPairGenerator;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;

import io.aiven.elasticsearch.repositories.RsaKeyAwareTest;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RsaKeysReaderTest extends RsaKeyAwareTest {

    @Test
    public void failsForUnknownPaths() {

        assertThrows(
                IOException.class, () ->
                        RsaKeysReader.readRsaKeyPair(
                                Files.newInputStream(Paths.get(".")).readAllBytes(),
                                Files.newInputStream(Paths.get(".")).readAllBytes())
        );
    }

    @Test
    void throwsIllegalArgumentExceptionUnsupportedKey(@TempDir final Path tmpDir) throws Exception {
        final var dsaPublicKeyPem = tmpDir.resolve("dsa_public_key.pem");
        final var dsaPrivateKeyPem = tmpDir.resolve("dsa_private_key.pem");

        final var dsaKeyPair = KeyPairGenerator.getInstance("DSA").generateKeyPair();
        writePemFile(dsaPublicKeyPem, new X509EncodedKeySpec(dsaKeyPair.getPublic().getEncoded()));
        writePemFile(dsaPrivateKeyPem, new PKCS8EncodedKeySpec(dsaKeyPair.getPrivate().getEncoded()));

        final var e =
                assertThrows(IllegalArgumentException.class, () ->
                        RsaKeysReader.readRsaKeyPair(
                                Files.newInputStream(dsaPublicKeyPem).readAllBytes(),
                                Files.newInputStream(dsaPrivateKeyPem).readAllBytes()
                        ));

        assertEquals(
            "Couldn't generate RSA key pair",
            e.getMessage()
        );

    }

    @Test
    void throwsIllegalArgumentExceptionForEmptyPublicKey(@TempDir final Path tmpDir) throws IOException {
        final var emptyPublicKeyPemFile =
            Files.createFile(tmpDir.resolve("empty_public_key.pem"));

        final var e = assertThrows(
                IllegalArgumentException.class, () ->
                        RsaKeysReader.readRsaKeyPair(
                                Files.newInputStream(emptyPublicKeyPemFile).readAllBytes(),
                                Files.newInputStream(privateKeyPem).readAllBytes())
        );

        assertEquals(
                "Couldn't read PEM file",
                e.getMessage()
        );
    }

    @Test
    void throwsIllegalArgumentExceptionForEmptyPrivateKey(@TempDir final Path tmpDir) throws IOException {
        final var emptyPrivateKeyPemFile =
            Files.createFile(tmpDir.resolve("empty_private_key.pem"));

        final var e = assertThrows(
                IllegalArgumentException.class, () ->
                        RsaKeysReader.readRsaKeyPair(
                                Files.newInputStream(publicKeyPem).readAllBytes(),
                                Files.newInputStream(emptyPrivateKeyPemFile).readAllBytes())
        );

        assertEquals(
            "Couldn't read PEM file",
            e.getMessage()
        );
    }

}
