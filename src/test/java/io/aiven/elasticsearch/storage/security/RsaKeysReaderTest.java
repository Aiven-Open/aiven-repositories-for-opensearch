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

import java.nio.file.Files;
import java.nio.file.Paths;

import io.aiven.elasticsearch.storage.RsaKeyAwareTest;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RsaKeysReaderTest extends RsaKeyAwareTest {

    @Test
    public void failsForWrongPaths() {

        assertThrows(
                IllegalArgumentException.class,
                () -> RsaKeysReader.readRsaKeyPair(
                        Files.newInputStream(Paths.get(".")),
                        Files.newInputStream(Paths.get(".")))
        );
    }

    @Test
    void throwsIllegalArgumentExceptionForEmptyPublicKey() {
        final var e = assertThrows(
                IllegalArgumentException.class,
                () -> RsaKeysReader.readRsaKeyPair(
                        Files.newInputStream(Paths.get(".")),
                        Files.newInputStream(privateKeyPem))
        );

        assertEquals(
                "Couldn't read PEM file",
                e.getMessage()
        );
    }

    @Test
    void throwsIllegalArgumentExceptionForEmptyPrivateKey() {
        final var e = assertThrows(
                IllegalArgumentException.class,
                () -> RsaKeysReader.readRsaKeyPair(
                        Files.newInputStream(privateKeyPem),
                        Files.newInputStream(Paths.get(".")))
        );

        assertEquals(
                "Couldn't generate key pair",
                e.getMessage()
        );
    }

}
