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

package io.aiven.elasticsearch.repositories.gcs;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import io.aiven.elasticsearch.repositories.security.EncryptionKeyProvider;

final class RsaKeyFiles {

    private static final String KEY_DIR = System.getProperty("user.home") + "/.config/aiven-jmh";

    private static final String KEY_SETUP =
        "  mkdir -p -m 700 ~/.config/aiven-jmh && cd ~/.config/aiven-jmh\n"
            + "  openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:3072 -out private.pem\n"
            + "  openssl pkey -in private.pem -pubout -out public.pem\n"
            + "  chmod 600 private.pem";

    private RsaKeyFiles() {
    }

    static EncryptionKeyProvider read() throws IOException {
        return EncryptionKeyProvider.of(
            read("bench.rsa.public_key_file", "public.pem"),
            read("bench.rsa.private_key_file", "private.pem"));
    }

    private static byte[] read(final String property, final String defaultName) throws IOException {
        final var path = Path.of(System.getProperty(property, KEY_DIR + "/" + defaultName));
        if (!Files.exists(path)) {
            throw new IllegalStateException(path + " doesn't exist. Create the RSA key pair with:\n" + KEY_SETUP);
        }
        return Files.readAllBytes(path);
    }
}
