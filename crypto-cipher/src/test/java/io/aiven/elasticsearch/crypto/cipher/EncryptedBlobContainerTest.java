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

package io.aiven.elasticsearch.crypto.cipher;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.EncryptedBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class EncryptedBlobContainerTest extends CipherTestSupport {

    static final String BLOB_NAME = "blob";

    @TempDir
    Path directory;

    @Test
    void roundTripThroughCoreEncryptedBlobContainer() throws IOException {
        final var length = 2 * MessageFormat.FRAME_LENGTH + 12345;
        final var plaintext = randomBytes(length);
        try (final var blobStore = new FsBlobStore(8192, directory, false)) {
            final var plainContainer = blobStore.blobContainer(BlobPath.cleanPath());
            final var container = new EncryptedBlobContainer<>(plainContainer, handler(MessageFormat.FRAME_LENGTH));

            container.writeBlob(BLOB_NAME, new ByteArrayInputStream(plaintext), length, true);

            assertEquals(length, container.listBlobs().get(BLOB_NAME).length());
            try (final var in = container.readBlob(BLOB_NAME)) {
                assertArrayEquals(plaintext, in.readAllBytes());
            }
            for (final var range : new long[][] {
                {0, 1},
                {0, length},
                {MessageFormat.FRAME_LENGTH - 1, 2},
                {MessageFormat.FRAME_LENGTH, MessageFormat.FRAME_LENGTH},
                {length - 1, 1},
                {12345, 2 * MessageFormat.FRAME_LENGTH}
            }) {
                try (final var in = container.readBlob(BLOB_NAME, range[0], range[1])) {
                    assertArrayEquals(Arrays.copyOfRange(plaintext, (int) range[0], (int) (range[0] + range[1])),
                        in.readAllBytes(), "position " + range[0] + ", length " + range[1]);
                }
            }

            final var stored = Files.readAllBytes(directory.resolve(BLOB_NAME));
            assertEquals(stored.length, plainContainer.listBlobs().get(BLOB_NAME).length());
            assertArrayEquals(plaintext, awsCrypto(MessageFormat.FRAME_LENGTH).decryptData(jceMasterKey(), stored).getResult());
        }
    }
}
