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

import java.io.IOException;
import java.util.stream.IntStream;

import com.amazonaws.encryptionsdk.AwsCrypto;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AwsCryptoCompatibilityTest extends CipherTestSupport {

    static final int RANDOM_LENGTHS = 104;

    @Test
    void awsCryptoDecryptsWhatWeWriteForEveryLengthUpToThreeFramesAndOne() throws IOException {
        final var handler = handler(SMALL_FRAME_LENGTH);
        final var awsCrypto = awsCrypto(SMALL_FRAME_LENGTH);
        for (int length = 0; length <= 3 * SMALL_FRAME_LENGTH + 1; length++) {
            assertAwsCryptoDecrypts(awsCrypto, handler, randomBytes(length));
        }
    }

    @Test
    void decryptsWhatAwsCryptoWritesForEveryLengthUpToThreeFramesAndOne() throws IOException {
        final var handler = handler(SMALL_FRAME_LENGTH);
        final var awsCrypto = awsCrypto(SMALL_FRAME_LENGTH);
        for (int length = 0; length <= 3 * SMALL_FRAME_LENGTH + 1; length++) {
            assertDecryptsAwsCryptoMessage(awsCrypto, handler, randomBytes(length));
        }
    }

    @Test
    void sizeFormulaMatchesAwsCryptoEstimateForEveryLengthUpToThreeFramesAndOne() {
        final var handler = handler(SMALL_FRAME_LENGTH);
        final var awsCrypto = awsCrypto(SMALL_FRAME_LENGTH);
        final var header = handler.initEncryptionMetadata();
        for (int length = 0; length <= 3 * SMALL_FRAME_LENGTH + 1; length++) {
            assertEquals(awsCrypto.estimateCiphertextSize(jceMasterKey(), length),
                handler.estimateEncryptedLengthOfEntireContent(header, length), "length " + length);
        }
    }

    @Test
    void compatibleBothWaysAtProductionFrameLengthForRandomLengths() throws IOException {
        final var handler = handler(MessageFormat.FRAME_LENGTH);
        final var awsCrypto = awsCrypto(MessageFormat.FRAME_LENGTH);
        final var header = handler.initEncryptionMetadata();
        final var lengths = IntStream.concat(
            IntStream.of(0, 1, MessageFormat.FRAME_LENGTH - 1, MessageFormat.FRAME_LENGTH, MessageFormat.FRAME_LENGTH + 1),
            RANDOM.ints(RANDOM_LENGTHS - 5, 0, 3 * MessageFormat.FRAME_LENGTH + 2)).toArray();
        for (final var length : lengths) {
            final var plaintext = randomBytes(length);
            assertEquals(awsCrypto.estimateCiphertextSize(jceMasterKey(), length),
                handler.estimateEncryptedLengthOfEntireContent(header, length), "length " + length);
            assertAwsCryptoDecrypts(awsCrypto, handler, plaintext);
            assertDecryptsAwsCryptoMessage(awsCrypto, handler, plaintext);
        }
    }

    @Test
    void awsCryptoRefusesMessageForAnotherKeyPair() throws IOException {
        final var ciphertext = encrypt(handler(otherKeyPair, SMALL_FRAME_LENGTH), randomBytes(100));

        assertThrows(Exception.class, () -> awsCrypto(SMALL_FRAME_LENGTH).decryptData(jceMasterKey(), ciphertext));
    }

    private static void assertAwsCryptoDecrypts(final AwsCrypto awsCrypto,
                                                final AwsMessageFormatCryptoHandler handler,
                                                final byte[] plaintext) throws IOException {
        final var ciphertext = encrypt(handler, plaintext);
        final var result = awsCrypto.decryptData(jceMasterKey(), ciphertext);

        assertArrayEquals(plaintext, result.getResult(), "length " + plaintext.length);
        assertEquals(AwsMessageFormatCryptoHandler.ALGORITHM, result.getCryptoAlgorithm());
        assertTrue(result.getEncryptionContext().isEmpty());
        assertEquals(1, result.getHeaders().getEncryptedKeyBlobCount());
    }

    private static void assertDecryptsAwsCryptoMessage(final AwsCrypto awsCrypto,
                                                       final AwsMessageFormatCryptoHandler handler,
                                                       final byte[] plaintext) throws IOException {
        final var ciphertext = awsCrypto.encryptData(jceMasterKey(), plaintext).getResult();

        assertArrayEquals(plaintext, decrypt(handler, ciphertext), "length " + plaintext.length);
        final var header = handler.loadEncryptionMetadata(headerSupplier(ciphertext));
        assertEquals(plaintext.length, handler.estimateDecryptedLength(header, ciphertext.length), "length " + plaintext.length);
    }
}
