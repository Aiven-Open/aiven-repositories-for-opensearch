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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import org.opensearch.common.io.InputStreamContainer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AwsMessageFormatCryptoHandlerTest extends CipherTestSupport {

    static final int F = 16;

    static final int FRAME_SIZE = F + MessageFormat.FRAME_OVERHEAD;

    @Test
    void headerLengthFollowsFromKeyProviderNameAndKeySize() {
        final var handler = handler(F);
        final var expected = 1 + 2 + 32 + 2 + 2
            + 2 + KEY_PROVIDER_NAME.length()
            + 2 + 64
            + 2 + 256
            + 1 + 4 + 32 + 16;

        assertEquals(expected, handler.initEncryptionMetadata().length());
        assertEquals(expected, handler.initEncryptionMetadata().length());
    }

    @Test
    void decryptedLengthInvertsEncryptedLength() throws IOException {
        final var handler = handler(F);
        final var header = handler.initEncryptionMetadata();
        for (int length = 0; length <= 3 * F + 1; length++) {
            final var encryptedLength = handler.estimateEncryptedLengthOfEntireContent(header, length);
            assertEquals(encryptedLength, encrypt(handler, header, randomBytes(length)).length);
            assertEquals(length, handler.estimateDecryptedLength(header, encryptedLength));
        }
    }

    @Test
    void loadsHeaderOfEmptyBlobShorterThanHeaderReadLength() throws IOException {
        final var handler = handler(F);
        final var ciphertext = encrypt(handler, new byte[0]);

        final var header = handler.loadEncryptionMetadata(headerSupplier(ciphertext));

        assertEquals(ciphertext.length - MessageFormat.FINAL_FRAME_OVERHEAD, header.length());
        assertEquals(0, handler.estimateDecryptedLength(header, ciphertext.length));
        assertEquals(0, decrypt(handler, ciphertext).length);
    }

    @Test
    void partSizeIsRoundedDownToWholeFrames() {
        final var handler = handler(F);
        final var header = handler.initEncryptionMetadata();

        assertEquals(5 * F, handler.adjustContentSizeForPartialEncryption(header, 5 * F + 3));
        assertEquals(5 * F, handler.adjustContentSizeForPartialEncryption(header, 5 * F));
        assertEquals(F, handler.adjustContentSizeForPartialEncryption(header, 3));
    }

    @ParameterizedTest
    @CsvSource({
        "1, 6, 83",
        "2, 3, 55",
        "7, 2, 202",
        "7, 2, 224"
    })
    void partsConcatenateToSingleStreamBytes(final int expectedParts,
                                             final int framesPerPart,
                                             final int length) throws IOException {
        final var handler = handler(F);
        final var header = handler.initEncryptionMetadata();
        final var plaintext = randomBytes(length);
        final var partSize = framesPerPart * F;
        final var parts = Math.max(1, (length + partSize - 1) / partSize);
        assertEquals(expectedParts, parts);

        final var out = new ByteArrayOutputStream();
        for (int part = 0; part < parts; part++) {
            final var offset = part * partSize;
            final var partLength = Math.min(partSize, length - offset);
            final var container = handler.createEncryptingStreamOfPart(
                header,
                new InputStreamContainer(new ByteArrayInputStream(plaintext, offset, partLength), partLength, offset),
                parts,
                part);
            assertEquals(out.size(), container.getOffset(), "offset of part " + part);
            final var bytes = container.getInputStream().readAllBytes();
            assertEquals(container.getContentLength(), bytes.length, "length of part " + part);
            out.write(bytes);
        }

        assertArrayEquals(encrypt(handler, header, plaintext), out.toByteArray());
    }

    @Test
    void partThatEndsInsideFrameIsRefused() {
        final var handler = handler(F);
        final var header = handler.initEncryptionMetadata();
        final var plaintext = randomBytes(3 * F);

        assertThrows(IllegalArgumentException.class, () -> handler.createEncryptingStreamOfPart(
            header, new InputStreamContainer(new ByteArrayInputStream(plaintext, 0, F + 1), F + 1, 0), 2, 0));
        assertThrows(IllegalArgumentException.class, () -> handler.createEncryptingStreamOfPart(
            header, new InputStreamContainer(new ByteArrayInputStream(plaintext, 3, F), F, 3), 2, 1));
    }

    @ParameterizedTest
    @ValueSource(ints = {3 * F + 7, 3 * F})
    void readsEveryRangeOfThreeFrameBlob(final int length) throws IOException {
        final var handler = handler(F);
        final var plaintext = randomBytes(length);
        final var ciphertext = encrypt(handler, plaintext);
        final var header = handler.loadEncryptionMetadata(headerSupplier(ciphertext));

        for (int start = 0; start < length; start++) {
            for (int end = start; end < length; end++) {
                final var provider = handler.createDecryptingStreamOfRange(header, start, end);
                final var range = provider.getAdjustedRange();
                try (final var in = provider.getDecryptedStreamProvider()
                    .apply(new ByteArrayInputStream(slice(ciphertext, range[0], range[1])))) {
                    assertArrayEquals(Arrays.copyOfRange(plaintext, start, end + 1), in.readAllBytes(),
                        "range [" + start + ", " + end + "]");
                }
            }
        }
    }

    @Test
    void adjustedRangeCoversWholeFrames() {
        final var handler = handler(F);
        final var header = handler.initEncryptionMetadata();
        final var headerLength = header.length();

        final var range = handler.createDecryptingStreamOfRange(header, F + 1, 2 * F + 3).getAdjustedRange();

        assertEquals(headerLength + FRAME_SIZE, range[0]);
        assertEquals(headerLength + 3 * FRAME_SIZE + 8 - 1, range[1]);
    }

    @Test
    void flippedMessageIdIsRefused() throws IOException {
        assertTamperedBlobIsRefused((ciphertext, headerLength) -> ciphertext[1 + 2 + 5] ^= 1, "commitment");
    }

    @Test
    void flippedCommitmentIsRefused() throws IOException {
        assertTamperedBlobIsRefused(
            (ciphertext, headerLength) -> ciphertext[headerLength - MessageFormat.TAG_LENGTH - 1] ^= 1, "commitment");
    }

    @Test
    void flippedFrameLengthIsRefused() throws IOException {
        assertTamperedBlobIsRefused(
            (ciphertext, headerLength) -> ciphertext[headerLength - MessageFormat.TAG_LENGTH - 32 - 1] ^= 2,
            "authentication");
    }

    @Test
    void flippedHeaderTagIsRefused() throws IOException {
        assertTamperedBlobIsRefused((ciphertext, headerLength) -> ciphertext[headerLength - 1] ^= 1, "authentication");
    }

    @Test
    void outOfOrderFramesAreRefused() throws IOException {
        assertTamperedBlobIsRefused((ciphertext, headerLength) -> {
            final var first = Arrays.copyOfRange(ciphertext, headerLength, headerLength + FRAME_SIZE);
            System.arraycopy(ciphertext, headerLength + FRAME_SIZE, ciphertext, headerLength, FRAME_SIZE);
            System.arraycopy(first, 0, ciphertext, headerLength + FRAME_SIZE, FRAME_SIZE);
        }, "Expected frame 1, found frame 2");
    }

    @Test
    void flippedFrameCiphertextIsRefused() throws IOException {
        assertTamperedBlobIsRefused(
            (ciphertext, headerLength) -> ciphertext[headerLength + 4 + MessageFormat.IV_LENGTH + 3] ^= 1,
            "Frame 1 failed authentication");
    }

    @Test
    void flippedFinalFrameContentLengthIsRefused() throws IOException {
        assertTamperedBlobIsRefused(
            (ciphertext, headerLength) -> ciphertext[headerLength + 3 * FRAME_SIZE + 4 + 4 + MessageFormat.IV_LENGTH + 3] ^= 1,
            "Frame 4");
    }

    @Test
    void missingFinalFrameIsRefused() throws IOException {
        final var handler = handler(F);
        final var ciphertext = encrypt(handler, randomBytes(3 * F + 7));
        final var headerLength = handler.loadEncryptionMetadata(headerSupplier(ciphertext)).length();

        assertThrows(IOException.class, () -> decrypt(handler, Arrays.copyOf(ciphertext, headerLength + 3 * FRAME_SIZE)));
    }

    @Test
    void blobForAnotherKeyPairIsRefused() throws IOException {
        final var ciphertext = encrypt(handler(otherKeyPair, F), randomBytes(10));

        final var e = assertThrows(IOException.class, () -> decrypt(handler(F), ciphertext));
        assertTrue(e.getMessage().contains("data key"), e.getMessage());
    }

    @Test
    void resetAtStartWritesSameBytesAgain() throws IOException {
        final var handler = handler(F);
        final var header = handler.initEncryptionMetadata();
        final var plaintext = randomBytes(3 * F + 7);
        final var expected = encrypt(handler, header, plaintext);

        final var in = handler.createEncryptingStream(
            header, new InputStreamContainer(new ByteArrayInputStream(plaintext), plaintext.length, 0)).getInputStream();
        assertTrue(in.markSupported());
        in.mark(Integer.MAX_VALUE);
        in.readNBytes(expected.length / 2);
        in.reset();
        in.readNBytes(expected.length - 1);
        in.reset();

        assertArrayEquals(expected, in.readAllBytes());
    }

    @Test
    void resetAfterMarkInsideStreamIsRefused() throws IOException {
        final var handler = handler(F);
        final var plaintext = randomBytes(3 * F);
        final var in = handler.createEncryptingStream(
            handler.initEncryptionMetadata(),
            new InputStreamContainer(new ByteArrayInputStream(plaintext), plaintext.length, 0)).getInputStream();

        in.read();
        in.mark(Integer.MAX_VALUE);

        assertThrows(IOException.class, in::reset);
    }

    @Test
    void closeRemovesHandlerFromRegistryOnceAndKeepsItUsable() throws IOException {
        final var plugin = new AwsMessageFormatCryptoPlugin();
        final var registryRemovals = new AtomicInteger();
        final var handler = (AwsMessageFormatCryptoHandler) plugin.getOrCreateCryptoHandler(
            keyProvider(), KEY_PROVIDER_NAME, "aiven-rsa", registryRemovals::incrementAndGet);

        handler.close();
        handler.close();

        assertEquals(1, registryRemovals.get());
        final var plaintext = randomBytes(100);
        assertArrayEquals(plaintext, decrypt(handler, encrypt(handler, plaintext)));
        assertNotSame(handler, plugin.getOrCreateCryptoHandler(keyProvider(), KEY_PROVIDER_NAME, "aiven-rsa", () -> { }));
    }

    @FunctionalInterface
    interface Tampering {
        void apply(byte[] ciphertext, int headerLength);
    }

    private static void assertTamperedBlobIsRefused(final Tampering tampering, final String message) throws IOException {
        final var handler = handler(F);
        final var ciphertext = encrypt(handler, randomBytes(3 * F + 7));
        final var headerLength = handler.loadEncryptionMetadata(headerSupplier(ciphertext)).length();
        tampering.apply(ciphertext, headerLength);

        final var e = assertThrows(IOException.class, () -> decrypt(handler, ciphertext));
        assertTrue(e.getMessage().contains(message), e.getMessage());
    }
}
