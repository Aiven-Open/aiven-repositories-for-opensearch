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

import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Arrays;

/**
 * Constants and primitives of the AWS Encryption SDK message format, version 2, with the algorithm suite
 * {@code ALG_AES_256_GCM_HKDF_SHA512_COMMIT_KEY} and framed content.
 * See https://docs.aws.amazon.com/encryption-sdk/latest/developer-guide/message-format.html
 */
final class MessageFormat {

    static final byte VERSION = 2;

    static final short ALGORITHM_SUITE_ID = 0x0478;

    static final byte FRAMED_CONTENT = 2;

    // Part of the stored format and of every ciphertext length. Treat a change as a new format: new key_provider_name,
    // new repositories, never in place.
    static final int FRAME_LENGTH = 1024 * 1024;

    static final int MESSAGE_ID_LENGTH = 32;

    static final int KEY_LENGTH = 32;

    static final int COMMITMENT_LENGTH = 32;

    static final int IV_LENGTH = 12;

    static final int TAG_LENGTH = 16;

    static final int FINAL_FRAME_MARKER = 0xFFFFFFFF;

    /** Sequence number, IV and tag of a regular frame. */
    static final int FRAME_OVERHEAD = 4 + IV_LENGTH + TAG_LENGTH;

    /** Final frame marker, sequence number, IV, content length and tag of the final frame. */
    static final int FINAL_FRAME_OVERHEAD = 4 + 4 + IV_LENGTH + 4 + TAG_LENGTH;

    static final long MAX_SEQUENCE_NUMBER = 0xFFFFFFFFL;

    private static final byte[] FRAME_LABEL = "AWSKMSEncryptionClient Frame".getBytes(StandardCharsets.UTF_8);

    private static final byte[] FINAL_FRAME_LABEL = "AWSKMSEncryptionClient Final Frame".getBytes(StandardCharsets.UTF_8);

    private static final byte[] DERIVE_KEY_LABEL = "DERIVEKEY".getBytes(StandardCharsets.UTF_8);

    private static final byte[] COMMIT_KEY_LABEL = "COMMITKEY".getBytes(StandardCharsets.UTF_8);

    private static final String HMAC_SHA512 = "HmacSHA512";

    private MessageFormat() {
    }

    /** The frame key and the commitment key, both derived from the data key with HKDF-SHA512 salted with the message id. */
    record DerivedKeys(SecretKey frameKey, byte[] commitment) {
    }

    static DerivedKeys deriveKeys(final byte[] dataKey, final byte[] messageId) throws GeneralSecurityException {
        final var mac = Mac.getInstance(HMAC_SHA512);
        mac.init(new SecretKeySpec(messageId, HMAC_SHA512));
        final var pseudoRandomKey = new SecretKeySpec(mac.doFinal(dataKey), HMAC_SHA512);
        final var frameKeyLabel = ByteBuffer.allocate(2 + DERIVE_KEY_LABEL.length)
            .putShort(ALGORITHM_SUITE_ID)
            .put(DERIVE_KEY_LABEL)
            .array();
        return new DerivedKeys(
            new SecretKeySpec(expand(mac, pseudoRandomKey, frameKeyLabel, KEY_LENGTH), "AES"),
            expand(mac, pseudoRandomKey, COMMIT_KEY_LABEL, COMMITMENT_LENGTH));
    }

    private static byte[] expand(final Mac mac,
                                 final SecretKey pseudoRandomKey,
                                 final byte[] info,
                                 final int length) throws GeneralSecurityException {
        mac.init(pseudoRandomKey);
        mac.update(info);
        mac.update((byte) 1);
        return Arrays.copyOf(mac.doFinal(), length);
    }

    // The spec requires 8 zero bytes followed by the sequence number. A random IV makes the AWS Encryption SDK reject the blob.
    static byte[] frameIv(final long sequenceNumber) {
        return ByteBuffer.allocate(IV_LENGTH).putInt(IV_LENGTH - 4, (int) sequenceNumber).array();
    }

    static byte[] frameAad(final byte[] messageId,
                           final boolean finalFrame,
                           final long sequenceNumber,
                           final long contentLength) {
        final var label = finalFrame ? FINAL_FRAME_LABEL : FRAME_LABEL;
        return ByteBuffer.allocate(messageId.length + label.length + 4 + 8)
            .put(messageId)
            .put(label)
            .putInt((int) sequenceNumber)
            .putLong(contentLength)
            .array();
    }

    /** Returns the ciphertext followed by the 16-byte tag. */
    static byte[] seal(final SecretKey key,
                       final byte[] iv,
                       final byte[] aad,
                       final byte[] plaintext) throws GeneralSecurityException {
        return gcm(Cipher.ENCRYPT_MODE, key, iv, aad).doFinal(plaintext);
    }

    /** Takes the ciphertext followed by the 16-byte tag. Throws {@code AEADBadTagException} if authentication fails. */
    static byte[] open(final SecretKey key,
                       final byte[] iv,
                       final byte[] aad,
                       final byte[] ciphertextAndTag) throws GeneralSecurityException {
        return gcm(Cipher.DECRYPT_MODE, key, iv, aad).doFinal(ciphertextAndTag);
    }

    private static Cipher gcm(final int mode,
                              final SecretKey key,
                              final byte[] iv,
                              final byte[] aad) throws GeneralSecurityException {
        final var cipher = Cipher.getInstance("AES/GCM/NoPadding");
        cipher.init(mode, key, new GCMParameterSpec(TAG_LENGTH * 8, iv));
        cipher.updateAAD(aad);
        return cipher;
    }
}
