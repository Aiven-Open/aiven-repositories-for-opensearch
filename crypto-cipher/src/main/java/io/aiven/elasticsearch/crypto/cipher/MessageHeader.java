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

import javax.crypto.SecretKey;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;

import com.amazonaws.encryptionsdk.model.KeyBlob;

/**
 * Version 2 message header with an empty encryption context and exactly one encrypted data key.
 * Holds the keys derived for the message, so it serves both for writing and for reading a blob.
 */
final class MessageHeader {

    @FunctionalInterface
    interface DataKeyDecryptor {
        byte[] decrypt(KeyBlob encryptedDataKey) throws IOException;
    }

    private final byte[] messageId;

    private final int frameLength;

    private final SecretKey frameKey;

    private final byte[] authenticatedFields;

    private final byte[] tag;

    private MessageHeader(final byte[] messageId,
                          final int frameLength,
                          final SecretKey frameKey,
                          final byte[] authenticatedFields,
                          final byte[] tag) {
        this.messageId = messageId;
        this.frameLength = frameLength;
        this.frameKey = frameKey;
        this.authenticatedFields = authenticatedFields;
        this.tag = tag;
    }

    static MessageHeader create(final byte[] messageId,
                                final KeyBlob encryptedDataKey,
                                final byte[] dataKey,
                                final int frameLength) {
        try {
            final var keys = MessageFormat.deriveKeys(dataKey, messageId);
            final var authenticatedFields =
                serializeAuthenticatedFields(messageId, encryptedDataKey, frameLength, keys.commitment());
            final var tag = MessageFormat.seal(
                keys.frameKey(), new byte[MessageFormat.IV_LENGTH], authenticatedFields, new byte[0]);
            return new MessageHeader(messageId, frameLength, keys.frameKey(), authenticatedFields, tag);
        } catch (final GeneralSecurityException e) {
            throw new IllegalStateException("Couldn't create message header", e);
        }
    }

    static MessageHeader read(final DataInputStream in, final DataKeyDecryptor dataKeyDecryptor) throws IOException {
        final var version = in.readByte();
        check(version == MessageFormat.VERSION, "Unsupported message format version " + version);
        final var algorithmSuiteId = in.readShort();
        check(algorithmSuiteId == MessageFormat.ALGORITHM_SUITE_ID,
            String.format("Unsupported algorithm suite 0x%04X", algorithmSuiteId));
        final var messageId = readBytes(in, MessageFormat.MESSAGE_ID_LENGTH);
        final var encryptionContextLength = in.readUnsignedShort();
        check(encryptionContextLength == 0,
            "Encryption context must be empty, found " + encryptionContextLength + " bytes");
        final var encryptedDataKeyCount = in.readUnsignedShort();
        check(encryptedDataKeyCount == 1, "Expected exactly one encrypted data key, found " + encryptedDataKeyCount);
        final var encryptedDataKey = new KeyBlob(
            new String(readBytes(in, in.readUnsignedShort()), StandardCharsets.UTF_8),
            readBytes(in, in.readUnsignedShort()),
            readBytes(in, in.readUnsignedShort()));
        final var contentType = in.readByte();
        check(contentType == MessageFormat.FRAMED_CONTENT, "Expected framed content, found content type " + contentType);
        final var frameLength = in.readInt();
        check(frameLength > 0, "Invalid frame length " + frameLength);
        final var commitment = readBytes(in, MessageFormat.COMMITMENT_LENGTH);
        final var tag = readBytes(in, MessageFormat.TAG_LENGTH);

        final MessageFormat.DerivedKeys keys;
        try {
            keys = MessageFormat.deriveKeys(dataKeyDecryptor.decrypt(encryptedDataKey), messageId);
        } catch (final GeneralSecurityException e) {
            throw new IOException("Couldn't derive message keys", e);
        }
        final var authenticatedFields = serializeAuthenticatedFields(messageId, encryptedDataKey, frameLength, commitment);
        // Never skip either check. Without them a modified header or a substituted data key goes undetected.
        if (!MessageDigest.isEqual(commitment, keys.commitment())) {
            throw new IOException("Key commitment doesn't match the data key");
        }
        try {
            MessageFormat.open(keys.frameKey(), new byte[MessageFormat.IV_LENGTH], authenticatedFields, tag);
        } catch (final GeneralSecurityException e) {
            throw new IOException("Message header failed authentication", e);
        }
        return new MessageHeader(messageId, frameLength, keys.frameKey(), authenticatedFields, tag);
    }

    private static void check(final boolean condition, final String message) throws IOException {
        if (!condition) {
            throw new IOException(message);
        }
    }

    private static byte[] readBytes(final DataInputStream in, final int length) throws IOException {
        final var bytes = new byte[length];
        in.readFully(bytes);
        return bytes;
    }

    private static byte[] serializeAuthenticatedFields(final byte[] messageId,
                                                       final KeyBlob encryptedDataKey,
                                                       final int frameLength,
                                                       final byte[] commitment) {
        final var providerId = encryptedDataKey.getProviderId().getBytes(StandardCharsets.UTF_8);
        final var providerInfo = encryptedDataKey.getProviderInformation();
        final var wrappedKey = encryptedDataKey.getEncryptedDataKey();
        return ByteBuffer.allocate(1 + 2 + messageId.length + 2 + 2
                + 2 + providerId.length + 2 + providerInfo.length + 2 + wrappedKey.length
                + 1 + 4 + commitment.length)
            .put(MessageFormat.VERSION)
            .putShort(MessageFormat.ALGORITHM_SUITE_ID)
            .put(messageId)
            .putShort((short) 0)
            .putShort((short) 1)
            .putShort((short) providerId.length)
            .put(providerId)
            .putShort((short) providerInfo.length)
            .put(providerInfo)
            .putShort((short) wrappedKey.length)
            .put(wrappedKey)
            .put(MessageFormat.FRAMED_CONTENT)
            .putInt(frameLength)
            .put(commitment)
            .array();
    }

    byte[] toByteArray() {
        return ByteBuffer.allocate(length()).put(authenticatedFields).put(tag).array();
    }

    int length() {
        return authenticatedFields.length + MessageFormat.TAG_LENGTH;
    }

    byte[] messageId() {
        return messageId;
    }

    int frameLength() {
        return frameLength;
    }

    SecretKey frameKey() {
        return frameKey;
    }
}
