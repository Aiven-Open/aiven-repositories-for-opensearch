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
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.security.SecureRandom;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.opensearch.common.crypto.CryptoHandler;
import org.opensearch.common.crypto.DecryptedRangedStreamProvider;
import org.opensearch.common.crypto.EncryptedHeaderContentSupplier;
import org.opensearch.common.io.InputStreamContainer;

import com.amazonaws.encryptionsdk.CommitmentPolicy;
import com.amazonaws.encryptionsdk.CryptoAlgorithm;
import com.amazonaws.encryptionsdk.CryptoMaterialsManager;
import com.amazonaws.encryptionsdk.model.DecryptionMaterialsRequest;
import com.amazonaws.encryptionsdk.model.EncryptionMaterialsRequest;
import com.amazonaws.encryptionsdk.model.KeyBlob;

/**
 * Writes and reads blobs in the AWS Encryption SDK message format, version 2, suite 0x0478, framed, with an empty
 * encryption context and one RSA-wrapped data key. The materials manager generates, caches and unwraps data keys.
 * The header and frames are serialised here, so whole blobs, multipart parts and ranged reads share one code path.
 */
final class AwsMessageFormatCryptoHandler implements CryptoHandler<MessageHeader, MessageHeader> {

    static final CryptoAlgorithm ALGORITHM = CryptoAlgorithm.ALG_AES_256_GCM_HKDF_SHA512_COMMIT_KEY;

    /** Bytes requested to parse a header. A header is 678 bytes for an RSA-4096 key and a 4-character key_provider_name. */
    static final int HEADER_READ_LENGTH = 4096;

    private final CryptoMaterialsManager materialsManager;

    private final int frameLength;

    private final Runnable onClose;

    private final AtomicBoolean closed = new AtomicBoolean();

    private final SecureRandom random = new SecureRandom();

    AwsMessageFormatCryptoHandler(final CryptoMaterialsManager materialsManager, final Runnable onClose) {
        this(materialsManager, MessageFormat.FRAME_LENGTH, onClose);
    }

    AwsMessageFormatCryptoHandler(final CryptoMaterialsManager materialsManager,
                                  final int frameLength,
                                  final Runnable onClose) {
        this.materialsManager = materialsManager;
        this.frameLength = frameLength;
        this.onClose = onClose;
    }

    @Override
    public MessageHeader initEncryptionMetadata() {
        // A plaintext size of 0 lets the caching materials manager reuse the data key. It bypasses the cache for -1.
        final var materials = materialsManager.getMaterialsForEncrypt(EncryptionMaterialsRequest.newBuilder()
            .setContext(Map.of())
            .setRequestedAlgorithm(ALGORITHM)
            .setCommitmentPolicy(CommitmentPolicy.RequireEncryptRequireDecrypt)
            .setPlaintextSize(0)
            .build());
        if (materials.getAlgorithm() != ALGORITHM || materials.getEncryptedDataKeys().size() != 1) {
            throw new IllegalStateException("Expected " + ALGORITHM + " with one encrypted data key, got "
                + materials.getAlgorithm() + " with " + materials.getEncryptedDataKeys().size());
        }
        final var messageId = new byte[MessageFormat.MESSAGE_ID_LENGTH];
        random.nextBytes(messageId);
        return MessageHeader.create(
            messageId, materials.getEncryptedDataKeys().get(0), materials.getCleartextDataKey().getEncoded(), frameLength);
    }

    @Override
    public MessageHeader loadEncryptionMetadata(final EncryptedHeaderContentSupplier headerContentSupplier) throws IOException {
        // Core's supplier only works with a start of 0. It pads with zeros past the end of a short blob.
        final var bytes = headerContentSupplier.supply(0, HEADER_READ_LENGTH - 1);
        try {
            return readHeader(new DataInputStream(new ByteArrayInputStream(bytes)));
        } catch (final EOFException e) {
            throw new IOException("Message header is longer than " + HEADER_READ_LENGTH + " bytes", e);
        }
    }

    private MessageHeader readHeader(final DataInputStream in) throws IOException {
        return MessageHeader.read(in, this::decryptDataKey);
    }

    private byte[] decryptDataKey(final KeyBlob encryptedDataKey) throws IOException {
        try {
            return materialsManager.decryptMaterials(DecryptionMaterialsRequest.newBuilder()
                    .setAlgorithm(ALGORITHM)
                    .setEncryptionContext(Map.of())
                    .setEncryptedDataKeys(List.of(encryptedDataKey))
                    .build())
                .getDataKey()
                .getKey()
                .getEncoded();
        } catch (final RuntimeException e) {
            throw new IOException("Couldn't decrypt the data key of key provider [" + encryptedDataKey.getProviderId() + "]", e);
        }
    }

    @Override
    public long adjustContentSizeForPartialEncryption(final MessageHeader header, final long contentSize) {
        final var frameLength = header.frameLength();
        return Math.max(1, contentSize / frameLength) * frameLength;
    }

    @Override
    public long estimateEncryptedLengthOfEntireContent(final MessageHeader header, final long contentLength) {
        return header.length() + framesLength(header.frameLength(), contentLength, true);
    }

    /** Length of the frames holding {@code contentLength} bytes of plaintext, with or without the final frame. */
    private static long framesLength(final int frameLength, final long contentLength, final boolean withFinalFrame) {
        final var frames = contentLength / frameLength;
        final var regularFramesLength = frames * (frameLength + MessageFormat.FRAME_OVERHEAD);
        return withFinalFrame
            ? regularFramesLength + MessageFormat.FINAL_FRAME_OVERHEAD + contentLength % frameLength
            : regularFramesLength;
    }

    @Override
    public long estimateDecryptedLength(final MessageHeader header, final long contentLength) {
        final var frameLength = header.frameLength();
        final var framesLength = contentLength - header.length() - MessageFormat.FINAL_FRAME_OVERHEAD;
        if (framesLength < 0) {
            throw new IllegalArgumentException("Encrypted length " + contentLength + " is shorter than an empty message");
        }
        final var frames = framesLength / (frameLength + MessageFormat.FRAME_OVERHEAD);
        final var finalFrameContentLength = framesLength - frames * (frameLength + MessageFormat.FRAME_OVERHEAD);
        return frames * frameLength + finalFrameContentLength;
    }

    @Override
    public InputStreamContainer createEncryptingStream(final MessageHeader header, final InputStreamContainer stream) {
        return new InputStreamContainer(
            new FrameWriter(header, stream.getInputStream(), 1, true, true),
            estimateEncryptedLengthOfEntireContent(header, stream.getContentLength()),
            0);
    }

    @Override
    public InputStreamContainer createEncryptingStreamOfPart(final MessageHeader header,
                                                             final InputStreamContainer stream,
                                                             final int totalStreams,
                                                             final int streamIdx) {
        final var frameLength = header.frameLength();
        final var offset = stream.getOffset();
        final var length = stream.getContentLength();
        final var lastPart = streamIdx == totalStreams - 1;
        if (offset % frameLength != 0 || !lastPart && length % frameLength != 0) {
            throw new IllegalArgumentException("Part " + streamIdx + " at offset " + offset + " with length " + length
                + " doesn't hold whole frames of " + frameLength + " bytes");
        }
        final var firstPart = streamIdx == 0;
        return new InputStreamContainer(
            new FrameWriter(header, stream.getInputStream(), offset / frameLength + 1, firstPart, lastPart),
            (firstPart ? header.length() : 0) + framesLength(frameLength, length, lastPart),
            firstPart ? 0 : header.length() + framesLength(frameLength, offset, false));
    }

    @Override
    public InputStream createDecryptingStream(final InputStream encryptingStream) {
        return new FrameReader(encryptingStream, this::readHeader);
    }

    @Override
    public DecryptedRangedStreamProvider createDecryptingStreamOfRange(final MessageHeader header,
                                                                       final long startPosOfRawContent,
                                                                       final long endPosOfRawContent) {
        final var frameLength = header.frameLength();
        final var firstFrame = startPosOfRawContent / frameLength;
        final var lastFrame = endPosOfRawContent / frameLength;
        final var frameSize = (long) frameLength + MessageFormat.FRAME_OVERHEAD;
        // The end covers the final frame too: its marker and content length add 8 bytes to a regular frame.
        final var adjustedRange = new long[] {
            header.length() + firstFrame * frameSize,
            header.length() + (lastFrame + 1) * frameSize + 8 - 1
        };
        return new DecryptedRangedStreamProvider(
            adjustedRange,
            in -> new FrameReader(
                in,
                header,
                firstFrame + 1,
                startPosOfRawContent - firstFrame * frameLength,
                endPosOfRawContent - startPosOfRawContent + 1));
    }

    // Core shares one handler between repositories with the same crypto_settings and closes it when any of them
    // closes. So close() only removes the handler from core's registry and leaves it usable.
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            onClose.run();
        }
    }
}
