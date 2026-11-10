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

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Objects;

/**
 * Decrypts frames in sequence-number order and returns a window of their plaintext.
 */
final class FrameReader extends InputStream {

    @FunctionalInterface
    interface HeaderReader {
        MessageHeader read(DataInputStream in) throws IOException;
    }

    private static final byte[] EMPTY = new byte[0];

    private final DataInputStream in;

    private final HeaderReader headerReader;

    private MessageHeader header;

    private long sequenceNumber;

    private long bytesToSkip;

    private long bytesRemaining;

    private boolean finalFrameRead;

    private byte[] plaintext = EMPTY;

    private int position;

    /** Reads a whole message: the header, then every frame up to and including the final frame. */
    FrameReader(final InputStream in, final HeaderReader headerReader) {
        this(in, null, headerReader, 1, 0, Long.MAX_VALUE);
    }

    /**
     * Reads frames that start at {@code firstSequenceNumber}, drops the first {@code bytesToSkip} bytes of their
     * plaintext and returns at most {@code length} bytes.
     */
    FrameReader(final InputStream in,
                final MessageHeader header,
                final long firstSequenceNumber,
                final long bytesToSkip,
                final long length) {
        this(in, header, null, firstSequenceNumber, bytesToSkip, length);
    }

    private FrameReader(final InputStream in,
                        final MessageHeader header,
                        final HeaderReader headerReader,
                        final long firstSequenceNumber,
                        final long bytesToSkip,
                        final long length) {
        this.in = new DataInputStream(in);
        this.header = header;
        this.headerReader = headerReader;
        this.sequenceNumber = firstSequenceNumber;
        this.bytesToSkip = bytesToSkip;
        this.bytesRemaining = length;
    }

    @Override
    public int read() throws IOException {
        final var one = new byte[1];
        return read(one, 0, 1) == -1 ? -1 : one[0] & 0xFF;
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
        Objects.checkFromIndexSize(off, len, b.length);
        if (len == 0) {
            return 0;
        }
        if (bytesRemaining == 0) {
            return -1;
        }
        while (position == plaintext.length) {
            if (finalFrameRead) {
                return -1;
            }
            readFrame();
        }
        final var n = (int) Math.min(Math.min(len, plaintext.length - position), bytesRemaining);
        System.arraycopy(plaintext, position, b, off, n);
        position += n;
        bytesRemaining -= n;
        return n;
    }

    private void readFrame() throws IOException {
        if (header == null) {
            header = headerReader.read(in);
        }
        final var first = in.readInt();
        final var finalFrame = first == MessageFormat.FINAL_FRAME_MARKER;
        final var frameSequenceNumber = Integer.toUnsignedLong(finalFrame ? in.readInt() : first);
        if (frameSequenceNumber != sequenceNumber) {
            throw new IOException("Expected frame " + sequenceNumber + ", found frame " + frameSequenceNumber);
        }
        final var iv = new byte[MessageFormat.IV_LENGTH];
        in.readFully(iv);
        if (!Arrays.equals(iv, MessageFormat.frameIv(sequenceNumber))) {
            throw new IOException("Frame " + sequenceNumber + " has an unexpected IV");
        }
        final var contentLength = finalFrame ? in.readInt() : header.frameLength();
        if (contentLength < 0 || contentLength > header.frameLength()) {
            throw new IOException("Frame " + sequenceNumber + " has invalid content length " + contentLength);
        }
        final var ciphertextAndTag = new byte[contentLength + MessageFormat.TAG_LENGTH];
        in.readFully(ciphertextAndTag);
        try {
            plaintext = MessageFormat.open(
                header.frameKey(),
                iv,
                MessageFormat.frameAad(header.messageId(), finalFrame, sequenceNumber, contentLength),
                ciphertextAndTag);
        } catch (final GeneralSecurityException e) {
            throw new IOException("Frame " + sequenceNumber + " failed authentication", e);
        }
        sequenceNumber++;
        finalFrameRead = finalFrame;
        position = (int) Math.min(bytesToSkip, plaintext.length);
        bytesToSkip -= position;
    }

    @Override
    public void close() throws IOException {
        in.close();
    }
}
