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
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.util.Objects;

/**
 * Encrypts plaintext read from the source into frames, starting at a given sequence number.
 * The whole-blob stream and every part of a multipart upload use this class, so parts concatenate to exactly the
 * bytes of the whole-blob stream.
 */
final class FrameWriter extends InputStream {

    private static final byte[] EMPTY = new byte[0];

    private final MessageHeader header;

    private final InputStream source;

    private final long firstSequenceNumber;

    private final boolean writesHeader;

    private final boolean writesFinalFrame;

    private long sequenceNumber;

    private boolean headerWritten;

    private boolean finished;

    private byte[] buffer = EMPTY;

    private int position;

    private boolean markedAtStart;

    /**
     * @param firstSequenceNumber sequence number of the first frame this stream writes, starting from 1
     * @param writesHeader        whether the stream starts with the message header
     * @param writesFinalFrame    whether the source ends the message. If not, the source must hold whole frames.
     */
    FrameWriter(final MessageHeader header,
                final InputStream source,
                final long firstSequenceNumber,
                final boolean writesHeader,
                final boolean writesFinalFrame) {
        this.header = header;
        this.source = source;
        this.firstSequenceNumber = firstSequenceNumber;
        this.writesHeader = writesHeader;
        this.writesFinalFrame = writesFinalFrame;
        this.sequenceNumber = firstSequenceNumber;
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
        while (position == buffer.length) {
            if (!fill()) {
                return -1;
            }
        }
        final var n = Math.min(len, buffer.length - position);
        System.arraycopy(buffer, position, b, off, n);
        position += n;
        return n;
    }

    private boolean fill() throws IOException {
        position = 0;
        if (!headerWritten) {
            headerWritten = true;
            if (writesHeader) {
                buffer = header.toByteArray();
                return true;
            }
        }
        if (finished) {
            buffer = EMPTY;
            return false;
        }
        final var frameLength = header.frameLength();
        final var plaintext = source.readNBytes(frameLength);
        if (plaintext.length == frameLength) {
            buffer = encryptFrame(false, plaintext);
        } else if (writesFinalFrame) {
            buffer = encryptFrame(true, plaintext);
            finished = true;
        } else if (plaintext.length == 0) {
            buffer = EMPTY;
            finished = true;
            return false;
        } else {
            throw new IOException("Part ends inside frame " + sequenceNumber + ". Only the last part may hold a partial frame");
        }
        sequenceNumber++;
        return true;
    }

    private byte[] encryptFrame(final boolean finalFrame, final byte[] plaintext) throws IOException {
        if (sequenceNumber > MessageFormat.MAX_SEQUENCE_NUMBER
            || sequenceNumber == MessageFormat.MAX_SEQUENCE_NUMBER && !finalFrame) {
            throw new IOException("Content is too large for frame length " + header.frameLength());
        }
        final var iv = MessageFormat.frameIv(sequenceNumber);
        final byte[] sealed;
        try {
            sealed = MessageFormat.seal(
                header.frameKey(),
                iv,
                MessageFormat.frameAad(header.messageId(), finalFrame, sequenceNumber, plaintext.length),
                plaintext);
        } catch (final GeneralSecurityException e) {
            throw new IOException("Couldn't encrypt frame " + sequenceNumber, e);
        }
        final var frame = ByteBuffer.allocate(
            (finalFrame ? MessageFormat.FINAL_FRAME_OVERHEAD : MessageFormat.FRAME_OVERHEAD) + plaintext.length);
        if (finalFrame) {
            frame.putInt(MessageFormat.FINAL_FRAME_MARKER);
        }
        frame.putInt((int) sequenceNumber).put(iv);
        if (finalFrame) {
            frame.putInt(plaintext.length);
        }
        return frame.put(sealed).array();
    }

    @Override
    public boolean markSupported() {
        return source.markSupported();
    }

    /**
     * Supported only before the first read. Encryption is deterministic for a given header, so reset rewinds the
     * source and writes the same bytes again.
     */
    @Override
    public synchronized void mark(final int readlimit) {
        markedAtStart = !headerWritten && sequenceNumber == firstSequenceNumber;
        if (markedAtStart) {
            source.mark(readlimit);
        }
    }

    @Override
    public synchronized void reset() throws IOException {
        if (!markedAtStart) {
            throw new IOException("Mark is supported only before the first read");
        }
        source.reset();
        sequenceNumber = firstSequenceNumber;
        headerWritten = false;
        finished = false;
        buffer = EMPTY;
        position = 0;
    }

    @Override
    public void close() throws IOException {
        source.close();
    }
}
