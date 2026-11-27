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
import java.util.Arrays;
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

    private final long contentLength;

    private long remaining;

    private long sequenceNumber;

    private boolean headerWritten;

    private boolean finished;

    // Reused for every frame of this stream
    private byte[] plaintext = EMPTY;

    private byte[] frame = EMPTY;

    private byte[] buffer = EMPTY;

    private int bufferLength;

    private int position;

    private boolean markedAtStart;

    /**
     * @param firstSequenceNumber sequence number of the first frame this stream writes, starting from 1
     * @param writesHeader        whether the stream starts with the message header
     * @param writesFinalFrame    whether the source ends the message. If not, the source must hold whole frames.
     * @param contentLength       expected number of plaintext bytes in the source, or a negative value if unknown
     */
    FrameWriter(final MessageHeader header,
                final InputStream source,
                final long firstSequenceNumber,
                final boolean writesHeader,
                final boolean writesFinalFrame,
                final long contentLength) {
        this.header = header;
        this.source = source;
        this.firstSequenceNumber = firstSequenceNumber;
        this.writesHeader = writesHeader;
        this.writesFinalFrame = writesFinalFrame;
        this.contentLength = contentLength;
        this.remaining = contentLength;
        this.sequenceNumber = firstSequenceNumber;
    }

    @Override
    public int read() throws IOException {
        while (position == bufferLength) {
            if (!fill()) {
                return -1;
            }
        }
        return buffer[position++] & 0xFF;
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
        Objects.checkFromIndexSize(off, len, b.length);
        if (len == 0) {
            return 0;
        }
        while (position == bufferLength) {
            if (!fill()) {
                return -1;
            }
        }
        final var n = Math.min(len, bufferLength - position);
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
                bufferLength = buffer.length;
                return true;
            }
        }
        if (finished) {
            bufferLength = 0;
            return false;
        }
        final var frameLength = header.frameLength();
        final var plaintextLength = readFrame(frameLength);
        if (plaintextLength == frameLength) {
            encryptFrame(false, plaintextLength);
        } else if (writesFinalFrame) {
            encryptFrame(true, plaintextLength);
            finished = true;
        } else if (plaintextLength == 0) {
            bufferLength = 0;
            finished = true;
            return false;
        } else {
            throw new IOException("Part ends inside frame " + sequenceNumber + ". Only the last part may hold a partial frame");
        }
        sequenceNumber++;
        return true;
    }

    private int readFrame(final int frameLength) throws IOException {
        final var expected = remaining < 0 ? frameLength : (int) Math.min(frameLength, remaining);
        if (plaintext.length < expected) {
            plaintext = new byte[expected];
        }
        var length = 0;
        while (length < frameLength) {
            if (length == plaintext.length) {
                final var next = source.read();
                if (next == -1) {
                    break;
                }
                plaintext = Arrays.copyOf(plaintext, frameLength);
                plaintext[length++] = (byte) next;
                continue;
            }
            final var n = source.read(plaintext, length, plaintext.length - length);
            if (n == -1) {
                break;
            }
            length += n;
        }
        remaining -= length;
        return length;
    }

    private void encryptFrame(final boolean finalFrame, final int plaintextLength) throws IOException {
        if (sequenceNumber > MessageFormat.MAX_SEQUENCE_NUMBER
            || sequenceNumber == MessageFormat.MAX_SEQUENCE_NUMBER && !finalFrame) {
            throw new IOException("Content is too large for frame length " + header.frameLength());
        }
        if (frame.length < MessageFormat.FINAL_FRAME_OVERHEAD + plaintextLength) {
            frame = new byte[MessageFormat.FINAL_FRAME_OVERHEAD + plaintext.length];
        }
        final var iv = MessageFormat.frameIv(sequenceNumber);
        final var fields = ByteBuffer.wrap(frame);
        if (finalFrame) {
            fields.putInt(MessageFormat.FINAL_FRAME_MARKER);
        }
        fields.putInt((int) sequenceNumber).put(iv);
        if (finalFrame) {
            fields.putInt(plaintextLength);
        }
        try {
            MessageFormat.seal(
                header.frameKey(),
                iv,
                MessageFormat.frameAad(header.messageId(), finalFrame, sequenceNumber, plaintextLength),
                plaintext,
                plaintextLength,
                frame,
                fields.position());
        } catch (final GeneralSecurityException e) {
            throw new IOException("Couldn't encrypt frame " + sequenceNumber, e);
        }
        buffer = frame;
        bufferLength = fields.position() + plaintextLength + MessageFormat.TAG_LENGTH;
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
        remaining = contentLength;
        sequenceNumber = firstSequenceNumber;
        headerWritten = false;
        finished = false;
        buffer = EMPTY;
        bufferLength = 0;
        position = 0;
    }

    @Override
    public void close() throws IOException {
        plaintext = EMPTY;
        frame = EMPTY;
        buffer = EMPTY;
        bufferLength = 0;
        position = 0;
        source.close();
    }
}
