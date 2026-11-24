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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import org.opensearch.common.blobstore.BlobContainer;

import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

public abstract class AbstractReadBenchmark extends RepositoryState {

    // Large enough that no range reaches the end of the blob.
    static final int RANGE_BLOB_LENGTH = 16 * 1024 * 1024;

    private static final int FRAME_LENGTH = 1024 * 1024;

    // frameStart reads whole format v2 frames, acrossFrames moves the same range over one more frame boundary.
    static int rangeStart(final String rangePosition, final int rangeLength) {
        if ("frameStart".equals(rangePosition)) {
            return FRAME_LENGTH;
        }
        if ("acrossFrames".equals(rangePosition)) {
            return 2 * FRAME_LENGTH - Math.min(rangeLength, FRAME_LENGTH) / 2;
        }
        throw new IllegalArgumentException("Unknown rangePosition " + rangePosition);
    }

    BlobContainer formatV2Reader(final String repositoryMetadata) throws IOException {
        if ("present".equals(repositoryMetadata)) {
            writeRepositoryMetadata();
        } else if (!"absent".equals(repositoryMetadata)) {
            throw new IllegalArgumentException("Unknown repositoryMetadata " + repositoryMetadata);
        }
        final var reader = blobContainer(true);
        writeBlob(reader);
        return reader;
    }

    BlobContainer formatV1Reader() throws IOException {
        writeBlob(blobContainer(false));
        return blobContainer(true);
    }

    void checkRead(final BlobContainer reader) throws IOException {
        try (var in = reader.readBlob(BLOB_NAME)) {
            check(Arrays.equals(content, in.readAllBytes()));
        }
    }

    void checkRangeRead(final BlobContainer reader, final int position, final int length) throws IOException {
        try (var in = reader.readBlob(BLOB_NAME, position, length)) {
            check(Arrays.equals(Arrays.copyOfRange(content, position, position + length), in.readAllBytes()));
        }
    }

    private void writeBlob(final BlobContainer writer) throws IOException {
        writer.writeBlob(BLOB_NAME, new ByteArrayInputStream(content), content.length, false);
    }

    private static void check(final boolean matches) {
        if (!matches) {
            throw new IllegalStateException("Read returned different bytes than were written");
        }
    }

    @State(Scope.Thread)
    public static class ReadBuffer {

        private final byte[] bytes = new byte[64 * 1024];

        long readToEnd(final InputStream in) throws IOException {
            long total = 0;
            int read;
            while ((read = in.read(bytes)) != -1) {
                total += read;
            }
            return total;
        }
    }
}
