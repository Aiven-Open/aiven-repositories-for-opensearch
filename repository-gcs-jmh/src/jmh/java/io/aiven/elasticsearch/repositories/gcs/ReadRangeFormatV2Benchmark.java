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

import java.io.IOException;

import org.opensearch.common.blobstore.BlobContainer;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Param;

public class ReadRangeFormatV2Benchmark extends AbstractReadBenchmark {

    @Param({"4kb", "64kb", "1mb", "3mb"})
    public String rangeSize;

    @Param({"frameStart", "acrossFrames"})
    public String rangePosition;

    @Param({"absent", "present"})
    public String repositoryMetadata;

    private int position;

    private int length;

    @Override
    int contentLength() {
        return RANGE_BLOB_LENGTH;
    }

    @Override
    BlobContainer setUpContainer() throws IOException {
        length = parseSize(rangeSize);
        position = rangeStart(rangePosition, length);
        final var reader = formatV2Reader(repositoryMetadata);
        checkRangeRead(reader, position, length);
        return reader;
    }

    @Benchmark
    public long readBlobRange(final ReadBuffer buffer) throws IOException {
        try (var in = container.readBlob(BLOB_NAME, position, length)) {
            return buffer.readToEnd(in);
        }
    }
}
