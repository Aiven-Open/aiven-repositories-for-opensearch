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
import java.util.concurrent.atomic.AtomicInteger;

import org.opensearch.common.blobstore.BlobContainer;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

public class WriteFormatV2Benchmark extends RepositoryState {

    @Param({"64kb", "4mb", "64mb"})
    public String blobSize;

    @Override
    int contentLength() {
        return parseSize(blobSize);
    }

    @Override
    boolean discardWrites() {
        return true;
    }

    @Override
    BlobContainer setUpContainer() {
        return blobContainer(true);
    }

    @Benchmark
    public void writeBlob(final BlobNames names) throws IOException {
        container.writeBlob(names.next(), new ByteArrayInputStream(content), content.length, false);
    }

    @State(Scope.Thread)
    public static class BlobNames {

        private static final AtomicInteger THREADS = new AtomicInteger();

        private final String prefix = BLOB_NAME + "-" + THREADS.getAndIncrement() + "-";

        private int count;

        String next() {
            return prefix + count++ % 4;
        }
    }
}
