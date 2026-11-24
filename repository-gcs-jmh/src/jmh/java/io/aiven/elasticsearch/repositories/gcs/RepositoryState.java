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
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.unit.ByteSizeValue;

import io.aiven.elasticsearch.repositories.BlobStoreRepository;
import io.aiven.elasticsearch.repositories.RepositoryStorageIOProvider.StorageIO;
import io.aiven.elasticsearch.repositories.io.CryptoIOProvider;

import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.BenchmarkParams;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(3)
@Warmup(iterations = 3, time = 5)
@Measurement(iterations = 5, time = 5)
public abstract class RepositoryState {

    static final String BLOB_NAME = "blob";

    @Param("memory")
    public String backend;

    @Param("random")
    public String payload;

    @Param("4m")
    public String g1RegionSize;

    byte[] content;

    BlobContainer container;

    private KeyPair keyPair;

    private CryptoIOProvider repositoryKey;

    private Function<CryptoIOProvider, StorageIO> storage;

    private GcsBucket gcsBucket;

    private String basePath;

    @Setup(Level.Trial)
    public void setUp(final BenchmarkParams params) throws IOException {
        BenchmarkEnvironment.setUpJvm(g1RegionSize);
        final var keys = RsaKeyFiles.read();
        keyPair = keys.keyPair();
        repositoryKey = new CryptoIOProvider(
            keys.createKey(), Math.toIntExact(BlobStoreRepository.BUFFER_SIZE_SETTING.get(Settings.EMPTY).getBytes()));
        content = content(contentLength());
        if ("memory".equals(backend)) {
            final Map<String, byte[]> bucket = new ConcurrentHashMap<>();
            storage = crypto -> new InMemoryStorageIO(bucket, crypto, discardWrites());
            basePath = "base";
        } else if ("gcs".equals(backend)) {
            gcsBucket = GcsBucket.fromSystemProperties();
            gcsBucket.claimBasePath();
            storage = gcsBucket::storageIO;
            basePath = gcsBucket.newTrialPath();
        } else {
            throw new IllegalArgumentException("Unknown backend " + backend);
        }
        container = setUpContainer();
        BenchmarkEnvironment.markTrial("start", params);
    }

    @TearDown(Level.Trial)
    public void tearDown(final BenchmarkParams params) throws IOException {
        BenchmarkEnvironment.markTrial("end", params);
        if (gcsBucket != null) {
            gcsBucket.deleteTrialPath(basePath);
        }
    }

    abstract int contentLength();

    abstract BlobContainer setUpContainer() throws IOException;

    boolean discardWrites() {
        return false;
    }

    BlobContainer blobContainer(final boolean encryptionFormatV2) {
        return settingsService().blobContainer(basePath, encryptionFormatV2);
    }

    void writeRepositoryMetadata() throws IOException {
        settingsService().writeRepositoryMetadata(basePath + "/");
    }

    static int parseSize(final String size) {
        return Math.toIntExact(ByteSizeValue.parseBytesSizeValue(size, "size").getBytes());
    }

    private BenchmarkSettingsService settingsService() {
        return new BenchmarkSettingsService(storage, keyPair, repositoryKey);
    }

    private byte[] content(final int size) {
        final var random = new Random(size);
        final var bytes = new byte[size];
        if ("random".equals(payload)) {
            random.nextBytes(bytes);
        } else if ("text".equals(payload)) {
            final var letters = "abcdefghijklmnopqrstuvwxyz \n".getBytes(StandardCharsets.US_ASCII);
            for (int i = 0; i < size; i++) {
                bytes[i] = letters[random.nextInt(letters.length)];
            }
        } else {
            throw new IllegalArgumentException("Unknown payload " + payload);
        }
        return bytes;
    }
}
