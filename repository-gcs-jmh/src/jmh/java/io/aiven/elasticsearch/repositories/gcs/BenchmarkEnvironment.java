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
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.Security;
import java.util.stream.Collectors;

import com.sun.management.HotSpotDiagnosticMXBean;
import org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider;
import org.openjdk.jmh.infra.BenchmarkParams;

final class BenchmarkEnvironment {

    private BenchmarkEnvironment() {
    }

    static void setUpJvm(final String g1RegionSize) {
        final var actual = ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class)
            .getVMOption("G1HeapRegionSize").getValue();
        if (Long.parseLong(actual) != RepositoryState.parseSize(g1RegionSize)) {
            throw new IllegalStateException(
                "G1HeapRegionSize is " + actual + " bytes, run the fork with -XX:G1HeapRegionSize=" + g1RegionSize);
        }
        Security.insertProviderAt(new BouncyCastleFipsProvider(), 1);
    }

    static void markTrial(final String event, final BenchmarkParams params) throws IOException {
        final var trialLog = System.getProperty("bench.trial_log", "");
        if (trialLog.isEmpty()) {
            return;
        }
        final var paramValues = params.getParamsKeys().stream()
            .map(key -> key + "=" + params.getParam(key))
            .collect(Collectors.joining(","));
        Files.writeString(
            Path.of(trialLog),
            System.currentTimeMillis() + " " + event + " " + params.getBenchmark() + " " + paramValues + "\n",
            StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }
}
