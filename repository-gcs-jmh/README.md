# repository-gcs benchmarks

JMH benchmarks for the encryption format v2 write and read paths, profiled with async-profiler.

| Benchmark | What it measures |
|---|---|
| `WriteFormatV2Benchmark.writeBlob` | format v2 write; the memory backend discards the written bytes |
| `ReadFormatV2Benchmark.readBlob` | format v2 read, with and without `repository_metadata.json` |
| `ReadFormatV1Benchmark.readBlob` | version 1 blobs read with format v2 enabled |
| `ReadRangeFormatV2Benchmark.readBlobRange` | format v2 range read of a 16 MiB blob |
| `ReadRangeFormatV1Benchmark.readBlobRange` | version 1 range read of a 16 MiB blob with format v2 enabled |

Range reads take `rangeSize` (4kb, 64kb, 1mb, 3mb) and `rangePosition`: `frameStart` reads whole 1 MiB frames,

## RSA keys

```bash
mkdir -p -m 700 ~/.config/aiven-jmh && cd ~/.config/aiven-jmh
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:3072 -out private.pem
openssl pkey -in private.pem -pubout -out public.pem
chmod 600 private.pem
```

## Run

```bash
# all benchmarks, in-memory storage
./gradlew :repository-gcs-jmh:jmh

# one benchmark, one blob size, short iterations
./gradlew :repository-gcs-jmh:jmh -PjmhInclude=ReadFormatV1Benchmark -PjmhBlobSize=4mb -PjmhShort

# parameters, comma separated
./gradlew :repository-gcs-jmh:jmh -PjmhBlobSize=64kb,64mb -PjmhPayload=random,text -PjmhThreads=4
./gradlew :repository-gcs-jmh:jmh -PjmhInclude=ReadRange -PjmhRangeSize=4kb,1mb -PjmhRangePosition=acrossFrames

# concurrency sweep
for t in 1 4 16; do ./gradlew :repository-gcs-jmh:jmh -PjmhThreads=$t; done
```

## G1 region size

G1 treats arrays over half a region as humongous. 
The default is a 4 GB heap (`-PjmhHeap`) with 4 MiB regions.

| Plan memory | Heap | G1 region |
|---|---|---|
| 16 GB | 8 GB | 4m |
| 32 GB | 16 GB | 8m |
| 64 GB and more | 31 GB | 16m |

Use 2m only to make every 1 MiB frame humongous on purpose. One region size per run:

```bash
for r in 2m 4m 16m; do ./gradlew :repository-gcs-jmh:jmh -PjmhG1RegionSize=$r; done
```

## Real GCS bucket

```bash
gcloud auth application-default login            # or -PgcsCredentialsPath=/path/to/key.json

./gradlew :repository-gcs-jmh:jmh -PjmhBackend=gcs \
    -PgcsBucketName=<bucket> -PgcsBasePath=local-crypto-test/jmh

# delete everything under gcsBasePath, refuses without the jmh-benchmark-data marker
./gradlew :repository-gcs-jmh:jmhCleanGcs -PgcsBucketName=<bucket> -PgcsBasePath=local-crypto-test/jmh
```

Each trial writes under `<gcsBasePath>/<yyyyMMdd-HHmmss>-<id>/` and deletes it at the end.

## GCP runner

The `jmhRunner*` tasks create and delete a GCE machine.

```bash
gcloud auth login

./gradlew :repository-gcs-jmh:jmhRunnerCreate                      # prints the runner address
./gradlew :repository-gcs-jmh:jmhRemote -PrunnerHost=<ip> -PjmhBackend=gcs -PjmhHeap=8g \
    -PgcsBucketName=<bucket> -PgcsBasePath=jmh -PgcsCredentialsPath=<service account key.json>
./gradlew :repository-gcs-jmh:jmhRemoteFetch -PrunnerHost=<ip>    # any time, copies what the run has so far
./gradlew :repository-gcs-jmh:jmhRemoteStop -PrunnerHost=<ip>
./gradlew :repository-gcs-jmh:jmhHeatmaps
./gradlew :repository-gcs-jmh:jmhRunnerDelete
```

| Switch | Default |
|---|---|
| `-PgcpProject` | gcloud configuration |
| `-PrunnerName`, `-PrunnerZone` | `aiven-jmh-<user name>`, `europe-west1-c` |
| `-PrunnerMachineType` | `n2d-standard-4`; Arm `c4a-*` and `t2a-*` get the arm64 image |
| `-PrunnerSshSource` | this machine's public address, `/32` |

The benchmarks run as a systemd unit `aiven-jmh-<run id>`, so they keep going after the SSH session ends and have their
own cgroup. Java 21 is downloaded to `~/aiven-jmh/jdk` on the first run. `jmhRemote` refuses to start while another
run is active. Any Linux machine with passwordless sudo works as a runner:

| Switch | Default |
|---|---|
| `-PrunnerHost` | required |
| `-PrunnerUser`, `-PrunnerSshKey`, `-PrunnerPort` | `jmh`, `~/.config/aiven-jmh/runner-key`, `22` |
| `-PrunnerCpus`, `-PrunnerMemory` | unit `AllowedCPUs` and `MemoryMax`, for example `0-3` and `16G` |
| `-PrunnerRun` | latest run, for fetch and stop |

The fetched run is in `build/jmh-remote/runs/<run id>/`:

| Path | Content |
|---|---|
| `results/`, `jmh.log`, `status` | JMH results, output and exit status |
| `async/<benchmark>/jfr-cpu.jfr` | cpu, wall, alloc and lock samples |
| `gc/gc-<pid>.log` | GC log with humongous allocations, one per fork |
| `metrics/system.txt` | kernel, CPU, memory, MGLRU, transparent huge pages, GCE machine type, Java version |
| `metrics/trials.log` | trial start and end times with parameters, in epoch milliseconds |
| `metrics/*.log` | samples every second of `/proc/{stat,vmstat,meminfo,net/dev}`, `/proc/pressure/*` and the unit cgroup's memory, cpu and io files; `/sys/kernel/debug/lru_gen` every 10 seconds |

Each sample starts with `# <epoch seconds>`.

## Profiles

```bash
./gradlew :repository-gcs-jmh:jmhHeatmaps
```

| Output | Path |
|---|---|
| JMH results | `build/results/jmh/results-g1-<region size>-threads-<threads>.json` |
| JFR recordings | `build/reports/jmh/async/<benchmark>/jfr-cpu.jfr` |
| Heatmaps | `build/reports/jmh/async/<benchmark>/heatmap-{cpu,wall,alloc,lock}.html` |

Recordings hold cpu, alloc and lock samples, plus wall clock on Linux.
Another async-profiler build: `-PasyncProfilerLib=/path/to/libasyncProfiler.so`.

## Run the jar directly

```bash
./gradlew :repository-gcs-jmh:jmhJar
BC_FIPS=$(find ~/.gradle/caches -name 'bc-fips-2.1.2.jar' | head -1)
JMH_JAR=$(ls repository-gcs-jmh/build/libs/repository-gcs-jmh-*-jmh.jar)
java -cp "$BC_FIPS:$JMH_JAR" org.openjdk.jmh.Main -l
java -cp "$BC_FIPS:$JMH_JAR" org.openjdk.jmh.Main WriteFormatV2Benchmark -jvmArgsAppend "-Xmx4g -XX:G1HeapRegionSize=4m"
```
