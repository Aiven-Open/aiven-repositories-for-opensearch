#!/usr/bin/env bash
# Runs the JMH jar on the runner and samples Linux metrics next to it.
# Usage: run-benchmarks.sh <run dir> <JMH arguments...>
# @RUN_DIR@ and @PROFILER_LIB@ in the JMH arguments are replaced with paths on this machine.
set -euo pipefail

run_dir=$(cd "$1" && pwd)
shift
cd "$run_dir"
mkdir -p metrics gc results async

case $(uname -m) in
    aarch64) platform=linux-arm64 jdk_arch=aarch64 ;;
    x86_64) platform=linux-x64 jdk_arch=x64 ;;
    *) echo "Unsupported architecture $(uname -m)" >&2; exit 1 ;;
esac

jdk_dir=$HOME/aiven-jmh/jdk
if [[ ! -x $jdk_dir/bin/java ]]; then
    mkdir -p "$jdk_dir.download"
    curl -fsSL "https://api.adoptium.net/v3/binary/latest/21/ga/linux/$jdk_arch/jdk/hotspot/normal/eclipse" \
        | tar -xz --strip-components=1 -C "$jdk_dir.download"
    mv "$jdk_dir.download" "$jdk_dir"
fi

sampler_pid=
perf_event_paranoid=$(cat /proc/sys/kernel/perf_event_paranoid)
kptr_restrict=$(cat /proc/sys/kernel/kptr_restrict)

cleanup() {
    local status=$?
    [[ -n $sampler_pid ]] && kill "$sampler_pid" 2>/dev/null || true
    rm -f secrets/*
    sudo -n sysctl -q -w "kernel.perf_event_paranoid=$perf_event_paranoid" "kernel.kptr_restrict=$kptr_restrict" || true
    echo "$status" > status
}
trap cleanup EXIT
trap 'exit 143' TERM INT

# Kernel frames in the cpu and wall profiles need these.
sudo -n sysctl -q -w kernel.perf_event_paranoid=1 kernel.kptr_restrict=0 \
    || echo "Can't change perf_event_paranoid, profiles won't have kernel frames" >&2

cgroup=/sys/fs/cgroup$(awk -F: '$1 == "0" { print $3 }' /proc/self/cgroup)

{
    echo "# uname"; uname -a
    echo "# cmdline"; cat /proc/cmdline
    echo "# machine type"
    curl -fs -m 2 -H "Metadata-Flavor: Google" \
        http://metadata.google.internal/computeMetadata/v1/instance/machine-type || echo "not on GCE"
    echo
    echo "# lscpu"; lscpu
    echo "# free"; free -m
    echo "# lru_gen"; cat /sys/kernel/mm/lru_gen/enabled 2>/dev/null || echo "not available"
    echo "# transparent hugepages"
    cat /sys/kernel/mm/transparent_hugepage/enabled /sys/kernel/mm/transparent_hugepage/defrag
    echo "# cgroup"; echo "$cgroup"
    cat "$cgroup/cpu.max" "$cgroup/cpuset.cpus.effective" "$cgroup/memory.max" 2>/dev/null || true
    echo "# java"; "$jdk_dir/bin/java" -version 2>&1
    echo "# jmh arguments"; printf '%s\n' "$@"
} > metrics/system.txt

snapshot() {
    [[ -r $2 ]] && { echo "# $now"; cat "$2"; } >> "metrics/$1.log" 2>/dev/null || true
}

sample() {
    local tick=0 now file
    while true; do
        now=$(date +%s.%N)
        for file in stat vmstat meminfo net/dev pressure/cpu pressure/memory pressure/io; do
            snapshot "${file//\//-}" "/proc/$file"
        done
        for file in memory.current memory.stat memory.events memory.pressure cpu.stat io.stat; do
            snapshot "cgroup-$file" "$cgroup/$file"
        done
        if (( tick % 10 == 0 )) && sudo -n test -r /sys/kernel/debug/lru_gen 2>/dev/null; then
            { echo "# $now"; sudo -n cat /sys/kernel/debug/lru_gen; } >> metrics/debug-lru_gen.log
        fi
        tick=$((tick + 1))
        sleep 1
    done
}
sample &
sampler_pid=$!

jmh_args=("${@//@RUN_DIR@/$run_dir}")
jmh_args=("${jmh_args[@]//@PROFILER_LIB@/$run_dir/lib/$platform/libasyncProfiler.so}")

"$jdk_dir/bin/java" -cp lib/bc-fips.jar:lib/jmh.jar org.openjdk.jmh.Main "${jmh_args[@]}" > jmh.log 2>&1
