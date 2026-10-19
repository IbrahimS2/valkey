#!/bin/sh
# Part II CPU profiles (REASSESSMENT.md, BENCHMARK_RESULTS.md Part II).
#
# GET-only load (50 connections, 1M keys of 100 B) against valkey-1t, rvk-1t
# and rvk-2t, unpipelined and with pipeline depth 16: perf samples
# (software cpu-clock, call graphs) of the server process, split by DSO
# (kernel vs user space) and top symbols. Output goes to
# ../results/profile2/. Needs perf; set PERF=/path/to/perf if it is not on PATH.
set -eu
HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/../.." && pwd)
OUT="$HERE/../results/profile2"
PERF=${PERF:-perf}
BIN="$HERE/rust/target/release"
RVK="$HERE/../poc/target/release/rvk-server"
CLI="$REPO/src/valkey-cli"
PORT=6460
WORK=$(mktemp -d)
mkdir -p "$OUT"

start() { # $1 = name; sets SPID
    case $1 in
    valkey-1t)
        taskset -c 0 "$REPO/src/valkey-server" --port $PORT --save '' --appendonly no --dir "$WORK" \
            --logfile "$WORK/valkey.log" --daemonize no > /dev/null 2>&1 &
        SPID=$! ;;
    rvk-1t)
        taskset -c 0 "$RVK" --port $PORT --threads 1 > /dev/null 2>&1 &
        SPID=$! ;;
    rvk-2t)
        taskset -c 0,1 "$RVK" --port $PORT --threads 2 > /dev/null 2>&1 &
        SPID=$! ;;
    esac
    until "$CLI" -p $PORT ping >/dev/null 2>&1; do sleep 0.05; done
    "$BIN/loadgen" --preload --port $PORT --conns 8 --threads 2 --keyspace 1000000 --value-size 100 \
        --pipeline 64 >/dev/null
    "$CLI" -p $PORT config resetstat >/dev/null
}

report() { # $1 = perf data, $2 = name
    "$PERF" report -i "$1" --no-children --sort dso --stdio 2>/dev/null | grep -E '^ +[0-9]' > "$OUT/$2.dso.txt"
    "$PERF" report -i "$1" --no-children --sort dso,sym --stdio 2>/dev/null | grep -E '^ +[0-9]' \
        | head -40 > "$OUT/$2.top-symbols.txt"
}

for name in valkey-1t rvk-1t rvk-2t; do
    for pl in 1 16; do
        start $name
        taskset -c 2,3 "$BIN/loadgen" --port $PORT --conns 50 --threads 2 --duration 14 --warmup 2 \
            --keyspace 1000000 --get-ratio 1.0 --value-size 100 --pipeline $pl > "$OUT/$name-p$pl.loadgen.json" &
        LPID=$!
        sleep 5
        "$PERF" record -F 2999 -g -e cpu-clock -p "$SPID" -o "$WORK/$name-p$pl.perf" -- sleep 8 2>/dev/null
        wait $LPID
        report "$WORK/$name-p$pl.perf" "$name-p$pl"
        "$CLI" -p $PORT info stats | grep -E 'total_commands_processed|total_net_' > "$OUT/$name-p$pl.stats.txt"
        "$CLI" -p $PORT shutdown nosave >/dev/null 2>&1 || true
        wait $SPID 2>/dev/null || true
    done
done
rm -rf "$WORK"
echo "profiles written to $OUT"
