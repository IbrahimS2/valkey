#!/bin/sh
# CPU profiles behind the bottleneck analysis (ASSESSMENT.md §4.1).
#
# For valkey-1t, and for Valkey plus proxy-poc, under get100 load with 50
# connections: perf samples (software cpu-clock, call graphs), the share of
# kernel vs user samples, the top symbols, and Valkey INFO commandstats.
# Output goes to ../results/profile/. Needs perf; set PERF=/path/to/perf if
# it is not on PATH.
set -eu
HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/../.." && pwd)
OUT="$HERE/../results/profile"
PERF=${PERF:-perf}
BIN="$HERE/rust/target/release"
CLI="$REPO/src/valkey-cli"
PORT=6460
PPORT=7000
WORK=$(mktemp -d)
mkdir -p "$OUT"

start_valkey() {
    taskset -c 0 "$REPO/src/valkey-server" --port $PORT --save '' --appendonly no --dir "$WORK" \
        --logfile "$WORK/valkey.log" --daemonize yes
    until "$CLI" -p $PORT ping >/dev/null 2>&1; do sleep 0.05; done
    "$BIN/loadgen" --preload --port $PORT --conns 8 --threads 2 --keyspace 1000000 --value-size 100 \
        --pipeline 64 >/dev/null
    "$CLI" -p $PORT config resetstat >/dev/null
}

load() { # $1 = port, $2 = seconds
    taskset -c 2,3 "$BIN/loadgen" --port "$1" --conns 50 --threads 2 --duration "$2" --warmup 2 \
        --keyspace 1000000 --get-ratio 1.0 --value-size 100
}

report() { # $1 = perf data, $2 = name
    "$PERF" report -i "$1" --no-children --sort dso --stdio 2>/dev/null | grep -E '^ +[0-9]' > "$OUT/$2.dso.txt"
    "$PERF" report -i "$1" --no-children --sort dso,sym --stdio 2>/dev/null | grep -E '^ +[0-9]' \
        | head -40 > "$OUT/$2.top-symbols.txt"
}

# 1. Valkey alone
start_valkey
VPID=$(pgrep -f "valkey-server.*:$PORT")
load $PORT 14 > "$OUT/valkey-1t.loadgen.json" &
sleep 5
"$PERF" record -F 2999 -g -e cpu-clock -p "$VPID" -o "$WORK/valkey.perf" -- sleep 8 2>/dev/null
wait
report "$WORK/valkey.perf" valkey-1t
"$CLI" -p $PORT info commandstats > "$OUT/valkey-1t.commandstats.txt"
"$CLI" -p $PORT info stats | grep -E 'total_commands_processed|total_net_' >> "$OUT/valkey-1t.commandstats.txt"

# 2. Valkey behind proxy-poc
"$CLI" -p $PORT config resetstat >/dev/null
taskset -c 0,1 "$BIN/proxy-poc" --listen 127.0.0.1:$PPORT --upstream 127.0.0.1:$PORT --threads 2 \
    --upstream-conns 1 2> "$OUT/proxy.stderr.txt" &
PPID_=$!
until "$CLI" -p $PPORT ping >/dev/null 2>&1; do sleep 0.05; done
load $PPORT 14 > "$OUT/rust-proxy.loadgen.json" &
LOAD_PID=$!
sleep 5
"$PERF" record -F 2999 -g -e cpu-clock -p "$PPID_" -o "$WORK/proxy.perf" -- sleep 8 2>/dev/null &
PERF_PID=$!
"$PERF" record -F 2999 -g -e cpu-clock -p "$VPID" -o "$WORK/valkey-behind-proxy.perf" -- sleep 8 2>/dev/null
wait $PERF_PID
wait $LOAD_PID
report "$WORK/proxy.perf" rust-proxy.proxy
report "$WORK/valkey-behind-proxy.perf" rust-proxy.valkey
"$CLI" -p $PORT info commandstats > "$OUT/rust-proxy.valkey.commandstats.txt"
kill -INT $PPID_
wait $PPID_ 2>/dev/null || true
"$CLI" -p $PORT shutdown nosave >/dev/null 2>&1 || true
rm -rf "$WORK"
echo "profiles written to $OUT"
