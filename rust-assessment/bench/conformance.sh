#!/bin/sh
# Runs a subset of Valkey's own Tcl test suite in external-server mode, once
# directly against Valkey and once through proxy-poc, and records pass, fail
# and skip counts. This is the compatibility check behind the "no compatibility
# claim" statement in THREAT_MODEL.md.
#
# usage: bench/conformance.sh [test ...]   (run from anywhere; needs a built tree)
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/../.." && pwd)
OUT="$HERE/../results/conformance"
VPORT=6450
PPORT=7000
TESTS=${*:-"unit/type/string unit/type/incr unit/type/hash unit/type/list unit/expire unit/multi unit/pubsub unit/protocol"}
mkdir -p "$OUT"
WORK=$(mktemp -d)

start_valkey() {
    taskset -c 0 "$REPO/src/valkey-server" --port $VPORT --save '' --appendonly no --dir "$WORK" \
        --logfile "$WORK/valkey.log" --enable-debug-command yes --daemonize yes
    until "$REPO/src/valkey-cli" -p $VPORT ping >/dev/null 2>&1; do sleep 0.05; done
}

stop_valkey() {
    "$REPO/src/valkey-cli" -p $VPORT shutdown nosave >/dev/null 2>&1
    sleep 0.3
}

run_tests() { # $1 = label, $2 = port
    for t in $TESTS; do
        f="$OUT/$1-$(echo "$t" | tr / _).log"
        (cd "$REPO" && timeout 600 ./runtest --host 127.0.0.1 --port "$2" --singledb --single "$t" \
            --timeout 60 > "$f" 2>&1)
        ok=$(grep -c '\[ok\]' "$f")
        err=$(grep -c '\[err\]' "$f")
        exc=$(grep -c '\[exception\]' "$f")
        skip=$(grep -c '\[skip\]' "$f")
        printf '{"path":"%s","test":"%s","ok":%s,"err":%s,"exception":%s,"skip":%s}\n' \
            "$1" "$t" "$ok" "$err" "$exc" "$skip" | tee -a "$OUT/summary.jsonl"
    done
}

start_valkey
run_tests direct $VPORT
stop_valkey

start_valkey
taskset -c 0,1 "$HERE/rust/target/release/proxy-poc" --listen 127.0.0.1:$PPORT \
    --upstream 127.0.0.1:$VPORT --threads 2 --upstream-conns 1 2> "$WORK/proxy.log" &
PROXY_PID=$!
until "$REPO/src/valkey-cli" -p $PPORT ping >/dev/null 2>&1; do sleep 0.05; done
run_tests proxy $PPORT
kill -INT $PROXY_PID
wait $PROXY_PID 2>/dev/null
stop_valkey
rm -rf "$WORK"
