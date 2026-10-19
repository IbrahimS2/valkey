#!/bin/sh
# Runs files from Valkey's own Tcl test suite in external-server mode against
# Valkey and against rvk-server, and records pass/fail/abort counts per file.
# A file aborts at its first [exception] (typically an unsupported command),
# so later tests in that file never run; the first blocker is recorded.
#
# usage: poc/tests/valkey_suite.sh [test ...]   (needs a built tree and tclsh)
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/../../.." && pwd)
OUT="$HERE/../../results/poc/valkey_suite"
VPORT=6450
RPORT=6451
TESTS=${*:-"unit/type/string unit/type/incr unit/expire unit/keyspace unit/multi unit/protocol unit/auth unit/info-command"}
RVK="$HERE/../target/release/rvk-server"
mkdir -p "$OUT"
WORK=$(mktemp -d)

summarize_log() { # $1 = target, $2 = test, $3 = log
    ok=$(grep -c '^\[ok\]' "$3")
    err=$(grep -c '^\[err\]' "$3")
    tmo=$(grep -c '^\*\*\* \[TIMEOUT\]' "$3")
    exc=$(grep -c '^\[exception\]' "$3")
    why=$(grep -m1 '^\[exception\]' "$3" | sed 's/^\[exception\]: Executing test client: //; s/"/'"'"'/g' | cut -c1-160)
    printf '{"target":"%s","test":"%s","ok":%s,"err":%s,"timeout":%s,"exception":%s,"first_blocker":"%s"}\n' \
        "$1" "$2" "$ok" "$err" "$tmo" "$exc" "$why"
}

run_tests() { # $1 = target label, $2 = port
    for t in $TESTS; do
        f="$OUT/$1-$(echo "$t" | tr / _).log"
        (cd "$REPO" && timeout 900 ./runtest --host 127.0.0.1 --port "$2" --singledb --single "$t" \
            --timeout 60 > "$f" 2>&1)
        summarize_log "$1" "$t" "$f" | tee -a "$OUT/summary.jsonl"
    done
}

: > "$OUT/summary.jsonl"
"$REPO/src/valkey-server" --port $VPORT --save '' --appendonly no --dir "$WORK" --logfile "$WORK/v.log" \
    --enable-debug-command yes --daemonize yes
until "$REPO/src/valkey-cli" -p $VPORT ping >/dev/null 2>&1; do sleep 0.05; done
run_tests valkey $VPORT
"$REPO/src/valkey-cli" -p $VPORT shutdown nosave >/dev/null 2>&1

"$RVK" --port $RPORT --threads 2 > "$WORK/rvk.log" 2>&1 &
RPID=$!
until "$REPO/src/valkey-cli" -p $RPORT ping >/dev/null 2>&1; do sleep 0.05; done
run_tests rvk-2t $RPORT
kill $RPID 2>/dev/null
wait $RPID 2>/dev/null
rm -rf "$WORK"
