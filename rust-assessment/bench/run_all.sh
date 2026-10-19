#!/bin/sh
# Runs every measurement in the order used for BENCHMARK_RESULTS.md, then
# regenerates ../results/summary.md. Expect about 2 hours on a 4-vCPU host.
# Results append to ../results/raw; delete it first for a clean run.
set -e
cd "$(dirname "$0")"
./harness.py env > /dev/null
./harness.py crosscheck
./harness.py run core conns openloop large ttl pipeline cold aof evict churn
./recovery.py
PERF=${PERF:-perf} ./profile.sh
./conformance.sh
./summarize.py
