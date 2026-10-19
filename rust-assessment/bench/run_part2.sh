#!/bin/sh
# Part II (reassessment) measurements in the order used for BENCHMARK_RESULTS.md.
# Expect about 2.5 hours on a 4-vCPU host. Raw results append to
# ../results/raw2 (Part I's ../results/raw is not touched); delete raw2 first
# for a clean run. Build ../poc first: (cd ../poc && cargo build --release).
set -e
cd "$(dirname "$0")"
./harness.py env --part2 > /dev/null
./harness.py run p2-crosscheck p2-core p2-core-p16 p2-proxy p2-conns p2-openloop p2-large p2-ttl p2-cold \
    p2-evict p2-churn p2-clients
