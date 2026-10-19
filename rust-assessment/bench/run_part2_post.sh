#!/bin/sh
# Part II measurements that follow run_part2.sh: characterization suites added
# after pre-registration (overload, Valkey AOF), profiles, memory per key,
# startup and failover. About 25 minutes. Then regenerates the Part II tables.
set -e
cd "$(dirname "$0")"
./harness.py run p2-overload p2-aof
PERF=${PERF:-perf} ./profile_part2.sh
./recovery.py p2-memory p2-startup p2-failover
./summarize_part2.py
