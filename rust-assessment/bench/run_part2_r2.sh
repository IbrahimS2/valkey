#!/bin/sh
# Part II round 2 (post hoc): the Gate 3 points, open-loop latency and
# eviction re-measured after the review fixes and the profile-guided changes
# to rvk-server, with every baseline re-run in the same window. Profiles go
# to ../results/profile2_r2 and rvk memory records are labelled rvk-r2, so
# round 1 results are kept. About 85 minutes.
set -e
cd "$(dirname "$0")"
./harness.py env --part2 --tag r2 > /dev/null
./harness.py run --resume p2-r2-core p2-r2-core-p16 p2-r2-conns p2-r2-openloop p2-r2-evict
PROFILE_OUT="$(pwd)/../results/profile2_r2" PERF=${PERF:-perf} ./profile_part2.sh
P2_RVK_LABEL=rvk-r2 ./recovery.py p2-memory
./summarize_part2.py
