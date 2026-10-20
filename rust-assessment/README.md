# Rust alternative to Valkey: engineering assessment and reassessment

This directory holds a performance-engineering assessment of whether Rust could improve on, complement or replace
parts of Valkey. It is self-contained and changes nothing in the Valkey source tree.

It has two parts:

* **Part I** evaluated one candidate, a Rust multiplexing proxy, and reached NO-GO.
* **Part II**, the reassessment, audited Part I and evaluated three candidates separately. It built and measured a
  Rust-native, multi-threaded, Valkey-compatible server (`rvk-server`). It placed the proxy on the new host. It
  measured established Rust client functionality (redis-rs).

## Decision

| Candidate | Decision | One-line reason |
|---|---|---|
| A. Rust-native server (`rvk-server`) | **NO-GO** | Per core it matched Valkey at best on the pre-registered points. On the same 2 CPUs it roughly matched two Valkey shards and beat them by the pre-registered 20% at none of the 5 Gate 3 points (outside them: 1.29× at 10 connections, 1.27× on `SET EX 5`) |
| B. Production proxy | **NO-GO** | On the measured host its CPU floor is above sharding's by construction. Measured today: 0.56× two shards' throughput at 1.60× their CPU per operation |
| C. Rust optimization layer | **Adopt client multiplexing through a maintained client (no new code). Reject the experimental client-side cache** | Multiplexing gave 1.70× throughput and −77% server CPU per operation. The cache served stale data on 34% of reads |

The NO-GO covers the candidates as built, on one 4-vCPU host with synthetic workloads. It does not show that no
Rust server could outperform Valkey. The decision record, the answers to the directive's nine questions, cost
estimates and the evidence that would change the decision are in
[ARCHITECTURE_DECISION.md](ARCHITECTURE_DECISION.md) (§II.1 decision, §II.10 the nine answers).

## Reading order

| # | Document | What it holds |
|---|---|---|
| 1 | [ARCHITECTURE_DECISION.md](ARCHITECTURE_DECISION.md) | Part II decision per candidate, Gates 1–4, costs, next steps, the nine answers. Part I's decision record follows it unchanged |
| 2 | [REASSESSMENT.md](REASSESSMENT.md) | Audit of Part I: what the proxy did and did not test, defects versus architectural constraints, issues in Part I's evidence, defects found in Part II, the multi-agent review |
| 3 | [ARCHITECTURE_COMPARISON.md](ARCHITECTURE_COMPARISON.md) | The three candidates: why each takes its shape, what was built or reused, results by candidate |
| 4 | [FEATURE_MATRIX.md](FEATURE_MATRIX.md) | `rvk-server`'s declared scope and compatibility status per feature, with the evidence for each, and Gate 2 status |
| 5 | [BENCHMARK_PLAN.md](BENCHMARK_PLAN.md) | Part I methodology; Part II configurations and decision thresholds, pre-registered before measurement |
| 6 | [BENCHMARK_RESULTS.md](BENCHMARK_RESULTS.md) | Part I results, then Part II (round 1 pre-registered, round 2 post hoc), with validity checks and limitations |
| 7 | [THREAT_MODEL.md](THREAT_MODEL.md) | Part I: the proxy. Part II: `rvk-server` (STRIDE, denial of service, what Rust did and did not prevent, supply chain) and the client-side options |
| 8 | [ASSESSMENT.md](ASSESSMENT.md) | Part I's analysis of Valkey's architecture and bottlenecks |

## Disclosures

* **Who did the work.** A single agent produced the audit, `rvk-server` and its tests, because the multi-agent
  workflow could not launch while an organization spend limit applied. After the limit reset, a five-dimension
  multi-agent review with adversarial verifiers reviewed `rvk-server` (REASSESSMENT.md §2, §7).
* **The host changed between Part I and Part II** (Xeon 2.8 GHz model 85 to Xeon 2.1 GHz model 207). Part I and
  Part II numbers must not be compared with each other. Within Part II, all comparisons use baselines re-run in
  the same round.
* **Round 2 is post hoc.** It re-measured the Gate 3 points after the review fixes and two profile-guided
  changes, which were chosen after seeing round 1. It cannot change the pre-registered round 1 decision.
* **Suites and methods added after pre-registration** are labeled as such: `p2-overload`, `p2-aof`, the round 2
  suites, and the switch to precise open-loop pacing (BENCHMARK_PLAN.md §II.4).
* **No raw data was edited.** Part I's `results/raw/` is unchanged. Round 1 artifacts that round 2 replaced are
  kept under `*_round1*` names.
* **Corrections after review.** A multi-agent fact-check of these documents against the raw data confirmed 51
  reports of errors in the text (fewer distinct errors, because the checkers overlapped). Examples: review-finding
  counts, the size of baseline drift between rounds, and ranges stated without their scope. All were corrected;
  no measurement changed.

## Directory map

| Path | Contents |
|---|---|
| [`poc/rvk-server/`](poc/rvk-server/) | **Candidate A.** About 4,100 lines of Rust: `server.rs` (event loop per thread), `resp.rs` (protocol), `store.rs` (lock-sharded keyspace, expiry, eviction), `cmd.rs` (61 commands, transactions), `main.rs` (options) |
| [`poc/rsclient/`](poc/rsclient/) | **Candidate C** driver: redis-rs 1.7.1 with one socket per client, a multiplexed socket, or the experimental client-side cache, plus a stale-read check |
| [`poc/tests/`](poc/tests/) | `differential.py` (replies compared with Valkey), `concurrency.py` (atomicity and isolation invariants), `regressions.py` (22 scenarios from the review), `valkey_suite.sh` (Valkey's own Tcl tests) |
| [`bench/harness.py`](bench/harness.py) | Starts each topology on fixed CPUs, drives load, records client results with per-process CPU, memory and `INFO` deltas. Suites for both parts (`./harness.py list`) |
| `bench/run_all.sh` | Part I, all measurements |
| `bench/run_part2.sh`, `bench/run_part2_post.sh`, `bench/run_part2_r2.sh` | Part II round 1, the characterization suites and profiles that follow it, and round 2 |
| `bench/recovery.py`, `bench/profile.sh`, `bench/profile_part2.sh`, `bench/conformance.sh` | Memory, startup and failover; `perf` profiles; Valkey's tests through the proxy (Part I) |
| `bench/summarize.py`, `bench/summarize_part2.py` | Generate the tables in `results/` from the raw data |
| `bench/rust/` | `loadgen` (load generator with open and closed loop, precise pacing, intended-time latency), `proxy-poc` (Candidate B, benchmark-only), `mapmem`, `resp-common` |
| `results/raw/`, `results/summary.md`, `results/key_tables.md` | Part I raw data (one JSON record per run) and generated tables |
| `results/raw2/`, `results/summary_part2.md`, `results/key_tables_part2.md` | Part II raw data, generated tables and Gate 3 evaluated by code. `raw2/p2-smoke.jsonl` and `raw2/dev/` hold smoke and calibration runs, which are not formal results. `raw2/run_part2*.txt` are the run logs |
| `results/environment*.json` | Host, kernel, CPU and binary hashes for Part I, Part II round 1 and round 2 |
| `results/profile/`, `results/profile2/`, `results/profile2_r2/` | `perf` profiles for Part I, Part II round 1 and round 2 |
| `results/poc/` | Correctness evidence: differential, concurrency, regressions, Tcl suite, eviction stress, review findings, and their round 1 versions |

## Reproduction

Requirements: Linux, 4 or more CPUs, `taskset`, Python 3.8+ (standard library only), the Rust toolchain pinned
in `rust-toolchain.toml` (1.97.0), `perf` for profiles, and `tclsh` 8.5+ for the Tcl suites. The harness pins
the data service to CPUs 0–1 and the load generator to CPUs 2–3; edit `SERVER_CPUS` and `CLIENT_CPUS` in
`bench/harness.py` on other hosts. Results are appended, so delete `results/raw/` or `results/raw2/` for a clean
rerun.

**Build** (from the repository root):

```sh
make -j"$(nproc)"                                          # valkey-server, valkey-cli, valkey-benchmark
(cd rust-assessment/bench/rust && cargo build --release)   # loadgen, proxy-poc, mapmem
(cd rust-assessment/poc && cargo build --release)          # rvk-server, rsclient
```

**Unit tests:**

```sh
(cd rust-assessment/poc && cargo test --release)                         # rvk-server, including a property test of resumable parsing
(cd rust-assessment/bench/rust && cargo test --release -p resp-common)   # RESP framing used by the tools
```

**Correctness tests for `rvk-server`** (from the repository root; the ports are the ones used for the recorded
results):

```sh
V=src; R=rust-assessment/poc/target/release/rvk-server; T=rust-assessment/poc/tests

# Differential test: start both servers, then compare replies
$V/valkey-server --port 6490 --save "" --appendonly no --daemonize yes --logfile /tmp/vdiff.log
$R --port 6491 --threads 2 & sleep 0.5
python3 $T/differential.py 6490 6491 --seed 7 --fuzz 50000 --out rust-assessment/results/poc/differential.json
$V/valkey-cli -p 6490 shutdown nosave; $V/valkey-cli -p 6491 shutdown

# Concurrency invariants: run against each server in turn (shown for rvk with 4 threads under maxmemory)
$R --port 6471 --threads 4 --maxmemory 64mb --maxmemory-policy allkeys-lru & sleep 0.5
python3 $T/concurrency.py 6471 --label rvk-4t-maxmem --out rust-assessment/results/poc/concurrency.jsonl
$V/valkey-cli -p 6471 shutdown

# Regression scenarios (start and stop their own servers)
python3 $T/regressions.py --out rust-assessment/results/poc/regressions.json

# Valkey's own Tcl tests against Valkey and rvk-server
$T/valkey_suite.sh

# Eviction stress: 5M SETs into rvk-2t under maxmemory, then check error replies
$R --port 6470 --threads 2 --maxmemory 256mb --maxmemory-policy allkeys-lru & sleep 0.5
$V/valkey-benchmark -p 6470 -t set -P 64 -c 8 --threads 2 -r 5000000 -n 5000000 -d 100 -q
$V/valkey-cli -p 6470 info stats | grep -E 'evicted_keys|total_error_replies'
$V/valkey-cli -p 6470 shutdown
```

The recorded concurrency results ran six configurations: Valkey, Valkey with `--maxmemory 64mb --maxmemory-policy
allkeys-lru`, and `rvk-server` with 2 and 4 threads, each with and without the same memory limit.

**Benchmarks** (from `rust-assessment/bench`; durations are for a 4-vCPU host):

```sh
./run_all.sh            # Part I: about 2 hours; writes results/raw and the Part I tables
./run_part2.sh          # Part II round 1, pre-registered matrix: about 2.5 hours (see the note below)
./run_part2_post.sh     # overload and AOF suites, profiles, memory, startup, failover: about 25 minutes
./run_part2_r2.sh       # Part II round 2, post hoc: about 85 minutes
./summarize_part2.py    # regenerate results/key_tables_part2.md and summary_part2.md
```

Single suites: `./harness.py list`, then for example `./harness.py run p2-core`. A smoke test first is
recommended: `./harness.py run p2-smoke`. An interrupted run can continue with `./harness.py run --resume
SUITE`, which skips runs already recorded.

**Which binaries.** Building at HEAD gives the round 2 `rvk-server` and `rsclient`, which `run_part2_r2.sh` uses.
To reproduce round 1, build `rust-assessment/poc` from commit `92febac` first (the `git_head` recorded in
`results/environment_part2.json`; `poc/` is unchanged from there through `4cda2f7`).

**Check the environment.** `./harness.py env --part2` and the run scripts overwrite
`results/environment_part2*.json`. Copy those files aside before rerunning (or compare against `git show
HEAD:rust-assessment/results/environment_part2.json` and `..._r2.json`), and compare host and binary hashes before
reading any comparison as a reproduction.

## How to read the results

* Throughput is the median of 3 trials in fresh processes. Server CPU µs/op covers every data-service process.
* A ⚠ marks a point where the load generator exceeded 170% CPU, so that throughput is a lower bound. Gate 3
  decides such points on pipelined runs, as pre-registered.
* Open-loop latency is reported from the actual send time and from the intended send time; the latter includes
  load-generator lag.
* Absolute numbers depend on the host. The comparisons hold the CPU budget, workload and host constant within a
  round.

The Rust code in `bench/rust/` and `poc/` consists of measurement instruments and a proof of concept. None of it
is production software. `proxy-poc` and `rvk-server` refuse many Valkey commands on purpose (THREAT_MODEL.md §3,
FEATURE_MATRIX.md §8).
