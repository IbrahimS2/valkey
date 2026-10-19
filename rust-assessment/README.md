# Rust alternative to Valkey: engineering assessment

This directory holds a performance-engineering assessment of whether a Rust component could improve on,
complement, or replace parts of Valkey. It is self-contained and changes nothing in the Valkey source tree.

**Decision: NO-GO** for building or replacing anything in Rust on the evidence available. The full answer to the
six final questions is in [ARCHITECTURE_DECISION.md §1](ARCHITECTURE_DECISION.md#1-decision).

Read in this order:

| # | Deliverable | File |
|---|---|---|
| 1 | Current architecture, bottleneck analysis, opportunity assessment | [ASSESSMENT.md](ASSESSMENT.md) |
| 2 | Workloads, methodology, environment, reproducibility, pre-set acceptance thresholds | [BENCHMARK_PLAN.md](BENCHMARK_PLAN.md) |
| 3 | Benchmark scripts, configuration and tooling | [`bench/`](bench/) |
| 4 | Measurements, statistics, comparisons, limitations | [BENCHMARK_RESULTS.md](BENCHMARK_RESULTS.md); raw data in [`results/`](results/) |
| 5 | Advantages, disadvantages, alternatives, costs, GO/NO-GO | [ARCHITECTURE_DECISION.md](ARCHITECTURE_DECISION.md) |
| 6 | Threat model and correctness assessment | [THREAT_MODEL.md](THREAT_MODEL.md) |
| 7 | Implementation plan | Not produced: implementation was not approved. ARCHITECTURE_DECISION.md §9 lists what would reopen the decision |
| 8 | Production source code, deployment artifacts | Not produced, for the same reason |

## What is in `bench/`

| Path | Purpose |
|---|---|
| `bench/harness.py` | Runs every topology and workload with CPU pinning, collects per-process CPU, memory and INFO deltas, and writes JSON Lines |
| `bench/recovery.py` | Startup, recovery, failover and memory-per-key measurements |
| `bench/conformance.sh` | Runs Valkey's own Tcl tests directly and through the proxy |
| `bench/profile.sh` | `perf` profiles and `INFO commandstats` for Valkey alone and behind the proxy |
| `bench/summarize.py` | Builds `results/summary.md` from the raw data |
| `bench/run_all.sh` | Runs everything in order |
| `bench/rust/loadgen` | Rust load generator: GET/SET ratio, Zipf skew, TTL, pipelining, open/closed loop, exact percentiles |
| `bench/rust/proxy-poc` | **Benchmark-only** multiplexing, auto-pipelining RESP proxy (the Rust candidate). Not for deployment |
| `bench/rust/mapmem` | Lower bound on the per-entry memory of a naive Rust hash map |
| `bench/rust/resp-common` | RESP framing used by the tools, with unit tests |

Results: `results/raw/*.jsonl` holds one record per run; `results/summary.md` and `results/key_tables.md` are
generated from it; `results/profile/`, `results/conformance/` and `results/environment.json` hold profiles,
test-suite logs, and host and binary details.

**Headline (2 vCPUs for the data service, 50 connections):** the tuned Rust proxy reached 0.60–0.67× the
throughput of two plain Valkey shards, at 1.36–1.51× the CPU per operation. Client pipelining gave 5.7×
single-threaded Valkey. Valkey spent 64–71% of its CPU in the kernel, so a faster language is not the lever.

The Rust crates are measurement instruments. They were written to test hypotheses, not as a start on an
implementation. `proxy-poc` refuses many Valkey commands on purpose; see THREAT_MODEL.md §3.
