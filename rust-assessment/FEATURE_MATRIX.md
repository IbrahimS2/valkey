# Feature matrix: rvk-server (Candidate A) against Valkey

`rvk-server` ([`poc/rvk-server`](poc/rvk-server)) is the Rust-native, Valkey-compatible server built for the
reassessment. Its declared scope is a **string cache**: the commands a typical caching client uses, with Valkey's
replies, errors, expiry, eviction and transaction semantics for those commands. Everything else is declared out
of scope here rather than discovered later.

Statuses:

| Status | Meaning |
|---|---|
| **verified** | implemented, and checked against Valkey by at least one of: the differential test, the concurrency invariants, or Valkey's own Tcl tests |
| **implemented** | present and exercised by the benchmarks, but without a direct comparison against Valkey |
| **partial** | present with a documented difference from Valkey |
| **unsupported** | in Valkey and reasonably expected of a cache server, but absent; the server replies with Valkey's unknown-command error |
| **out of scope** | deliberately not built in this proof of concept; a production server would need it (see the cost estimate in ARCHITECTURE_DECISION.md) |

Evidence sources:

| Source | What it does | Result |
|---|---|---|
| Differential test, [`poc/tests/differential.py`](poc/tests/differential.py) | Sends identical command streams to Valkey and rvk-server and compares replies exactly, error texts included. It covers 316 hand-written cases (strings, integers, keyspace, transactions, connection, RESP3, two-client WATCH, raw protocol) and 50,000 random commands (seed 7) | 50,358 cases, **1 mismatch** (`INCRBYFLOAT` formatting, below). [`results/poc/differential.json`](results/poc/differential.json) |
| Concurrency invariants, [`poc/tests/concurrency.py`](poc/tests/concurrency.py) | Atomic INCR, atomic MSET across shards, MULTI/EXEC isolation, WATCH compare-and-set, 1 ms TTL races, memory bound under `maxmemory` with 1000- and 100-byte values | **All pass** on Valkey (control), rvk with 2 and 4 threads, and both with `maxmemory 64mb allkeys-lru`. [`results/poc/concurrency.jsonl`](results/poc/concurrency.jsonl) |
| Valkey's Tcl suite, [`poc/tests/valkey_suite.sh`](poc/tests/valkey_suite.sh) | Runs 8 of Valkey's test files against an external server | Valkey 444 passed. rvk **68 passed, 3 failed** (all 3 use `SADD`/`SRANDMEMBER`, which are out of scope). 6 files stopped at their first out-of-scope command, so most of their tests never ran. [`results/poc/valkey_suite/`](results/poc/valkey_suite/) |
| Benchmarks (BENCHMARK_RESULTS.md Part II) | Every run checks for error replies, timeouts and reply misattribution | See Gate 2 in BENCHMARK_RESULTS.md |

The Tcl suite is weak evidence for rvk-server. Valkey's test files mix data types, so most files abort early on a
list or set command and the in-scope tests after that point never run. The differential test is the main evidence
for in-scope commands. That leaves a real limitation: it only checks cases someone thought to write, plus random
commands from a fixed grammar.

## 1. Protocol and connections

| Feature | Status | Notes |
|---|---|---|
| RESP2 multibulk requests, pipelining | **verified** | Strict CRLF checks, `string2ll` length parsing ("-0", leading zeros and 21+ digit lengths rejected), Valkey's error texts. A property test splits every request at every byte boundary |
| Inline requests | **verified** | `sdsparsearg` tokenizer semantics: quoting, `\xHH` escapes, concatenated segments, "unbalanced quotes", embedded NUL rejected |
| Unauthenticated request limits | **verified** | Multibulk > 10 or bulk > 16 KiB before AUTH is refused, as in Valkey |
| `proto-max-bulk-len`, `client-query-buffer-limit` | **implemented** | Same defaults as Valkey |
| RESP3 (`HELLO 3`): maps, nulls, doubles, verbatim strings | **verified** (17 cases) | `HELLO` reply has the same shape. `server`/`version` fields identify rvk |
| Output backpressure | **partial** | A client whose pending output exceeds 64 MB stops being read until it drains. Valkey's `client-output-buffer-limit` (disconnect) is not implemented |
| `maxclients` | **implemented** | Rejected with Valkey's error text |
| TLS, Unix sockets, `bind` to multiple addresses | **out of scope** | |

## 2. String commands (20 of Valkey's 26)

| Command(s) | Status | Notes |
|---|---|---|
| `GET`, `SET` with `NX`/`XX`/`GET`/`EX`/`PX`/`EXAT`/`PXAT`/`KEEPTTL`/`IFEQ`/`IFNE`, `SETNX`, `SETEX`, `PSETEX`, `GETSET`, `GETEX`, `GETDEL` | **verified** | Option conflicts and error texts match |
| `MGET`, `MSET`, `MSETNX` | **verified** | Atomic across shards (concurrency invariant `mset_atomic`) |
| `APPEND`, `STRLEN`, `GETRANGE`, `SETRANGE` | **verified** | `SETRANGE` honors `proto-max-bulk-len` |
| `INCR`, `DECR`, `INCRBY`, `DECRBY` | **verified** | Overflow and "not an integer" errors match |
| `INCRBYFLOAT` | **partial** | Result formatting: rvk uses the shortest round-trip `f64`, Valkey uses `long double` with 17 significant digits. Example: Valkey `1005.59999999999999998`, rvk `1005.6`. This is the one differential mismatch |
| `LCS`, `SUBSTR`, `INCREX`, `DELEX`, `DELIFEQ`, `MSETEX` | **unsupported** | `INCREX`, `DELEX`, `DELIFEQ` and `MSETEX` are recent Valkey additions |
| Integer and embedded-string encodings, `OBJECT ENCODING` | **out of scope** | Values are stored as bytes; integers are parsed on use |

## 3. Keyspace (17 of Valkey's 29 generic commands)

| Command(s) | Status | Notes |
|---|---|---|
| `DEL`, `UNLINK`, `EXISTS`, `TYPE`, `RENAME`, `RENAMENX`, `KEYS`, `RANDOMKEY`, `DBSIZE` | **verified** | `UNLINK` frees synchronously |
| `FLUSHALL`, `FLUSHDB` | **verified** | `ASYNC` accepted and runs synchronously |
| `SCAN` | **unsupported** | Operators rely on it; a production server needs it |
| `COPY`, `TOUCH`, `OBJECT`, `SORT`, `SORT_RO`, `MOVE` | **unsupported** | |
| `DUMP`, `RESTORE`, `MIGRATE`, `WAIT`, `WAITAOF` | **out of scope** | Need RDB encoding, replication or persistence |
| `SELECT` | **partial** | Database 0 only; other indexes return Valkey's "DB index is out of range" error |

## 4. Expiry

| Feature | Status | Notes |
|---|---|---|
| `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT` with `NX`/`XX`/`GT`/`LT` | **verified** | Past times delete the key, as in Valkey |
| `TTL`, `PTTL`, `EXPIRETIME`, `PEXPIRETIME`, `PERSIST` | **verified** | `TTL` rounds as Valkey does |
| Lazy expiry on access | **verified** | An expired key is never returned (concurrency invariant `expiry_race`) |
| Active expiry | **partial** | Different algorithm with the same guarantee: a per-shard min-heap of deadlines processed every 100 ms with a 25% time budget, instead of Valkey's random sampling. Expired keys are reclaimed in deadline order, so reclamation timing differs from Valkey's. `DEBUG SET-ACTIVE-EXPIRE` is supported |
| Keyspace notifications for expiry | **out of scope** | |

## 5. Memory limit and eviction

| Feature | Status | Notes |
|---|---|---|
| `maxmemory`, `allkeys-lru` | **partial** | Approximated LRU with 5 samples and a 16-entry candidate pool, like `evict.c`, but the pool is per thread. Memory is jemalloc's `stats.allocated`, refreshed every 100 ms and adjusted between refreshes by the store's own estimate. Peak overshoot measured at 0.2–0.3% of a 64 MB limit (`memory_bound`; Valkey: 0.02%). rvk holds fewer keys in the same limit (§ memory per key in BENCHMARK_RESULTS.md Part II) |
| `allkeys-random`, `noeviction` | **implemented** | |
| `volatile-*` policies, LFU, `maxmemory-clients`, `lazyfree-*` | **unsupported** | |
| OOM error under `noeviction` | **verified** | Same text as Valkey |

## 6. Transactions

| Feature | Status | Notes |
|---|---|---|
| `MULTI`, `EXEC`, `DISCARD` | **verified** | Queuing, `EXECABORT` on queue-time errors, run-time errors inside `EXEC`, nesting errors. Isolation across threads holds (`exec_isolation`) |
| `WATCH`, `UNWATCH` | **verified** | 26 two-client cases. Compare-and-set loses no updates at 2 and 4 threads (`watch_cas`). Expiry, eviction and flushes invalidate watchers |
| Commands Valkey refuses inside `MULTI` | **verified** | Same `NO_MULTI` errors |

## 7. Connection and server commands

| Command(s) | Status | Notes |
|---|---|---|
| `PING`, `ECHO`, `QUIT`, `RESET`, `HELLO` (with `AUTH`/`SETNAME`), `AUTH` | **verified** | `AUTH` supports `requirepass` (the `default` user) only |
| `CLIENT ID`/`GETNAME`/`SETNAME`/`SETINFO`/`INFO`/`LIST` | **partial** | No `KILL`, `PAUSE`, `NO-EVICT`, `TRACKING`, `CACHING`, `REPLY`, `UNBLOCK`. `LIST`/`INFO` fields are a subset |
| `INFO` | **partial** | Server, clients, memory, stats and keyspace fields used by the harness. No `redis_version`/`valkey_version` field, which stops Valkey's `unit/info-command` test at its first line; no `commandstats`, `errorstats`, `latencystats` |
| `CONFIG GET`/`SET` | **partial** | `GET` matches patterns over a fixed list of values. `SET` supports `maxmemory` and `maxmemory-policy`, and accepts `save ""`/`appendonly no`; anything else is refused. `CONFIG RESETSTAT` |
| `DBSIZE`, `TIME`, `SHUTDOWN` | **implemented** | |
| `COMMAND` | **partial** | `COUNT` is correct; bare `COMMAND`, `INFO` and `DOCS` return empty results, which is enough for the clients tested (redis-rs, valkey-benchmark) |
| `DEBUG` | **partial** | `SET-ACTIVE-EXPIRE` and `SLEEP` only |
| `FUNCTION FLUSH`, `SCRIPT FLUSH` | **partial** | Accepted as no-ops (there are no scripts) so test harnesses can reset state |

## 8. Out of scope (absent by design in this proof of concept)

Each of these exists in Valkey and would be needed by many production deployments. A production decision has to
cost them (ARCHITECTURE_DECISION.md §7).

| Area | What is missing |
|---|---|
| Durability | RDB snapshots, AOF, `SAVE`/`BGSAVE`, restart with data. **rvk-server always restarts empty** |
| High availability | Replication, `PSYNC`, Sentinel, failover |
| Scale-out | Cluster mode, slots, `MOVED`/`ASK`, cluster bus |
| Security | ACL users and permissions, TLS, protected mode, `rename-command` |
| Data types | Hashes, lists, sets, sorted sets, streams, geo, HyperLogLog, bitmaps, JSON/Bloom modules |
| Messaging and scripting | Pub/Sub, sharded Pub/Sub, Lua, Functions, blocking commands |
| Client features | `CLIENT TRACKING` (server-assisted client-side caching), `MONITOR`, `CLIENT PAUSE`, `CLIENT KILL` |
| Observability | `SLOWLOG`/`COMMANDLOG`, `LATENCY`, `MEMORY USAGE`/`DOCTOR`, `HOTKEYS`, keyspace notifications, `commandstats` |
| Operations | Multiple databases, `CONFIG REWRITE`, config file, modules |

## 9. Valkey Tcl suite details

| Test file | Valkey | rvk passed | rvk failed | Where rvk stopped |
|---|---|---|---|---|
| `unit/type/string` | 125 | 6 | 0 | `MEMORY USAGE` |
| `unit/type/incr` | 82 | 11 | 0 | `RPUSH` |
| `unit/expire` | 79 | 3 | 0 | `LPUSH` |
| `unit/keyspace` | 48 | 27 | 0 | `COPY` |
| `unit/multi` | 68 | 0 | 0 | `RPUSH` (first test) |
| `unit/protocol` | 37 | 21 | 3 (`SRANDMEMBER`, `SADD`) | `DEBUG PROTOCOL` |
| `unit/auth` | 0 | 0 | 0 | every test is tagged for a self-started server |
| `unit/info-command` | 5 | 0 | 0 | no `redis_version` in `INFO` |
| **Total** | **444** | **68** | **3** | |

No test that ran against an in-scope command failed.

## 10. Gate 2 (functional validity) status

Pre-registered in BENCHMARK_PLAN.md Part II §II.3:

| Condition | Result |
|---|---|
| Differential mismatches only in features declared partial | **Met**: the only mismatch is `INCRBYFLOAT` formatting (partial, §2) |
| Concurrency invariants pass at 2 and 4 threads, including under `maxmemory` | **Met** after two defects were fixed (REASSESSMENT.md §6) |
| Zero errors, timeouts and reply misattributions in every benchmark run | See BENCHMARK_RESULTS.md Part II §2 |
