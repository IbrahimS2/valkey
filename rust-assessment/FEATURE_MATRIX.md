# Feature matrix: rvk-server (Candidate A) against Valkey

`rvk-server` ([`poc/rvk-server`](poc/rvk-server)) is the Rust-native, Valkey-compatible server built for the
reassessment. Its declared scope is a **string cache**: the commands a typical caching client uses, with Valkey's
replies, errors, expiry, eviction and transaction semantics for those commands. Everything else is declared out
of scope here rather than discovered later.

This matrix describes the **round 2 build** (commit `f7c6100`), which is the version measured in
BENCHMARK_RESULTS.md Part II round 2. Round 1 had defects that this matrix's first version did not know about;
§11 lists what the multi-agent review changed.

Statuses:

| Status | Meaning |
|---|---|
| **verified** | implemented, and checked against Valkey by at least one of: the differential test, the regression scenarios, the concurrency invariants, or Valkey's own Tcl tests |
| **implemented** | present and exercised by the benchmarks, but without a direct comparison against Valkey |
| **partial** | present with a documented difference from Valkey |
| **unsupported** | in Valkey and reasonably expected of a cache server, but absent; the server replies with Valkey's unknown-command error |
| **out of scope** | deliberately not built in this proof of concept; a production server would need it (see the cost estimate in ARCHITECTURE_DECISION.md) |

"Verified" means the checks below found no difference. It does not mean free of defects: the round 1 build
passed every check here and still had two ways to hang the server (§11).

## Evidence sources

| Source | What it does | Result (round 2 build) |
|---|---|---|
| Differential test, [`poc/tests/differential.py`](poc/tests/differential.py) | Sends identical command streams to Valkey and rvk-server and compares replies exactly, error texts included. It covers 358 hand-written cases (strings, integers, keyspace, transactions, connection, RESP3, two-client WATCH, raw protocol) and 50,000 random commands (seed 7) | 50,358 cases, **1 mismatch** (`INCRBYFLOAT` formatting, §2). [`results/poc/differential.json`](results/poc/differential.json); round 1: same result |
| Regression scenarios, [`poc/tests/regressions.py`](poc/tests/regressions.py) | 22 scenarios built from the review's findings. Each starts a fresh Valkey and a fresh rvk-server with the same options and compares what a client observes | **22 of 22 pass.** On the round 1 binary, 17 fail outright and 4 cannot run (that binary has no `--enable-debug-command`). [`results/poc/regressions.json`](results/poc/regressions.json), [`regressions_round1_binary.json`](results/poc/regressions_round1_binary.json) |
| Concurrency invariants, [`poc/tests/concurrency.py`](poc/tests/concurrency.py) | Atomic INCR, atomic MSET across shards, MULTI/EXEC isolation, WATCH compare-and-set, 1 ms TTL races, memory bound under `maxmemory` with 1000- and 100-byte values | **All pass** on Valkey (control), and on rvk with 2 and 4 threads, with and without `maxmemory 64mb allkeys-lru`. [`results/poc/concurrency.jsonl`](results/poc/concurrency.jsonl) |
| Eviction stress | `valkey-benchmark` writes 5M SETs (pipeline 64, 8 connections) into rvk-2t at `maxmemory 256mb allkeys-lru` | **0 errors** in 3 runs. [`results/poc/eviction_stress.txt`](results/poc/eviction_stress.txt). This found a race that the Python invariants missed (REASSESSMENT.md §6) |
| Valkey's Tcl suite, [`poc/tests/valkey_suite.sh`](poc/tests/valkey_suite.sh) | Runs 8 of Valkey's test files against an external server | Valkey 444 passed. rvk **68 passed, 3 failed** (all 3 use `SADD`/`SRANDMEMBER`, which are out of scope). 6 files stopped at their first out-of-scope command (§9) |
| Benchmarks (BENCHMARK_RESULTS.md Part II) | Every run checks for error replies, timeouts and reply misattribution | 0, 0 and 0 in every run |

The Tcl suite is weak evidence for rvk-server. Valkey's test files mix data types, so most files abort early on a
list or set command, and the in-scope tests after that point never run. The differential test only checks cases
someone thought to write, plus random commands from a fixed grammar. Neither caught the round 1 deadlocks. Code
review did.

## 1. Protocol and connections

| Feature | Status | Notes |
|---|---|---|
| RESP2 multibulk requests, pipelining | **verified** | Strict CRLF checks, `string2ll` length parsing ("-0", leading zeros and 21+ digit lengths rejected), Valkey's error texts, CR/LF in a quoted byte mapped to a space. A partly received request is resumed, not re-parsed, so each byte is parsed once as in Valkey. Property tests split requests at every byte and resume across buffer compaction |
| Inline requests | **verified** | `sdsparsearg` tokenizer semantics: quoting, `\xHH` escapes, concatenated segments, "unbalanced quotes", embedded NUL rejected |
| Unauthenticated limits | **verified** | Multibulk > 10 or bulk > 16 KiB before AUTH is refused. Like Valkey, a client that has never authenticated is limited to a 1 MB query buffer and about 1 KB of output beyond the 16 KB static buffer |
| `proto-max-bulk-len`, `client-query-buffer-limit` | **verified** | Same defaults. As in Valkey, the limit applies to input not yet executed, and queued MULTI arguments count toward it. Values are capped below 2 GB (offsets are 32-bit) |
| RESP3 (`HELLO 3`): maps, nulls, doubles, verbatim strings | **verified** | `HELLO` reply has the same shape. `server`/`version` fields identify rvk. HELLO inside MULTI switches the protocol of later replies in the same EXEC |
| Output backpressure | **implemented** | A connection with more than 64 MB of pending output is not read until it drains to 32 MB, so TCP flow control pushes back on the client. Valkey's default for normal clients is unlimited buffering; its `client-output-buffer-limit` disconnect is not implemented |
| Half-close and EOF | **verified** | Data that arrives together with the FIN is executed and answered before the connection closes |
| `maxclients` | **verified** | Valkey's error text. The check cannot be raced by concurrent accepts on different workers. At start-up the open-files limit is raised, and `maxclients` is lowered to what it allows, as Valkey does |
| TCP keepalive | **implemented** | 300 s, as Valkey's default `tcp-keepalive` |
| Port already in use | **implemented** | Start-up fails, even though the workers use `SO_REUSEPORT` |
| TLS, Unix sockets, `bind` to multiple addresses | **out of scope** | |

## 2. String commands (20 of Valkey's 26)

| Command(s) | Status | Notes |
|---|---|---|
| `GET`, `SET` with `NX`/`XX`/`GET`/`EX`/`PX`/`EXAT`/`PXAT`/`KEEPTTL`/`IFEQ`/`IFNE`, `SETNX`, `SETEX`, `PSETEX`, `GETSET`, `GETEX`, `GETDEL` | **verified** | Option conflicts and error texts match |
| `MGET`, `MSET`, `MSETNX` | **verified** | Atomic across shards (concurrency invariant `mset_atomic`) |
| `STRLEN`, `GETRANGE` | **verified** | `GETRANGE` copies only the requested range |
| `APPEND`, `SETRANGE` | **partial** | Correct results, but each call copies the whole value. Building a large value with many small APPENDs is quadratic, where Valkey grows the string in place |
| `INCR`, `DECR`, `INCRBY`, `DECRBY` | **verified** | Overflow and "not an integer" errors match |
| `INCRBYFLOAT` | **partial** | rvk computes in `f64`, Valkey in `long double`. (a) Formatting: the shortest round-trip `f64` against 17 significant digits, e.g. Valkey `1005.59999999999999998`, rvk `1005.6` (the one differential mismatch). (b) Range: results beyond `f64` (above about 1.8e308) are refused, and increments below about 1e-17 print differently. (c) Input: hexadecimal floats are refused |
| `LCS`, `SUBSTR`, `INCREX`, `DELEX`, `DELIFEQ`, `MSETEX` | **unsupported** | `INCREX`, `DELEX`, `DELIFEQ` and `MSETEX` are recent Valkey additions |
| Integer and embedded-string encodings, `OBJECT ENCODING` | **out of scope** | Values are stored as bytes; integers are parsed on use |

## 3. Keyspace (17 of Valkey's 29 generic commands)

| Command(s) | Status | Notes |
|---|---|---|
| `DEL`, `UNLINK`, `EXISTS`, `TYPE`, `RENAME`, `RENAMENX`, `DBSIZE` | **verified** | `UNLINK` frees synchronously |
| `KEYS` | **verified** | `stringmatchlen` semantics, including an unterminated `[` class. Valkey's cutoff of 1000 nested `*` recursions is approximated as 1000 `*` groups on the match path |
| `RANDOMKEY` | **verified** | Expired keys it samples are deleted and sampling continues, so a live key is returned whenever one exists |
| `FLUSHALL`, `FLUSHDB` | **verified** | `ASYNC` accepted and runs synchronously |
| `SCAN` | **unsupported** | Operators rely on it; a production server needs it |
| `COPY`, `TOUCH`, `OBJECT`, `SORT`, `SORT_RO`, `MOVE` | **unsupported** | |
| `DUMP`, `RESTORE`, `MIGRATE`, `WAIT`, `WAITAOF` | **out of scope** | Need RDB encoding, replication or persistence |
| `SELECT` | **partial** | Database 0 only; other indexes return Valkey's "DB index is out of range" error |

## 4. Expiry

| Feature | Status | Notes |
|---|---|---|
| `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT` with `NX`/`XX`/`GT`/`LT` | **verified** | Past times delete the key, as in Valkey |
| `TTL`, `PTTL`, `EXPIRETIME`, `PEXPIRETIME`, `PERSIST` | **verified** | `TTL` and `EXPIRETIME` round as Valkey does |
| Lazy expiry on access | **verified** | An expired key is never returned (`expiry_race`). A key is live through the millisecond of its deadline, as in Valkey |
| Active expiry | **partial** | Different algorithm with the same guarantee: a per-shard min-heap of deadlines processed every 100 ms with a 25% time budget, instead of Valkey's random sampling. Reclamation timing therefore differs from Valkey's. `DEBUG SET-ACTIVE-EXPIRE` applies to every worker |
| Keyspace notifications for expiry | **out of scope** | |

## 5. Memory limit and eviction

| Feature | Status | Notes |
|---|---|---|
| `maxmemory`, `allkeys-lru` | **partial** | Approximated LRU with 5 samples and a 16-entry candidate pool, like `evict.c`, but the pool is per thread. Memory is jemalloc's `stats.allocated`, refreshed every 100 ms per thread and adjusted between refreshes by the store's own estimate, so new allocations outside the store (reply buffers) are seen up to 100 ms late. Peak overshoot measured at 0.2–0.3% of a 64 MB limit (Valkey: 0.02%). Eviction runs before DENYOOM commands, queued ones included; Valkey also evicts before read-only commands once over the limit |
| `allkeys-random`, `noeviction` | **implemented** | |
| OOM errors | **verified** | Same texts as Valkey, at queue time inside MULTI and as `EXECABORT` at EXEC |
| `volatile-*` policies, LFU, `maxmemory-clients`, `lazyfree-*` | **unsupported** | |

## 6. Transactions

| Feature | Status | Notes |
|---|---|---|
| `MULTI`, `EXEC`, `DISCARD` | **verified** | Queuing, `EXECABORT` on queue-time errors (including subcommand and arity errors of container commands), run-time errors inside `EXEC`, nesting errors. Isolation across threads holds (`exec_isolation`). `INFO` and `UNWATCH` inside a transaction work (they deadlocked in round 1) |
| `WATCH`, `UNWATCH` | **verified** | 26 two-client cases, plus Valkey's expiry rules from `unit/multi`: a key that expires before EXEC aborts it; deleting a key that was already expired at WATCH time does not; `FLUSHALL` touches only watched keys that exist. Compare-and-set loses no updates at 2 and 4 threads (`watch_cas`) |
| Commands Valkey refuses inside `MULTI` | **verified** | Same `NO_MULTI` errors (`SHUTDOWN` included) |
| `EXEC` with `IFEQ`/`IFNE`/`NX`/`XX` conditions | **unsupported** | Recent Valkey addition. The transaction is discarded with an error, so the client is not left inside MULTI |

## 7. Connection and server commands

| Command(s) | Status | Notes |
|---|---|---|
| `PING`, `ECHO`, `QUIT`, `RESET`, `AUTH` | **verified** | `AUTH` supports `requirepass` (the `default` user) only, compared in constant time. `NOAUTH` is returned before unknown-command and arity errors, as in Valkey |
| `HELLO` (with `AUTH`/`SETNAME`) | **verified** | Options are validated, client names included, before authenticating |
| `CLIENT ID`/`GETNAME`/`SETNAME`/`SETINFO`/`INFO`/`LIST` | **partial** | `LIST` shows only the calling connection, and its fields are a subset. No `KILL`, `PAUSE`, `NO-EVICT`, `TRACKING`, `CACHING`, `REPLY`, `UNBLOCK` |
| `INFO` | **partial** | Server, clients, memory, stats and keyspace fields used by the harness, read without locks. No `redis_version`/`valkey_version` field, which stops Valkey's `unit/info-command` test at its first line. No `commandstats`, `errorstats`, `latencystats` |
| `CONFIG GET`/`SET`/`RESETSTAT` | **partial** | `GET` matches patterns over a fixed list of values. `SET` supports `maxmemory` (Valkey's units) and `maxmemory-policy`, accepts `save ""`/`appendonly no`, refuses anything else, and is all-or-nothing. `RESETSTAT` is race-free |
| `SHUTDOWN` | **verified** | Valkey's argument parsing and errors (`ABORT` gives "No shutdown in progress."). `SAVE` is refused because there is no persistence. On success the process exits without a reply |
| `DBSIZE`, `TIME` | **implemented** | |
| `COMMAND` | **partial** | `COUNT` is correct; bare `COMMAND`, `INFO` and `DOCS` return empty results, which is enough for the clients tested (redis-rs, valkey-benchmark) |
| `DEBUG` | **partial** | Refused unless `--enable-debug-command yes` (Valkey's default is also no). `SET-ACTIVE-EXPIRE` and `SLEEP` only |
| `FUNCTION FLUSH`, `SCRIPT FLUSH` | **partial** | Accepted as no-ops (there are no scripts) so test harnesses can reset state |

## 8. Out of scope (absent by design in this proof of concept)

Each of these exists in Valkey and would be needed by many production deployments. A production decision has to
cost them (ARCHITECTURE_DECISION.md §II.7).

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

Identical counts in round 1 and round 2.

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

No test that ran against an in-scope command failed. `unit/multi` aborts on its first test (it uses a list), so the
WATCH expiry tests in it never ran; the regression scenarios reproduce those tests instead.

## 10. Gate 2 (functional validity) status

Pre-registered in BENCHMARK_PLAN.md Part II §II.3:

| Condition | Round 1 build | Round 2 build |
|---|---|---|
| Differential mismatches only in features declared partial | Met (`INCRBYFLOAT`) | Met (`INCRBYFLOAT`) |
| Concurrency invariants pass at 2 and 4 threads, including under `maxmemory` | Met, after two defects found by the invariants were fixed | Met |
| Zero errors, timeouts and reply misattributions in every benchmark run | Met | Met (BENCHMARK_RESULTS.md Part II) |
| **Not part of the pre-registered gate:** no known defect that can hang or crash the server | **Not met**: two self-deadlocks and one remotely triggerable abort, found by the review after the round 1 measurements | Met for the reviewed code; every confirmed finding is fixed or documented (§11) |

Gate 2 as pre-registered did not test for this class of defect. The round 1 performance numbers are unaffected:
the deadlocks need MULTI with INFO or UNWATCH, which no benchmark sends. But the round 1 build was not fit to run
anywhere, and the gate's checks could not show that.

## 11. What the multi-agent review changed

Five reviewers (concurrency, protocol and I/O, command semantics, security, benchmark fairness) each had an
adversarial verifier. They reported 62 findings: 57 confirmed (52 from the concurrency, protocol and I/O, semantics
and security reviewers, 5 from the fairness reviewer), 4 refuted (all fairness) and 1 uncertain
([`results/poc/review_findings.json`](results/poc/review_findings.json)). Several findings appeared in more than one
dimension.

| Outcome | Findings |
|---|---|
| **Fixed, with a regression scenario** | EXEC deadlock with queued INFO or UNWATCH; MULTI/EXEC bypassing `maxmemory`; WATCH expiry and FLUSH rules; DEBUG SLEEP abort and DEBUG protection; SHUTDOWN parsing; NOAUTH ordering; subcommand errors at queue time; CONFIG SET atomicity and units; HELLO ordering and name validation; HELLO inside EXEC; EXPIRETIME rounding; RANDOMKEY with expired keys; KEYS unterminated class; protocol-error text; EOF with data; EOF after a full buffer; output backpressure; MULTI queue limit |
| **Fixed, checked by unit tests or inspection** | Resumable multibulk parsing; WATCH duplicate check O(1); unauthenticated buffer caps; bounded read-buffer growth; 32-bit offset limits; accept-error retry and open-files limit; maxclients race; TCP keepalive; duplicate-port detection; constant-time password comparison; race-free RESETSTAT (its race also produced the one negative eviction counter in the round 1 data, BENCHMARK_RESULTS.md Part II); eviction bookkeeping for expired candidates; expiry boundary; read-path statistics and NOTOUCH lookups; GETRANGE copy |
| **Documented as partial, not fixed** | APPEND/SETRANGE whole-value copies; INCRBYFLOAT range; `CLIENT LIST` scope; eviction only before DENYOOM commands; 100 ms memory snapshot |
| **Fairness findings** (affect how the benchmark comparison is read, not correctness) | Valkey spends about 5% of pipelined CPU on per-command latency and statistics bookkeeping that rvk does not do, which favors rvk. rvk paid avoidable costs of its own (false sharing on shared counters, an `Arc` clone per command, a vector allocation per command, hashing each key twice), which disfavored it. The round 2 build removes the false sharing and the `Arc` clone, keeps the other two, and uses keyed SipHash, as Valkey does, instead of foldhash. Of the five confirmed fairness findings, the other three duplicate rows above (unauthenticated buffer caps, MULTI/EXEC under `maxmemory`, TCP keepalive). See BENCHMARK_RESULTS.md §II.4 (profiles), §II.5 (round 2 results) and REASSESSMENT.md §7 |
