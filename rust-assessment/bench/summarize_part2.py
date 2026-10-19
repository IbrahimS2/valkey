#!/usr/bin/env python3
"""Aggregates Part II raw data (../results/raw2/*.jsonl) into ../results/summary_part2.md
and ../results/key_tables_part2.md, including the pre-registered Gate 3 evaluation
(BENCHMARK_PLAN.md Part II §II.3).

Every number is computed from the raw records. Conventions follow summarize.py
(Part I): medians of trials with [min-max], CV of throughput, server CPU µs per
operation over the whole data-service side.
"""

import argparse
import hashlib
import json
import os
import statistics

import summarize as s1
from summarize import fmt, med, rng, row_stats

RAW2 = os.path.join(s1.RESULTS, "raw2")
TOPOS = ["valkey-1t", "valkey-io2", "valkey-2shard", "rvk-1t", "rvk-2t"]
NATIVE_2CPU = ("valkey-io2", "valkey-2shard")
SINGLE_ENDPOINT = ("valkey-1t", "valkey-io2")
CORE = ("read90", "mixed50", "write90", "skew-read90")
PROVISIONED = {"valkey-1t": 1, "rvk-1t": 1, "valkey-io2": 2, "valkey-2shard": 2, "rvk-2t": 2, "rust-proxy": 2}


def load(name):
    path = os.path.join(RAW2, f"{name}.jsonl")
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return [json.loads(l) for l in f if l.strip()]


def ok(rs):
    return [r for r in rs if "error" not in r]


def stats(records, vcpu_hour=0.05, **match):
    rs = [r for r in ok(records) if all(r.get(k) == v for k, v in match.items())]
    return row_stats(rs, vcpu_hour) if rs else None


def cell(v, lat="p99"):
    if not v:
        return "–"
    sat = "⚠" if v["client_sat"] else ""
    return f"{fmt(v['ops'])}{sat} ({fmt(v['us_op'], 2)}; {fmt(v[lat])})"


def ratio(a, b, key="ops"):
    if not a or not b or not a[key] or not b[key]:
        return None
    return a[key] / b[key]


def rfmt(x):
    return "–" if x is None else f"{x:.2f}×"


# --------------------------------------------------------------------------
# key tables
# --------------------------------------------------------------------------

def throughput_table(out, title, recs, workloads, conns=50, note=""):
    out += [title, "",
            "ops/s median (server CPU µs/op; p99 µs). ⚠ = load generator above 170% CPU in at least one trial "
            "(throughput is then a lower bound). " + note, "",
            "| workload | " + " | ".join(TOPOS) + " | R1 rvk-1t÷valkey-1t | R2 rvk-2t÷valkey-io2 | R3 rvk-2t÷valkey-2shard |",
            "|---|" + "---|" * (len(TOPOS) + 3)]
    rows = {}
    for wl in workloads:
        row = {t: stats(recs, topo=t, workload=wl, conns=conns) for t in TOPOS}
        rows[wl] = row
        out.append(f"| {wl} | " + " | ".join(cell(row[t]) for t in TOPOS) +
                   f" | {rfmt(ratio(row['rvk-1t'], row['valkey-1t']))} | {rfmt(ratio(row['rvk-2t'], row['valkey-io2']))}"
                   f" | {rfmt(ratio(row['rvk-2t'], row['valkey-2shard']))} |")
    out.append("")
    return rows


def gate3(out, core, core16, conns, title="### P-G. Gate 3 evaluation (pre-registered, BENCHMARK_PLAN.md Part II §II.3)"):
    """Pre-registered Gate 3: five points, client-bound comparisons decided on -p16."""
    pts = []
    for wl in CORE:
        pts.append((f"{wl} c=50", {t: core.get((wl, t)) for t in TOPOS}, {t: core16.get((wl + "-p16", t)) for t in TOPOS}))
    pts.append(("read90 c=200", {t: conns.get(("read90", 200, t)) for t in TOPOS},
                {t: conns.get(("read90-p16", 200, t)) for t in TOPOS}))
    out += [title, "",
            "Each point uses the unpipelined run unless the load generator was above 170% CPU in a trial of a "
            "topology being compared, in which case the depth-16 pipelined run decides (as pre-registered). "
            "CPU/op columns compare `rvk-2t` with the best Valkey 2-CPU option (lowest CPU/op of `valkey-io2` "
            "and `valkey-2shard`) on the run used.", "",
            "| point | run used | R1 | R2 | R3 | rvk-2t CPU µs/op | best native CPU µs/op | R3 ≥ 1.20 | R2 ≥ 1.20 and R3 ≥ 0.90 |",
            "|---|---|---|---|---|---|---|---|---|"]
    tally = {"go_r3": 0, "cond": 0, "cpu_ok": 0}
    for label, p1, p16 in pts:
        bound = any(p1[t] and p1[t]["client_sat"] for t in ("rvk-2t", "valkey-io2", "valkey-2shard", "rvk-1t", "valkey-1t"))
        use = p16 if bound and all(p16[t] for t in ("rvk-2t", "valkey-io2", "valkey-2shard")) else p1
        used = "p16" if use is p16 else "unpipelined"
        r1, r2, r3 = ratio(use["rvk-1t"], use["valkey-1t"]), ratio(use["rvk-2t"], use["valkey-io2"]), \
            ratio(use["rvk-2t"], use["valkey-2shard"])
        nat = [use[t] for t in NATIVE_2CPU if use[t]]
        best_cpu = min(v["us_op"] for v in nat) if nat else None
        rvk_cpu = use["rvk-2t"]["us_op"] if use["rvk-2t"] else None
        go = r3 is not None and r3 >= 1.20
        cond = r2 is not None and r3 is not None and r2 >= 1.20 and r3 >= 0.90
        tally["go_r3"] += go
        tally["cond"] += cond
        tally["cpu_ok"] += bool(rvk_cpu is not None and best_cpu is not None and rvk_cpu <= best_cpu)
        out.append(f"| {label} | {used} | {rfmt(r1)} | {rfmt(r2)} | {rfmt(r3)} | {fmt(rvk_cpu, 2)} | {fmt(best_cpu, 2)} | "
                   f"{'yes' if go else 'no'} | {'yes' if cond else 'no'} |")
    out += ["", f"Points with R3 ≥ 1.20: **{tally['go_r3']} of 5**. Points with R2 ≥ 1.20 and R3 ≥ 0.90: "
                f"**{tally['cond']} of 5**. Points where rvk-2t CPU/op ≤ best native: **{tally['cpu_ok']} of 5**.", ""]
    return tally


def openloop_table(out, recs, title="### P-O. Latency at a fixed offered load (open loop, read90, 50 connections)"):
    out += [title, "",
            "Precise pacing (`--spin-pacing`). Each cell: p50 / p99 / p99.9 µs measured from the actual send time, "
            "then the same from the intended send time (includes load-generator lag), then server CPU µs/op.", "",
            "| offered ops/s | " + " | ".join(TOPOS) + " |", "|---|" + "---|" * len(TOPOS)]
    rates = sorted({r["rate"] for r in ok(recs) if r.get("pacing") == "spin"})
    by = {}
    for rate in rates:
        cells = []
        for t in TOPOS:
            rs = [r for r in ok(recs) if r["topo"] == t and r["rate"] == rate and r.get("pacing") == "spin"]
            if not rs:
                cells.append("–")
                continue
            a = lambda k: med([r["client"]["lat_us"][k] for r in rs])
            i = lambda k: med([(r["client"].get("lat_from_intended_us") or {}).get(k) for r in rs])
            us = med([r["derived"]["server_side_cpu_us_per_op"] for r in rs])
            by[(rate, t)] = dict(p50=a("p50"), p99=a("p99"), p999=a("p999"), ip50=i("p50"), ip99=i("p99"),
                                 ip999=i("p999"), us=us, ops=med([r["client"]["ops_per_sec"] for r in rs]),
                                 lag99=med([r["client"]["sched_lag_us"]["p99"] for r in rs]), n=len(rs))
            v = by[(rate, t)]
            cells.append(f"{fmt(v['p50'])} / {fmt(v['p99'])} / {fmt(v['p999'])}; intended {fmt(v['ip50'])} / "
                         f"{fmt(v['ip99'])} / {fmt(v['ip999'])} ({fmt(v['us'], 1)})")
        out.append(f"| {rate:,} | " + " | ".join(cells) + " |")
    out.append("")
    # p99 criterion at the same offered load
    out += ["p99 at the same offered load, rvk-2t relative to the best Valkey option (lowest p99), measured from the "
            "actual send time [and from the intended send time]:", "",
            "| offered ops/s | achieved by all? | vs best native 2-CPU (io2, 2shard) | vs best single-endpoint (1t, io2) |",
            "|---|---|---|---|"]
    for rate in rates:
        rv = by.get((rate, "rvk-2t"))
        if not rv:
            continue
        achieved = all(by.get((rate, t)) and by[(rate, t)]["ops"] >= 0.98 * rate for t in TOPOS if (rate, t) in by)
        def rel(group, k):
            vals = [by[(rate, t)][k] for t in group if (rate, t) in by and by[(rate, t)][k]]
            return rv[k] / min(vals) if vals and rv[k] else None
        out.append(f"| {rate:,} | {'yes' if achieved else 'no'} | {rfmt(rel(NATIVE_2CPU, 'p99'))} "
                   f"[{rfmt(rel(NATIVE_2CPU, 'ip99'))}] | {rfmt(rel(SINGLE_ENDPOINT, 'p99'))} "
                   f"[{rfmt(rel(SINGLE_ENDPOINT, 'ip99'))}] |")
    lags = [r["client"]["sched_lag_us"]["p99"] for r in ok(recs) if r.get("pacing") == "spin"]
    if lags:
        out += ["", f"Load-generator send lag with spin pacing: p99 median {fmt(med(lags))} µs across runs "
                    f"(range {fmt(min(lags))}–{fmt(max(lags))} µs)."]
    timer = [r for r in ok(recs) if r.get("pacing") != "spin"]
    if timer:
        out += ["", "Timer pacing (as in Part I), 60,000 ops/s, for comparison: " + "; ".join(
            f"{t} p50 {fmt(med([r['client']['lat_us']['p50'] for r in timer if r['topo'] == t]))} / p99 "
            f"{fmt(med([r['client']['lat_us']['p99'] for r in timer if r['topo'] == t]))} µs"
            for t in TOPOS if any(r["topo"] == t for r in timer)) + "."]
    out.append("")
    return by


def overload_table(out):
    recs = ok(load("p2-overload"))
    if not recs:
        return
    out += ["### P-L. High offered load, open loop (read90, 50 connections; added after pre-registration)", "",
            "Intended as overload, but no topology was overloaded: open-loop clients keep several requests in "
            "flight per connection, which works like pipelining, so every server sustained both rates.", "",
            "Achieved ops/s, sends skipped because a connection already had the maximum requests in flight, and "
            "p50 / p99 µs from the actual and from the intended send time.", "",
            "| offered ops/s | topology | achieved ops/s | skipped sends | p50 / p99 (sent) | p50 / p99 (intended) | "
            "server CPU % | errors+timeouts |", "|---|---|---|---|---|---|---|---|"]
    for rate in sorted({r["rate"] for r in recs}):
        for t in TOPOS:
            rs = [r for r in recs if r["rate"] == rate and r["topo"] == t]
            if not rs:
                continue
            c = lambda f: med([f(r) for r in rs])
            out.append(f"| {rate:,} | {t} | {fmt(c(lambda r: r['client']['ops_per_sec']))} | "
                       f"{fmt(c(lambda r: r['client'].get('send_skipped')))} | "
                       f"{fmt(c(lambda r: r['client']['lat_us']['p50']))} / {fmt(c(lambda r: r['client']['lat_us']['p99']))} | "
                       f"{fmt(c(lambda r: r['client']['lat_from_intended_us']['p50']))} / "
                       f"{fmt(c(lambda r: r['client']['lat_from_intended_us']['p99']))} | "
                       f"{fmt(c(lambda r: r['derived']['server_side_cpu_pct']))} | "
                       f"{sum((r['client'].get('errors') or 0) + (r['client'].get('timeouts') or 0) for r in rs)} |")
    out.append("")


def other_table(out, vcpu_hour):
    out += ["### P-X. Other scenarios", "",
            "| scenario | topology | ops/s median [min–max] | server CPU µs/op | p50 µs | p99 µs | hit rate | notes |",
            "|---|---|---|---|---|---|---|---|"]
    for suite, wl, label in (("p2-proxy", "read90", "Part I proxy, read90 (continuity)"),
                             ("p2-large", "large-read90", "16 KiB values, read90"),
                             ("p2-ttl", "ttl-mixed50", "SET EX 5, mixed50"),
                             ("p2-cold", "cold-read90", "cold cache, read90"),
                             ("p2-evict", "evict-uniform", "maxmemory 256mb LRU, uniform"),
                             ("p2-evict", "evict-skew", "maxmemory 256mb LRU, Zipf 0.99"),
                             ("p2-r2-evict", "evict-uniform", "round 2: maxmemory 256mb LRU, uniform"),
                             ("p2-r2-evict", "evict-skew", "round 2: maxmemory 256mb LRU, Zipf 0.99"),
                             ("p2-churn", "get100-churn", "new connection per request, GET"),
                             ("p2-aof", "read90-aof", "AOF everysec, read90 (Valkey only)"),
                             ("p2-aof", "write90-aof", "AOF everysec, write90 (Valkey only)")):
        recs = [r for r in ok(load(suite)) if r["workload"] == wl]
        for t in TOPOS + ["rust-proxy"]:
            rs = [r for r in recs if r["topo"] == t]
            if not rs:
                continue
            v = row_stats(rs, vcpu_hour)
            notes = []
            ev = med([r["server"].get("evicted_keys") for r in rs])
            ex = med([r["server"].get("expired_keys") for r in rs])
            if ev:
                notes.append(f"evicted/run {fmt(ev)}")
            if ex:
                notes.append(f"expired/run {fmt(ex)}")
            keys = med([sum(int(str(g.get("db0") or "keys=0").split(",")[0].split("=")[1])
                            for k, g in r["server"].items() if k.startswith("gauges-")) for r in rs])
            if wl.startswith("evict") and keys:
                notes.append(f"keys held {fmt(keys)}")
            if v["client_sat"]:
                notes.append("client-bound")
            hit = fmt(v["hit"], 3) if v["hit"] is not None else "–"
            out.append(f"| {label} | {t} | {fmt(v['ops'])} [{fmt(v['lo'])}–{fmt(v['hi'])}] | {fmt(v['us_op'], 2)} | "
                       f"{fmt(v['p50'])} | {fmt(v['p99'])} | {hit} | {'; '.join(notes)} |")
    out.append("")


def clients_table(out):
    recs = ok(load("p2-clients"))
    if not recs:
        return
    out += ["### P-C. Candidate C: redis-rs 1.7.1 client functionality (rsclient), 50 logical clients", "",
            "`rs-*` workloads: 1M keys, 100-byte values, 90% GET. `mux1` = all 50 logical clients multiplexed on one "
            "socket; otherwise one socket each. `cache` = redis-rs client-side cache of 100,000 entries "
            "(RESP3, server-assisted invalidation). Stale reads: a GET returned a value older than a write that was "
            "acknowledged before the GET was issued.", "",
            "| topology | workload | ops/s median [min–max] | server CPU µs/op | client CPU % | p50 µs | p99 µs | "
            "stale reads / GETs | stale age p50 / p99 / max µs | cache hits / misses / invalidations |",
            "|---|---|---|---|---|---|---|---|---|---|"]
    seen = []
    for r in recs:
        k = (r["topo"], r["workload"])
        if k not in seen:
            seen.append(k)
    for t, wl in seen:
        rs = [r for r in recs if r["topo"] == t and r["workload"] == wl]
        v = row_stats(rs, 0.05)
        c = lambda k: med([r["client"].get(k) for r in rs])
        age = lambda k: med([(r["client"].get("stale_age_us") or {}).get(k) for r in rs])
        stale = c("stale_reads")
        gets = c("gets")
        frac = f"{fmt(stale)} / {fmt(gets)} ({100 * stale / gets:.2f}%)" if gets else "–"
        ages = f"{fmt(age('p50'))} / {fmt(age('p99'))} / {fmt(age('max'))}" if stale else "–"
        cache = (f"{fmt(c('cache_hits'))} / {fmt(c('cache_misses'))} / {fmt(c('cache_invalidations'))}"
                 if c("cache_entries") else "–")
        out.append(f"| {t} | {wl} | {fmt(v['ops'])} [{fmt(v['lo'])}–{fmt(v['hi'])}] | {fmt(v['us_op'], 2)} | "
                   f"{fmt(v['client_cpu'])} | {fmt(v['p50'])} | {fmt(v['p99'])} | {frac} | {ages} | {cache} |")
    out.append("")


def crosscheck_table(out):
    recs = ok(load("p2-crosscheck"))
    if not recs:
        return
    out += ["### P-K. Load generator cross-check (GET only, 50 connections)", "",
            "| topology | tool | n | ops/s median [min–max] | CV | p50 µs | p99 µs | server CPU % | client CPU % |",
            "|---|---|---|---|---|---|---|---|---|"]
    for t in ("valkey-1t", "rvk-1t", "rvk-2t"):
        for tool in ("valkey-benchmark", "loadgen"):
            rs = [r for r in recs if r["topo"] == t and r["tool"] == tool]
            if not rs:
                continue
            v = row_stats(rs, 0.05)
            out.append(f"| {t} | {tool} | {v['n']} | {fmt(v['ops'])} [{fmt(v['lo'])}–{fmt(v['hi'])}] | "
                       f"{fmt(v['cv'] * 100 if v['cv'] is not None else None, 1)}% | {fmt(v['p50'])} | {fmt(v['p99'])} | "
                       f"{fmt(v['server_cpu'])} | {fmt(v['client_cpu'])} |")
    out.append("")


def cpu_split(out):
    recs = ok(load("p2-core")) + ok(load("p2-core-p16"))
    out += ["### P-S. User vs kernel CPU split (median per process)", "",
            "| topology | pipeline | process | user % | sys % | sys share |", "|---|---|---|---|---|---|"]
    for t in TOPOS:
        for pl in (1, 16):
            rs = [r for r in recs if r["topo"] == t and r["workload_def"]["pipeline"] == pl]
            procs = {}
            for r in rs:
                for p, c in r["cpu"].items():
                    name = p.split("-")[0] if p != "client" else p
                    procs.setdefault(name, ([], []))
                    procs[name][0].append(c["user_pct"])
                    procs[name][1].append(c["sys_pct"])
            for p, (u, sy) in procs.items():
                mu, ms = med(u), med(sy)
                out.append(f"| {t} | {pl} | {p} | {fmt(mu)} | {fmt(ms)} | {fmt(100 * ms / (mu + ms), 0)}% |")
    out.append("")


def cost_table(out, core, vcpu_hour):
    out += ["### P-$. Cost per million operations, read90, 50 connections", "",
            f"Assumed price ${vcpu_hour} per vCPU-hour (illustrative, not a quote). \"Provisioned\" counts the vCPUs "
            "a topology occupies (1 for single-threaded servers). Linear scale-out to 1M ops/s is assumed, not "
            "verified.", "",
            "| topology | consumed CPU-s per Mop | throughput per provisioned vCPU | vCPUs for 1M ops/s | "
            "monthly cost at 730 h | vs valkey-2shard |", "|---|---|---|---|---|---|"]
    base = None
    rows = []
    for t in TOPOS:
        v = core.get(("read90", t))
        if not v:
            continue
        per_vcpu = v["ops"] / PROVISIONED[t]
        vcpus = 1e6 / per_vcpu
        cost = vcpus * vcpu_hour * 730
        rows.append((t, v, per_vcpu, vcpus, cost))
        if t == "valkey-2shard":
            base = cost
    for t, v, per_vcpu, vcpus, cost in rows:
        rel = f"{cost - base:+,.0f} $ ({cost / base:.2f}×)" if base else "–"
        sat = " ⚠" if v["client_sat"] else ""
        out.append(f"| {t}{sat} | {fmt(v['us_op'], 2)} | {fmt(per_vcpu)} ops/s | {vcpus:.1f} | ${cost:,.0f} | {rel} |")
    out.append("")


def recovery2(out):
    recs = load("recovery")
    if not recs:
        return
    mem = [r for r in recs if r["section"] == "p2-memory"]
    if mem:
        out += ["### P-M. Memory per key (both jemalloc)", "",
                "| implementation | keys | value bytes | payload bytes/key | used_memory bytes/key | RSS bytes/key |",
                "|---|---|---|---|---|---|"]
        for (impl, keys, vs), rs in s1.group(mem, ["impl", "keys", "value_size"]).items():
            out.append(f"| {impl} | {keys:,} | {vs:,} | {rs[0]['payload_per_key']} | "
                       f"{fmt(med([r['used_memory_per_key'] for r in rs]), 1)} | {fmt(med([r['rss_per_key'] for r in rs]), 1)} |")
        out.append("")
    st = [r for r in recs if r["section"] == "p2-startup"]
    if st:
        out.append("Start to first PONG, empty dataset: " + "; ".join(
            f"{impl} median {fmt(med([r['start_to_pong_seconds'] for r in rs]) * 1000, 1)} ms (n={len(rs)})"
            for (impl,), rs in s1.group(st, ["impl"]).items()) + ".")
        out.append("")
    fo = [r for r in recs if r["section"] == "p2-failover"]
    if fo:
        out += ["Failover: server `SIGKILL`ed 4 s into a 10 s open-loop run at 20k ops/s and restarted at once (empty).",
                "", "| server | n | ops completed (of 200,000) | errors | timeouts | connections lost | restart s | hit rate |",
                "|---|---|---|---|---|---|---|---|"]
        for (impl,), rs in s1.group(fo, ["impl"]).items():
            out.append(f"| {impl} | {len(rs)} | {fmt(med([r['ops_completed'] for r in rs]))} | "
                       f"{fmt(med([r['errors'] for r in rs]))} | {fmt(med([r['timeouts'] for r in rs]))} | "
                       f"{fmt(med([r['conn_failures'] for r in rs]))} | {fmt(med([r['restart_seconds'] for r in rs]), 3)} | "
                       f"{fmt(med([r['hit_rate'] for r in rs if r['hit_rate'] is not None]), 3)} |")
        out.append("")


# --------------------------------------------------------------------------
# validity
# --------------------------------------------------------------------------

def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def validity(suites):
    lines = []
    runs = {"loadgen": 0, "rsclient": 0, "valkey-benchmark": 0}
    mism = bound = errs = touts = lost = 0
    gaps = []
    failed = []
    for name in suites:
        for r in load(name):
            if "error" in r:
                failed.append(f"{name}: {r.get('topo')} {r.get('workload')} c={r.get('conns')} t{r.get('trial')}: "
                              f"{r['error'][:200]}")
                continue
            tool = r.get("tool", "loadgen")
            runs[tool] = runs.get(tool, 0) + 1
            c = r["client"]
            errs += c.get("errors") or 0
            touts += c.get("timeouts") or 0
            lost += c.get("conn_failures") or 0
            bound += bool(r["derived"]["client_saturated"])
            if tool == "loadgen" and c["gets"] != c["hits"] + c["misses"]:
                mism += 1
            srv = r["derived"].get("server_measured_cmds_per_sec")
            if tool == "loadgen" and srv and c["ops_per_sec"] and r["workload_def"]["pipeline"] == 1 and not r["rate"]:
                gaps.append(abs(srv - c["ops_per_sec"]) / c["ops_per_sec"])
    lines.append(f"- runs: " + ", ".join(f"{k} {v}" for k, v in runs.items()))
    lines.append(f"- loadgen runs where a GET received a non-bulk, non-nil reply (reply misattribution): {mism}")
    lines.append(f"- error replies, timeouts and lost connections summed over loadgen and rsclient runs: "
                 f"{errs}, {touts}, {lost} (valkey-benchmark does not report errors in this mode)")
    lines.append(f"- runs flagged client-bound (load generator > 170% CPU): {bound}")
    if gaps:
        gaps.sort()
        lines.append(f"- client vs server-side (INFO) throughput, absolute difference: median "
                     f"{100 * statistics.median(gaps):.1f}%, max {100 * gaps[-1]:.1f}% (n={len(gaps)}; closed-loop "
                     f"unpipelined loadgen runs)")
    paths = {"loadgen": os.path.join(s1.HERE, "rust", "target", "release", "loadgen"),
             "rvk-server": os.path.join(s1.HERE, "..", "poc", "target", "release", "rvk-server"),
             "rsclient": os.path.join(s1.HERE, "..", "poc", "target", "release", "rsclient"),
             "valkey-server": os.path.join(s1.HERE, "..", "..", "src", "valkey-server")}
    now = {k: sha256(p) for k, p in paths.items() if os.path.exists(p)}
    for name in ("environment_part2.json", "environment_part2_r2.json"):
        env = os.path.join(s1.RESULTS, name)
        if os.path.exists(env):
            rec = json.load(open(env))["binaries_sha256"]
            lines.append(f"- binaries now vs {name}: " + ", ".join(
                f"{k} {'same' if now.get(k) == rec.get(k) else 'different'}" for k in paths))
    return lines, failed


SUITES = ["p2-crosscheck", "p2-core", "p2-core-p16", "p2-proxy", "p2-conns", "p2-openloop", "p2-large", "p2-ttl",
          "p2-cold", "p2-evict", "p2-churn", "p2-clients", "p2-overload", "p2-aof"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--vcpu-hour", type=float, default=0.05)
    args = ap.parse_args()
    vh = args.vcpu_hour
    key = ["# Part II key tables (generated by bench/summarize_part2.py)", ""]

    core_recs, core16_recs, conns_recs = load("p2-core"), load("p2-core-p16"), load("p2-conns")
    core = {(wl, t): stats(core_recs, vh, topo=t, workload=wl) for wl in CORE for t in TOPOS}
    core16 = {(wl + "-p16", t): stats(core16_recs, vh, topo=t, workload=wl + "-p16") for wl in CORE for t in TOPOS}
    conns = {(wl, c, t): stats(conns_recs, vh, topo=t, workload=wl, conns=c)
             for wl in ("read90", "read90-p16") for c in (1, 10, 200, 1000) for t in TOPOS}

    throughput_table(key, "### P-1. Saturation throughput, 50 connections, unpipelined", core_recs, CORE)
    throughput_table(key, "### P-2. Saturation throughput, 50 connections, pipeline depth 16", core16_recs,
                     [w + "-p16" for w in CORE])
    key += ["### P-3. Concurrency sweep, read90", "",
            "ops/s median (server CPU µs/op; p50 µs). The 50-connection row is from P-1.", "",
            "| connections | " + " | ".join(TOPOS) + " | R2 | R3 |", "|---|" + "---|" * (len(TOPOS) + 2)]
    for c in (1, 10, 50, 200, 1000):
        row = {t: (core[("read90", t)] if c == 50 else conns[("read90", c, t)]) for t in TOPOS}
        key.append(f"| {c} | " + " | ".join(cell(row[t], "p50") for t in TOPOS) +
                   f" | {rfmt(ratio(row['rvk-2t'], row['valkey-io2']))} | {rfmt(ratio(row['rvk-2t'], row['valkey-2shard']))} |")
    row = {t: conns[("read90-p16", 200, t)] for t in TOPOS}
    key.append("| 200, pipeline 16 | " + " | ".join(cell(row[t], "p50") for t in TOPOS) +
               f" | {rfmt(ratio(row['rvk-2t'], row['valkey-io2']))} | {rfmt(ratio(row['rvk-2t'], row['valkey-2shard']))} |")
    key.append("")
    gate3(key, core, core16, conns)
    openloop_table(key, load("p2-openloop"))
    overload_table(key)
    other_table(key, vh)
    clients_table(key)
    crosscheck_table(key)
    cpu_split(key)
    cost_table(key, core, vh)
    recovery2(key)
    # Round 2 (post hoc): the same points after the review fixes and optimizations.
    r2_core, r2_core16, r2_conns = load("p2-r2-core"), load("p2-r2-core-p16"), load("p2-r2-conns")
    if r2_core:
        c2 = {(wl, t): stats(r2_core, vh, topo=t, workload=wl) for wl in CORE for t in TOPOS}
        c216 = {(wl + "-p16", t): stats(r2_core16, vh, topo=t, workload=wl + "-p16") for wl in CORE for t in TOPOS}
        cn2 = {(wl, c, t): stats(r2_conns, vh, topo=t, workload=wl, conns=c)
               for wl in ("read90", "read90-p16") for c in (200,) for t in TOPOS}
        throughput_table(key, "### P-R1. Round 2 (post hoc): saturation throughput, 50 connections, unpipelined",
                         r2_core, CORE)
        throughput_table(key, "### P-R2. Round 2 (post hoc): pipeline depth 16", r2_core16, [w + "-p16" for w in CORE])
        throughput_table(key, "### P-R3. Round 2 (post hoc): 200 connections", r2_conns, ["read90", "read90-p16"],
                         conns=200)
        gate3(key, c2, c216, cn2, title="### P-RG. Round 2 (post hoc): Gate 3 rules applied to the re-measured points")
        r2_ol = load("p2-r2-openloop")
        if r2_ol:
            openloop_table(key, r2_ol, title="### P-RO. Round 2 (post hoc): latency at a fixed offered load")
        cost_table(key, c2, vh)
    lines, failed = validity(SUITES + ["p2-r2-core", "p2-r2-core-p16", "p2-r2-conns", "p2-r2-openloop", "p2-r2-evict"])
    key += ["### P-V. Validity checks", ""] + lines + ["", "Failed runs: " + ("none." if not failed else "")]
    key += [f"- {f}" for f in failed]

    with open(os.path.join(s1.RESULTS, "key_tables_part2.md"), "w") as f:
        f.write("\n".join(key) + "\n")

    # Detailed per-scenario tables, reusing Part I's formatter.
    out = ["# Part II benchmark summary (generated by bench/summarize_part2.py)", "",
           "Per-scenario tables with trial ranges, CV, p95/p99.9, CPU and cost columns. Key tables, the Gate 3 "
           "evaluation and validity checks are in key_tables_part2.md."]
    titles = [("p2-core", "## Core workloads, 50 connections", ["topo", "workload"], lambda s: f"workload {s[0]}"),
              ("p2-core-p16", "## Core workloads, pipeline 16", ["topo", "workload"], lambda s: f"workload {s[0]}"),
              ("p2-proxy", "## Part I proxy (continuity)", ["topo", "workload"], lambda s: f"workload {s[0]}"),
              ("p2-conns", "## Concurrency sweep", ["topo", "workload", "conns"],
               lambda s: f"{s[0]}, {s[1]} connections"),
              ("p2-openloop", "## Open loop", ["topo", "rate", "pacing"], lambda s: f"offered {s[0]:,} ops/s, "
                                                                                   f"pacing {s[1] or 'timer'}"),
              ("p2-large", "## Large values", ["topo", "workload"], lambda s: f"workload {s[0]}"),
              ("p2-ttl", "## TTL", ["topo", "workload"], lambda s: f"workload {s[0]}"),
              ("p2-cold", "## Cold cache", ["topo", "workload"], lambda s: f"workload {s[0]}"),
              ("p2-evict", "## Eviction", ["topo", "workload"], lambda s: f"workload {s[0]}"),
              ("p2-churn", "## Connection churn", ["topo", "workload"], lambda s: f"workload {s[0]}"),
              ("p2-clients", "## Candidate C clients", ["topo", "workload"], lambda s: f"workload {s[0]}"),
              ("p2-overload", "## Overload (open loop above capacity; added after pre-registration)",
               ["topo", "rate"], lambda s: f"offered {s[0]:,} ops/s"),
              ("p2-aof", "## Valkey with AOF (added after pre-registration)", ["topo", "workload"],
               lambda s: f"workload {s[0]}"),
              ("p2-crosscheck", "## Cross-check", ["topo", "tool"], lambda s: f"tool {s[0]}")]
    for name, title, keys, tfn in titles:
        recs = load(name)
        for r in recs:
            r.setdefault("pacing", None)
        if recs:
            out.append(f"\n{title}")
            s1.suite_tables(out, name, recs, keys, vh, tfn)
    with open(os.path.join(s1.RESULTS, "summary_part2.md"), "w") as f:
        f.write("\n".join(out) + "\n")
    print("wrote results/key_tables_part2.md and results/summary_part2.md")


if __name__ == "__main__":
    main()
