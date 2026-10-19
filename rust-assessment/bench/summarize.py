#!/usr/bin/env python3
"""Aggregates ../results/raw/*.jsonl into ../results/summary.md.

Every table cell is computed from the raw records; nothing is typed in by
hand. Trials are summarized as median with [min-max] range and the
coefficient of variation (CV) of throughput.

Cost columns:
  * CPU-s/Mop: server-side CPU seconds consumed per million operations. This
    equals the server-side CPU microseconds per operation.
  * $/Bop provisioned: the 2-vCPU data-service budget divided by measured
    throughput, per billion operations, priced at --vcpu-hour (an assumed
    price, not a quote).
"""

import argparse
import glob
import json
import os
import statistics
from collections import OrderedDict, defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
RESULTS = os.path.join(HERE, "..", "results")
BUDGET_VCPUS = 2


def load(name):
    path = os.path.join(RESULTS, "raw", f"{name}.jsonl")
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return [json.loads(l) for l in f if l.strip()]


def med(xs):
    xs = [x for x in xs if x is not None]
    return statistics.median(xs) if xs else None


def rng(xs):
    xs = [x for x in xs if x is not None]
    return (min(xs), max(xs)) if xs else (None, None)


def cv(xs):
    xs = [x for x in xs if x is not None]
    if len(xs) < 2 or statistics.mean(xs) == 0:
        return None
    return statistics.stdev(xs) / statistics.mean(xs)


def fmt(v, nd=0):
    if v is None:
        return "–"
    if isinstance(v, float) and nd == 0:
        return f"{v:,.0f}"
    return f"{v:,.{nd}f}"


def net_bytes_per_op(r):
    """Loopback bytes per op: client<->(proxy|Valkey), plus proxy<->Valkey.

    Both loadgen's byte counters and Valkey's INFO counters span the whole
    run (warmup included), so both are divided by the commands Valkey
    processed over the same span; the proxy forwards commands one to one.
    """
    c, s = r["client"], r["server"]
    cmds = max(1, s["total_commands_processed"])
    per_op = (c["bytes_tx"] + c["bytes_rx"]) / cmds
    if r["topo"] == "rust-proxy":
        per_op += (s["total_net_input_bytes"] + s["total_net_output_bytes"]) / cmds
    return per_op


def group(records, keys):
    g = OrderedDict()
    for r in records:
        if "error" in r:
            continue
        g.setdefault(tuple(r[k] for k in keys), []).append(r)
    return g


def row_stats(rs, vcpu_hour):
    ops = [r["client"]["ops_per_sec"] for r in rs]
    lat = lambda k: med([r["client"]["lat_us"].get(k) for r in rs])
    d = lambda k: med([r["derived"][k] for r in rs])
    errs = sum((r["client"].get("errors") or 0) + (r["client"].get("timeouts") or 0) for r in rs)
    hit = med([r["client"].get("hit_rate") for r in rs if isinstance(r["client"].get("hit_rate"), (int, float))])
    m_ops = med(ops)
    lo, hi = rng(ops)
    usd_per_bop = (BUDGET_VCPUS * vcpu_hour / 3600) / m_ops * 1e9 if m_ops else None
    return dict(n=len(rs), ops=m_ops, lo=lo, hi=hi, cv=cv(ops), p50=lat("p50"), p95=lat("p95"), p99=lat("p99"),
                p999=lat("p999") or lat("p999_upper"), server_cpu=d("server_side_cpu_pct"),
                client_cpu=d("client_cpu_pct"), us_op=d("server_side_cpu_us_per_op"),
                total_us_op=d("total_cpu_us_per_op"), errs=errs, hit=hit,
                client_sat=any(r["derived"]["client_saturated"] for r in rs),
                srv_ops=med([r["derived"].get("server_measured_cmds_per_sec") for r in rs]),
                rss=med([r["derived"]["server_side_rss_bytes"] for r in rs]),
                usd=usd_per_bop)


HEADER = ("| topology | n | ops/s median [min–max] | CV | p50 µs | p99 µs | p99.9 µs | server-side CPU % "
          "| client CPU % | server CPU-s/Mop | total CPU-s/Mop | $/Bop provisioned | errors+timeouts |")
SEP = "|" + "---|" * 13


def table_row(label, s):
    sat = " ⚠client-bound" if s["client_sat"] else ""
    return (f"| {label}{sat} | {s['n']} | {fmt(s['ops'])} [{fmt(s['lo'])}–{fmt(s['hi'])}] | "
            f"{fmt(s['cv'] * 100 if s['cv'] is not None else None, 1)}% | {fmt(s['p50'])} | {fmt(s['p99'])} | "
            f"{fmt(s['p999'])} | {fmt(s['server_cpu'])} | {fmt(s['client_cpu'])} | {fmt(s['us_op'], 2)} | "
            f"{fmt(s['total_us_op'], 2)} | {fmt(s['usd'], 3)} | {s['errs']} |")


def suite_tables(out, name, records, keys, vcpu_hour, title_fn):
    groups = group(records, keys)
    by_scenario = OrderedDict()
    for k, rs in groups.items():
        by_scenario.setdefault(k[1:], []).append((k[0], rs))
    for scen, items in by_scenario.items():
        out.append(f"\n#### {title_fn(scen)}\n")
        out.append(HEADER)
        out.append(SEP)
        base = None
        stats = []
        for topo, rs in items:
            s = row_stats(rs, vcpu_hour)
            stats.append((topo, s))
            if topo == "valkey-1t":
                base = s
            out.append(table_row(topo, s))
        if base and base["ops"] and len(stats) > 1:
            rel = ", ".join(f"{t}: {s['ops'] / base['ops']:.2f}×" for t, s in stats if t != "valkey-1t")
            line = f"\nThroughput relative to valkey-1t (median/median): {rel}."
            # Highest throughput wins; at equal throughput (open loop) the
            # lowest CPU per operation wins.
            best_native = max((s for t, s in stats if t in ("valkey-io2", "valkey-2shard")),
                              key=lambda s: (round((s["ops"] or 0) / 1000), -(s["us_op"] or 0)), default=None)
            proxy = next((s for t, s in stats if t == "rust-proxy"), None)
            if proxy and best_native and best_native["ops"]:
                line += (f" rust-proxy vs best Valkey-native 2-CPU topology: "
                         f"{proxy['ops'] / best_native['ops']:.2f}× throughput, "
                         f"{proxy['us_op'] / best_native['us_op']:.2f}× server CPU per op.")
            out.append(line)
        extra = []
        for topo, rs in items:
            h = [r["client"].get("hit_rate") for r in rs]
            if any(isinstance(x, (int, float)) and x < 1 for x in h):
                extra.append(f"{topo} hit rate {fmt(med(h), 3)}")
            ev = med([r["server"].get("evicted_keys") for r in rs])
            ex = med([r["server"].get("expired_keys") for r in rs])
            if ev:
                extra.append(f"{topo} evicted_keys/run {fmt(ev)}")
            if ex:
                extra.append(f"{topo} expired_keys/run {fmt(ex)}")
            ps = [r.get("proxy_stats", {}).get("avg_cmds_per_upstream_write") for r in rs]
            if any(ps):
                extra.append(f"{topo} commands per upstream write {fmt(med(ps), 1)}")
            srv = med([r["derived"].get("server_measured_cmds_per_sec") for r in rs])
            if srv:
                extra.append(f"{topo} Valkey-side cmds/s (INFO) {fmt(srv)}")
            bpo = med([net_bytes_per_op(r) for r in rs if "client" in r and "bytes_tx" in r["client"]])
            if bpo:
                extra.append(f"{topo} loopback bytes/op {fmt(bpo, 1)}")
        if extra:
            out.append("\n" + "; ".join(extra) + ".")


def crosscheck_table(out, records):
    out.append("\n| tool | n | ops/s median [min–max] | CV | p50 µs | p99 µs | p99.9 µs | server CPU % | client CPU % |")
    out.append("|---|---|---|---|---|---|---|---|---|")
    for tool in ("valkey-benchmark", "loadgen"):
        rs = [r for r in records if r.get("tool") == tool and "error" not in r]
        if not rs:
            continue
        s = row_stats(rs, 0.0)
        out.append(f"| {tool} | {s['n']} | {fmt(s['ops'])} [{fmt(s['lo'])}–{fmt(s['hi'])}] | "
                   f"{fmt(s['cv'] * 100 if s['cv'] is not None else None, 1)}% | {fmt(s['p50'])} | {fmt(s['p99'])} | "
                   f"{fmt(s['p999'])} | {fmt(s['server_cpu'])} | {fmt(s['client_cpu'])} |")
    out.append("\nvalkey-benchmark's p99.9 is the upper bound of the HDR bucket containing p99.9.")


def cpu_split_table(out, records):
    out.append("\n| topology | process | user % | sys % | sys share |")
    out.append("|---|---|---|---|---|")
    for (topo,), rs in group(records, ["topo"]).items():
        procs = defaultdict(lambda: ([], []))
        for r in rs:
            for p, c in r["cpu"].items():
                name = "valkey" if p.startswith("valkey") else p
                procs[name][0].append(c["user_pct"])
                procs[name][1].append(c["sys_pct"])
        for p, (u, s) in procs.items():
            mu, ms = med(u), med(s)
            out.append(f"| {topo} | {p} | {fmt(mu)} | {fmt(ms)} | {fmt(100 * ms / (mu + ms), 0)}% |")


def recovery_tables(out, records):
    sec = defaultdict(list)
    for r in records:
        sec[r["section"]].append(r)
    if sec["memory"]:
        out.append("\n| implementation | keys | value bytes | payload bytes/key | used_memory bytes/key | RSS bytes/key |")
        out.append("|---|---|---|---|---|---|")
        g = group(sec["memory"], ["impl", "keys", "value_size"])
        for (impl, keys, vs), rs in g.items():
            out.append(f"| {impl} | {keys:,} | {vs:,} | {rs[0]['payload_per_key']} | "
                       f"{fmt(med([r.get('used_memory_per_key') for r in rs]), 1)} | "
                       f"{fmt(med([r.get('rss_per_key') for r in rs]), 1)} |")
    if sec["rdb"]:
        out.append("\n| keys | RDB bytes | SAVE s | restart→ready s (median [min–max]) | logged load s |")
        out.append("|---|---|---|---|---|")
        for (keys,), rs in group(sec["rdb"], ["keys"]).items():
            lo, hi = rng([r["restart_to_ready_seconds"] for r in rs])
            out.append(f"| {keys:,} | {fmt(med([r['rdb_bytes'] for r in rs]))} | "
                       f"{fmt(med([r['save_seconds'] for r in rs]), 2)} | "
                       f"{fmt(med([r['restart_to_ready_seconds'] for r in rs]), 2)} [{fmt(lo, 2)}–{fmt(hi, 2)}] | "
                       f"{fmt(med([r['log_load_seconds'] for r in rs]), 2)} |")
    if sec["aof"]:
        rs = sec["aof"]
        lo, hi = rng([r["restart_to_ready_seconds"] for r in rs])
        out.append(f"\nAOF (1,000,000 keys, no rewrite): restart→ready median "
                   f"{fmt(med([r['restart_to_ready_seconds'] for r in rs]), 2)} s [{fmt(lo, 2)}–{fmt(hi, 2)}], "
                   f"logged load {fmt(med([r['log_load_seconds'] for r in rs]), 2)} s, n={len(rs)}.")
    if sec["proxy"]:
        rs = sec["proxy"]
        out.append(f"\nproxy-poc cold start to first PONG: median "
                   f"{fmt(med([r['proxy_cold_start_to_pong_seconds'] for r in rs]) * 1000, 1)} ms; "
                   f"proxy serving again after Valkey restart became ready: median "
                   f"{fmt(med([r['proxy_serving_after_valkey_ready_seconds'] for r in rs]) * 1000, 1)} ms "
                   f"[{', '.join(fmt(r['proxy_serving_after_valkey_ready_seconds'] * 1000, 1) for r in rs)}], n={len(rs)}.")
    if sec["failover"]:
        out.append("\n| path | n | ops completed (of 200,000 offered) | errors | timeouts | client connections lost | Valkey restart s |")
        out.append("|---|---|---|---|---|---|---|")
        for (via,), rs in group(sec["failover"], ["via"]).items():
            out.append(f"| {via} | {len(rs)} | {fmt(med([r['ops_completed'] for r in rs]))} | "
                       f"{fmt(med([r['errors'] for r in rs]))} | {fmt(med([r['timeouts'] for r in rs]))} | "
                       f"{fmt(med([r['conn_failures'] for r in rs]))} | "
                       f"{fmt(med([r['valkey_restart_seconds'] for r in rs]), 2)} |")


def validity(present):
    """Checks that hold for every run, reported as counts."""
    runs = mismatched = client_bound = 0
    gaps = []
    for name in present:
        if name == "recovery":
            continue
        for r in load(name):
            if "error" in r or r.get("tool") != "loadgen":
                continue
            runs += 1
            c = r["client"]
            # Every GET reply must be a bulk (hit) or nil (miss); anything else
            # means a reply reached the wrong request.
            if c["gets"] != c["hits"] + c["misses"]:
                mismatched += 1
            client_bound += bool(r["derived"]["client_saturated"])
            srv = r["derived"].get("server_measured_cmds_per_sec")
            if srv and c["ops_per_sec"] and r["workload_def"]["pipeline"] == 1:
                gaps.append(abs(srv - c["ops_per_sec"]) / c["ops_per_sec"])
    lines = [f"- loadgen runs checked: {runs}",
             f"- runs where a GET received a non-bulk, non-nil reply (reply misattribution): {mismatched}",
             f"- runs flagged client-bound (load generator > 170% CPU): {client_bound}"]
    if gaps:
        gaps.sort()
        lines.append(f"- client vs Valkey-side (INFO) throughput, absolute difference: median "
                     f"{100 * statistics.median(gaps):.1f}%, max {100 * gaps[-1]:.1f}% "
                     f"(n={len(gaps)}; open-loop and pipelined runs excluded)")
    return lines


TOPOS = ["valkey-1t", "valkey-io2", "valkey-2shard", "rust-proxy"]


def stats_by(records, keys, vcpu_hour):
    return {k: row_stats(rs, vcpu_hour) for k, rs in group(records, keys).items()}


def key_tables(vcpu_hour):
    """Compact cross-suite tables used verbatim in BENCHMARK_RESULTS.md."""
    out = []
    core = stats_by(load("core"), ["workload", "topo"], vcpu_hour)
    if core:
        out += ["### K1. Saturation throughput and CPU per operation, 50 connections",
                "",
                "ops/s median (server CPU µs/op; p99 µs). Best per row in bold.",
                "",
                "| workload | " + " | ".join(TOPOS) + " | proxy ÷ best native (throughput, CPU/op) |",
                "|---|" + "---|" * (len(TOPOS) + 1)]
        for wl in ("read90", "mixed50", "write90", "skew-read90"):
            row = {t: core.get((wl, t)) for t in TOPOS}
            best = max((v["ops"] for v in row.values() if v), default=0)
            cells = []
            for t in TOPOS:
                v = row[t]
                if not v:
                    cells.append("–")
                    continue
                c = f"{fmt(v['ops'])} ({fmt(v['us_op'], 1)}; {fmt(v['p99'])})"
                cells.append(f"**{c}**" if v["ops"] == best else c)
            nat = max((row[t] for t in ("valkey-io2", "valkey-2shard") if row[t]), key=lambda v: v["ops"])
            px = row["rust-proxy"]
            out.append(f"| {wl} | " + " | ".join(cells) +
                       f" | {px['ops'] / nat['ops']:.2f}×, {px['us_op'] / nat['us_op']:.2f}× |")
        out.append("")
    conns = stats_by(load("conns"), ["conns", "topo"], vcpu_hour)
    if conns:
        merged = dict(conns)
        for t in TOPOS:
            if ("read90", t) in core:
                merged[(50, t)] = core[("read90", t)]
        out += ["### K2. Concurrency sweep, read90, closed loop",
                "",
                "ops/s median (server CPU µs/op; p50 µs). The 50-connection row is the core suite's read90.",
                "",
                "| connections | " + " | ".join(TOPOS) + " | proxy ÷ best native (throughput) |",
                "|---|" + "---|" * (len(TOPOS) + 1)]
        for c in sorted({k[0] for k in merged}):
            cells = []
            for t in TOPOS:
                v = merged.get((c, t))
                cells.append(f"{fmt(v['ops'])} ({fmt(v['us_op'], 1)}; {fmt(v['p50'])})" if v else "n/a")
            nats = [merged[(c, t)] for t in ("valkey-io2", "valkey-2shard") if (c, t) in merged]
            px = merged.get((c, "rust-proxy"))
            ratio = f"{px['ops'] / max(n['ops'] for n in nats):.2f}×" if px and nats else "–"
            out.append(f"| {c} | " + " | ".join(cells) + f" | {ratio} |")
        out.append("")
    ol = stats_by(load("openloop"), ["rate", "topo"], vcpu_hour)
    if ol:
        out += ["### K3. Latency at fixed offered load (open loop), read90, 50 connections",
                "",
                "p50 / p99 / p99.9 µs (server CPU µs/op).",
                "",
                "| offered ops/s | " + " | ".join(TOPOS) + " |",
                "|---|" + "---|" * len(TOPOS)]
        for r in sorted({k[0] for k in ol}):
            cells = []
            for t in TOPOS:
                v = ol.get((r, t))
                cells.append(f"{fmt(v['p50'])} / {fmt(v['p99'])} / {fmt(v['p999'])} ({fmt(v['us_op'], 1)})" if v else "–")
            out.append(f"| {r:,} | " + " | ".join(cells) + " |")
        out.append("")
    other = []
    for suite_name, wl, label in (("large", "large-read90", "16 KiB values, read90"),
                                  ("ttl", "ttl-mixed50", "SET EX 5, mixed50"),
                                  ("cold", "cold-read90", "cold cache, read90"),
                                  ("pipeline", "read90-p16", "client pipeline 16, read90"),
                                  ("aof", "read90-aof", "AOF everysec, read90"),
                                  ("aof", "write90-aof", "AOF everysec, write90"),
                                  ("aof2", "read90-aof", "AOF everysec, read90"),
                                  ("aof2", "write90-aof", "AOF everysec, write90"),
                                  ("aof2", "write90-aof-p16", "AOF everysec, write90, pipeline 16"),
                                  ("evict", "evict-uniform", "maxmemory 256mb LRU, uniform"),
                                  ("evict", "evict-skew", "maxmemory 256mb LRU, Zipf 0.99"),
                                  ("churn", "get100-churn", "new connection per request, GET")):
        st = stats_by([r for r in load(suite_name) if r.get("workload") == wl], ["topo"], vcpu_hour)
        for (t,), v in st.items():
            other.append((label, t, v))
    if other:
        out += ["### K4. Other scenarios, 50 connections",
                "",
                "| scenario | topology | ops/s median [min–max] | server CPU µs/op | p50 µs | p99 µs | hit rate |",
                "|---|---|---|---|---|---|---|"]
        for label, t, v in other:
            out.append(f"| {label} | {t} | {fmt(v['ops'])} [{fmt(v['lo'])}–{fmt(v['hi'])}] | {fmt(v['us_op'], 1)} | "
                       f"{fmt(v['p50'])} | {fmt(v['p99'])} | {fmt(v['hit'], 3) if v['hit'] is not None else '–'} |")
        out.append("")
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--vcpu-hour", type=float, default=0.05,
                    help="assumed price per vCPU-hour in USD (illustrative, not a quote)")
    args = ap.parse_args()
    out = ["# Benchmark summary (generated by bench/summarize.py)", "",
           f"Assumed price for $/Bop (USD per billion operations, 2 provisioned vCPUs): {args.vcpu_hour} USD "
           "per vCPU-hour. This is an illustrative parameter, not a quote; rerun with --vcpu-hour.", "",
           "CPU-s/Mop is CPU seconds per million operations (equal to CPU µs per operation). In the "
           "user/kernel table, multi-process topologies show the per-process median."]
    present = sorted(os.path.basename(p)[:-6] for p in glob.glob(os.path.join(RESULTS, "raw", "*.jsonl")))
    failures = []
    for name in present:
        for r in load(name):
            if "error" in r:
                why = r["error"]
                if r.get("topo") == "valkey-2shard" and r.get("conns", 2) < 2:
                    why = "not applicable: one connection cannot reach two shards (loadgen refuses it)"
                failures.append(f"{name}: {r.get('topo')} {r.get('workload')} c={r.get('conns')} "
                                f"t{r.get('trial')}: {why}")

    if "crosscheck" in present:
        out.append("## Load generator cross-check (GET only, valkey-1t, 50 connections)")
        crosscheck_table(out, load("crosscheck"))
    titles = {
        "core": ("## Core workloads (50 connections, closed loop)", ["topo", "workload"],
                 lambda s: f"workload {s[0]}"),
        "conns": ("## Concurrency sweep (read90)", ["topo", "conns"], lambda s: f"{s[0]} connections"),
        "openloop": ("## Fixed offered load (open loop, read90, 50 connections)", ["topo", "rate"],
                     lambda s: f"offered {s[0]:,} ops/s"),
        "large": ("## Large values (16 KiB, read90)", ["topo", "workload"], lambda s: f"workload {s[0]}"),
        "ttl": ("## Short-lived keys (SET EX 5, mixed50, starts empty)", ["topo", "workload"],
                lambda s: f"workload {s[0]}"),
        "pipeline": ("## Client-side pipelining (depth 16, read90)", ["topo", "workload"],
                     lambda s: f"workload {s[0]}"),
        "cold": ("## Cold cache (empty dataset, read90)", ["topo", "workload"], lambda s: f"workload {s[0]}"),
        "aof": ("## Persistence: AOF everysec", ["topo", "workload"], lambda s: f"workload {s[0]}"),
        "aof2": ("## Persistence: AOF everysec, Valkey-native comparisons", ["topo", "workload"],
                 lambda s: f"workload {s[0]}"),
        "evict": ("## Memory pressure: maxmemory 256 MB, allkeys-lru, 5M-key keyspace", ["topo", "workload"],
                  lambda s: f"workload {s[0]}"),
        "churn": ("## Connection churn (valkey-benchmark -k 0: new TCP connection per request, GET)",
                  ["topo", "workload"], lambda s: f"workload {s[0]}"),
    }
    for name, (title, keys, tfn) in titles.items():
        recs = load(name)
        if recs:
            out.append(f"\n{title}")
            suite_tables(out, name, recs, keys, args.vcpu_hour, tfn)
    core = load("core")
    if core:
        out.append("\n## User vs kernel CPU split (core suite, all workloads)")
        cpu_split_table(out, [r for r in core if "error" not in r])
    rec = load("recovery")
    if rec:
        out.append("\n## Memory, startup and recovery")
        recovery_tables(out, rec)
    out.append("\n## Validity checks across all load runs\n")
    out.extend(validity(present))
    out.append("\n## Failed runs\n")
    out.extend(f"- {f}" for f in failures) if failures else out.append("None.")
    with open(os.path.join(RESULTS, "key_tables.md"), "w") as f:
        f.write("# Key tables (generated by bench/summarize.py)\n\n" + "\n".join(key_tables(args.vcpu_hour)) + "\n")
    path = os.path.join(RESULTS, "summary.md")
    with open(path, "w") as f:
        f.write("\n".join(out) + "\n")
    print(f"wrote {path}")


if __name__ == "__main__":
    main()
