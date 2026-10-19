#!/usr/bin/env python3
"""Benchmark harness for the Rust-vs-Valkey assessment.

Starts each topology on fixed CPUs, preloads data, drives load, and records
client results together with per-process CPU, memory and server INFO deltas
as one JSON object per run (JSON Lines). Python stdlib only; Linux only.

Usage:
    bench/harness.py env                 # record the environment
    bench/harness.py run SUITE [SUITE..] # run suites, append to results/raw
    bench/harness.py list                # list suites

See ../BENCHMARK_PLAN.md for the methodology this implements.
"""

import argparse
import hashlib
import json
import os
import platform
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", ".."))
VALKEY_SERVER = os.path.join(REPO, "src", "valkey-server")
VALKEY_CLI = os.path.join(REPO, "src", "valkey-cli")
VALKEY_BENCH = os.path.join(REPO, "src", "valkey-benchmark")
RUST_BIN = os.path.join(HERE, "rust", "target", "release")
LOADGEN = os.path.join(RUST_BIN, "loadgen")
PROXY = os.path.join(RUST_BIN, "proxy-poc")
MAPMEM = os.path.join(RUST_BIN, "mapmem")
POC_BIN = os.path.join(HERE, "..", "poc", "target", "release")
RVK = os.path.join(POC_BIN, "rvk-server")
RSCLIENT = os.path.join(POC_BIN, "rsclient")
RESULTS = os.path.join(HERE, "..", "results")

# CPU layout for a 4-vCPU host: the data-service side (Valkey and, when
# present, the Rust proxy) gets CPUs 0-1; the load generator gets CPUs 2-3.
SERVER_CPUS = ["0", "1"]
CLIENT_CPUS = "2,3"
BASE_PORT = 6400
PROXY_PORT = 7000
CLK_TCK = os.sysconf("SC_CLK_TCK")


# --------------------------------------------------------------------------
# process helpers
# --------------------------------------------------------------------------

def cpu_ticks(pid):
    """(user, system) clock ticks for the whole process.

    The process-level stat includes threads that have already exited, unlike
    a sum over /proc/<pid>/task/*.
    """
    with open(f"/proc/{pid}/stat") as f:
        fields = f.read().rsplit(")", 1)[1].split()
    return int(fields[11]), int(fields[12])


def rss_bytes(pid):
    with open(f"/proc/{pid}/status") as f:
        for line in f:
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) * 1024
    return None


def cli(port, *args, check=True):
    r = subprocess.run([VALKEY_CLI, "-p", str(port), *map(str, args)],
                       capture_output=True, text=True, timeout=120)
    if check and r.returncode != 0:
        raise RuntimeError(f"valkey-cli {args} failed: {r.stderr}")
    return r.stdout.strip()


def info(port):
    out = {}
    for line in cli(port, "INFO", "everything").splitlines():
        if ":" in line and not line.startswith("#"):
            k, v = line.split(":", 1)
            out[k] = v.strip()
    return out


def wait_ping(port, timeout=30.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        r = subprocess.run([VALKEY_CLI, "-p", str(port), "PING"], capture_output=True, text=True)
        if r.stdout.strip() == "PONG":
            return
        time.sleep(0.02)
    raise RuntimeError(f"port {port} did not answer PING")


class Valkey:
    kind = "valkey"

    def __init__(self, port, cpus, workdir, args):
        self.port = port
        self.log = os.path.join(workdir, f"valkey-{port}.log")
        d = os.path.join(workdir, f"data-{port}")
        os.makedirs(d, exist_ok=True)
        cmd = ["taskset", "-c", cpus, VALKEY_SERVER, "--port", str(port), "--bind", "127.0.0.1",
               "--save", "", "--appendonly", "no", "--dir", d, "--logfile", self.log,
               "--maxclients", "10000", *map(str, args)]
        self.proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        wait_ping(port)

    @property
    def pid(self):
        return self.proc.pid

    def stop(self):
        if self.proc.poll() is None:
            subprocess.run([VALKEY_CLI, "-p", str(self.port), "SHUTDOWN", "NOSAVE"],
                           capture_output=True, timeout=30)
            try:
                self.proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()


class Rvk(Valkey):
    """rvk-server (Candidate A). It accepts the flags the harness passes to valkey-server and stops on SHUTDOWN."""
    kind = "rvk"

    def __init__(self, port, cpus, threads, workdir, args):
        self.port = port
        self.log = os.path.join(workdir, f"rvk-{port}.log")
        with open(self.log, "w") as logf:
            cmd = ["taskset", "-c", cpus, RVK, "--port", str(port), "--bind", "127.0.0.1",
                   "--threads", str(threads), "--maxclients", "10000", *map(str, args)]
            self.proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=logf)
        wait_ping(port)


class Proxy:
    def __init__(self, cpus, upstream_port, threads, upstream_conns, workdir):
        self.port = PROXY_PORT
        self.log = os.path.join(workdir, "proxy.log")
        self.logf = open(self.log, "w")
        cmd = ["taskset", "-c", cpus, PROXY, "--listen", f"127.0.0.1:{PROXY_PORT}",
               "--upstream", f"127.0.0.1:{upstream_port}", "--threads", str(threads),
               "--upstream-conns", str(upstream_conns)]
        self.proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=self.logf)
        wait_ping(PROXY_PORT)

    @property
    def pid(self):
        return self.proc.pid

    def stop(self):
        stats = {}
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGINT)
            self.proc.wait(timeout=10)
        self.logf.close()
        with open(self.log) as f:
            lines = [l for l in f if l.startswith("proxy-stats")]
        if lines:
            for kv in lines[-1].split()[1:]:
                k, v = kv.split("=")
                stats[k] = float(v)
        return stats


# --------------------------------------------------------------------------
# topologies: every one is given CPUs 0-1 for the data-service side
# --------------------------------------------------------------------------

class Topology:
    name = ""
    description = ""

    def __init__(self, workdir, valkey_args):
        self.workdir = workdir
        self.valkey_args = list(valkey_args)
        self.valkeys = []
        self.proxy = None

    @property
    def data_ports(self):
        return [v.port for v in self.valkeys]

    @property
    def client_ports(self):
        return [self.proxy.port] if self.proxy else self.data_ports

    def processes(self):
        out = {f"{v.kind}-{v.port}": v.pid for v in self.valkeys}
        if self.proxy:
            out["proxy"] = self.proxy.pid
        return out

    def stop(self):
        stats = self.proxy.stop() if self.proxy else {}
        for v in self.valkeys:
            v.stop()
        return stats


class ValkeySingleThread(Topology):
    name = "valkey-1t"
    description = "Valkey, io-threads 1 (default), pinned to CPU 0; CPU 1 idle"

    def start(self):
        self.valkeys = [Valkey(BASE_PORT, SERVER_CPUS[0], self.workdir, self.valkey_args)]


class ValkeyIOThreads(Topology):
    name = "valkey-io2"
    description = "Valkey, io-threads 2, on CPUs 0-1"

    def start(self):
        self.valkeys = [Valkey(BASE_PORT, ",".join(SERVER_CPUS), self.workdir,
                               ["--io-threads", "2", *self.valkey_args])]


class ValkeyTwoShards(Topology):
    name = "valkey-2shard"
    description = "Two single-threaded Valkey shards, one per CPU, client-side key routing"

    def start(self):
        self.valkeys = [Valkey(BASE_PORT + i, SERVER_CPUS[i], self.workdir, self.valkey_args)
                        for i in range(2)]


class RustProxy(Topology):
    name = "rust-proxy"
    description = ("Valkey io-threads 1 pinned to CPU 0 behind proxy-poc "
                   "(2 tokio workers on CPUs 0-1, 1 upstream connection)")

    def start(self):
        self.valkeys = [Valkey(BASE_PORT, SERVER_CPUS[0], self.workdir, self.valkey_args)]
        self.proxy = Proxy(",".join(SERVER_CPUS), BASE_PORT, threads=2, upstream_conns=1,
                           workdir=self.workdir)


class RvkSingleThread(Topology):
    name = "rvk-1t"
    description = "rvk-server, 1 worker thread, pinned to CPU 0; CPU 1 idle"

    def start(self):
        self.valkeys = [Rvk(BASE_PORT, SERVER_CPUS[0], 1, self.workdir, self.valkey_args)]


class RvkTwoThreads(Topology):
    name = "rvk-2t"
    description = "rvk-server, 2 worker threads (lock-sharded keyspace), on CPUs 0-1"

    def start(self):
        self.valkeys = [Rvk(BASE_PORT, ",".join(SERVER_CPUS), 2, self.workdir, self.valkey_args)]


TOPOLOGIES = {t.name: t for t in (ValkeySingleThread, ValkeyIOThreads, ValkeyTwoShards, RustProxy,
                                  RvkSingleThread, RvkTwoThreads)}


# --------------------------------------------------------------------------
# workloads
# --------------------------------------------------------------------------

def workload(name, get_ratio, value_size=100, keyspace=1_000_000, zipf=0.0, ttl=0,
             pipeline=1, preload=True, valkey_args=(), evicting=False, keepalive=True, mux=0, cache=0):
    """mux and cache apply to rsclient only: logical clients share `mux` sockets (0 = one each) and a
    client-side cache of `cache` entries (0 = none)."""
    wl = dict(name=name, get_ratio=get_ratio, value_size=value_size, keyspace=keyspace,
              zipf=zipf, ttl=ttl, pipeline=pipeline, preload=preload,
              valkey_args=list(valkey_args), evicting=evicting, keepalive=keepalive)
    if mux or cache:
        wl.update(mux=mux, cache=cache)
    return wl


W = {
    "get100": workload("get100", 1.0),
    # valkey-benchmark -k 0: a new TCP connection for every request.
    "get100-churn": workload("get100-churn", 1.0, keepalive=False),
    "read90": workload("read90", 0.9),
    "mixed50": workload("mixed50", 0.5),
    "write90": workload("write90", 0.1),
    "skew-read90": workload("skew-read90", 0.9, zipf=0.99),
    "large-read90": workload("large-read90", 0.9, value_size=16384, keyspace=100_000),
    # Starts empty; with a 5 s TTL the live set reaches steady state during warmup.
    "ttl-mixed50": workload("ttl-mixed50", 0.5, ttl=5, preload=False),
    "read90-p16": workload("read90-p16", 0.9, pipeline=16),
    "read90-aof": workload("read90-aof", 0.9, valkey_args=["--appendonly", "yes", "--appendfsync", "everysec"]),
    "write90-aof": workload("write90-aof", 0.1, valkey_args=["--appendonly", "yes", "--appendfsync", "everysec"]),
    "write90-aof-p16": workload("write90-aof-p16", 0.1, pipeline=16,
                                valkey_args=["--appendonly", "yes", "--appendfsync", "everysec"]),
    # Preloading 5M keys into 256 MB fills memory first, so the run measures
    # steady-state eviction rather than the fill phase.
    "evict-uniform": workload("evict-uniform", 0.5, keyspace=5_000_000, evicting=True,
                              valkey_args=["--maxmemory", "256mb", "--maxmemory-policy", "allkeys-lru"]),
    "evict-skew": workload("evict-skew", 0.5, keyspace=5_000_000, zipf=0.99, evicting=True,
                           valkey_args=["--maxmemory", "256mb", "--maxmemory-policy", "allkeys-lru"]),
    "cold-read90": workload("cold-read90", 0.9, preload=False),
    # Part II
    "mixed50-p16": workload("mixed50-p16", 0.5, pipeline=16),
    "write90-p16": workload("write90-p16", 0.1, pipeline=16),
    "skew-read90-p16": workload("skew-read90-p16", 0.9, zipf=0.99, pipeline=16),
    # rsclient (redis-rs): one socket per logical client, all multiplexed on one socket, and that plus a
    # client-side cache sized at 10% of the keyspace.
    "rs-read90": workload("rs-read90", 0.9),
    "rs-read90-mux1": workload("rs-read90-mux1", 0.9, mux=1),
    "rs-skew-read90": workload("rs-skew-read90", 0.9, zipf=0.99),
    "rs-skew-read90-mux1": workload("rs-skew-read90-mux1", 0.9, zipf=0.99, mux=1),
    "rs-skew-read90-mux1-cache": workload("rs-skew-read90-mux1-cache", 0.9, zipf=0.99, mux=1, cache=100_000),
}

ALL_TOPOS = ["valkey-1t", "valkey-io2", "valkey-2shard", "rust-proxy"]
P2_TOPOS = ["valkey-1t", "valkey-io2", "valkey-2shard", "rvk-1t", "rvk-2t"]
P2_ONE_ENDPOINT = ["valkey-1t", "valkey-io2", "rvk-1t", "rvk-2t"]


def suite(topos, workloads, conns=(50,), rates=(0,), trials=3, duration=15, warmup=3, tool="loadgen"):
    return [dict(topo=t, workload=w, conns=c, rate=r, trial=i, duration=duration, warmup=warmup, tool=tool)
            for w in workloads for c in conns for r in rates for t in topos for i in range(trials)]


SUITES = {
    "smoke": suite(ALL_TOPOS, ["read90"], trials=1, duration=4, warmup=1),
    "core": suite(ALL_TOPOS, ["read90", "mixed50", "write90", "skew-read90"]),
    "conns": suite(ALL_TOPOS, ["read90"], conns=(1, 10, 200, 1000)),
    "large": suite(ALL_TOPOS, ["large-read90"]),
    "ttl": suite(ALL_TOPOS, ["ttl-mixed50"], warmup=8),
    "cold": suite(["valkey-1t", "rust-proxy"], ["cold-read90"]),
    "pipeline": suite(["valkey-1t", "valkey-2shard", "rust-proxy"], ["read90-p16"]),
    "openloop": suite(ALL_TOPOS, ["read90"], rates=(20_000, 40_000, 60_000)),
    "aof": suite(["valkey-1t", "rust-proxy"], ["read90-aof", "write90-aof"]),
    # Added after the first AOF runs: the same workloads on the Valkey-native
    # 2-CPU topology, and client pipelining with AOF.
    "aof2": suite(["valkey-2shard"], ["read90-aof", "write90-aof"])
            + suite(["valkey-1t"], ["write90-aof-p16"]),
    "evict": suite(["valkey-1t"], ["evict-uniform", "evict-skew"]),
    # valkey-benchmark drives one endpoint, so the two-shard topology is not run here.
    "churn": suite(["valkey-1t", "valkey-io2", "rust-proxy"], ["get100-churn"], tool="valkey-benchmark"),
}

# Part II (BENCHMARK_PLAN.md, Part II). Results go to results/raw2 so Part I raw data stays untouched.
CORE = ["read90", "mixed50", "write90", "skew-read90"]
SUITES.update({
    "p2-smoke": suite(P2_TOPOS + ["rust-proxy"], ["read90"], trials=1, duration=4, warmup=1)
                + suite(["valkey-1t", "rvk-2t"], ["rs-read90-mux1"], trials=1, duration=4, warmup=1, tool="rsclient"),
    "p2-core": suite(P2_TOPOS, CORE),
    "p2-core-p16": suite(P2_TOPOS, [w + "-p16" for w in CORE]),
    "p2-proxy": suite(["rust-proxy"], ["read90"]),
    "p2-conns": suite([t for t in P2_TOPOS if t != "valkey-2shard"], ["read90"], conns=(1,))
                + suite(P2_TOPOS, ["read90"], conns=(10, 200, 1000))
                + suite(P2_TOPOS, ["read90-p16"], conns=(200,)),
    "p2-large": suite(P2_TOPOS, ["large-read90"]),
    "p2-ttl": suite(P2_TOPOS, ["ttl-mixed50"], warmup=8),
    # Precise pacing (loadgen --spin-pacing), plus timer pacing as in Part I for comparison at one rate.
    "p2-openloop": [dict(s, pacing="spin") for s in suite(P2_TOPOS, ["read90"], rates=(20_000, 60_000, 100_000))]
                   + suite(["valkey-1t", "rvk-2t"], ["read90"], rates=(60_000,)),
    "p2-evict": suite(["valkey-1t", "rvk-1t", "rvk-2t"], ["evict-uniform", "evict-skew"]),
    "p2-cold": suite(P2_TOPOS, ["cold-read90"]),
    "p2-churn": suite(P2_ONE_ENDPOINT, ["get100-churn"], tool="valkey-benchmark"),
    "p2-crosscheck": suite(["valkey-1t", "rvk-1t", "rvk-2t"], ["get100"], tool="valkey-benchmark")
                     + suite(["valkey-1t", "rvk-1t", "rvk-2t"], ["get100"]),
    "p2-clients": suite(["valkey-1t", "valkey-io2", "rvk-2t"],
                        ["rs-read90", "rs-read90-mux1", "rs-skew-read90", "rs-skew-read90-mux1"], tool="rsclient")
                  + suite(["valkey-1t", "valkey-io2"], ["rs-skew-read90-mux1-cache"], tool="rsclient"),
})


# --------------------------------------------------------------------------
# running
# --------------------------------------------------------------------------

def preload(topo, wl):
    cmd = ["taskset", "-c", CLIENT_CPUS, LOADGEN, "--preload",
           "--port", ",".join(map(str, topo.data_ports)), "--conns", "8", "--threads", "2",
           "--keyspace", str(wl["keyspace"]), "--value-size", str(wl["value_size"]), "--pipeline", "64"]
    r = subprocess.run(cmd, capture_output=True, text=True, check=True)
    res = json.loads(r.stdout)
    keys = sum(int(cli(p, "DBSIZE")) for p in topo.data_ports)
    res["dbsize_after"] = keys
    if res["errors"] or (keys != wl["keyspace"] and not wl["evicting"]):
        raise RuntimeError(f"preload mismatch: {res} dbsize={keys}")
    return res


def sample(pids):
    return time.monotonic(), {name: cpu_ticks(pid) for name, pid in pids.items()}


def cpu_delta(a, b):
    secs = b[0] - a[0]
    out = {}
    for name in b[1]:
        u = (b[1][name][0] - a[1][name][0]) / CLK_TCK
        s = (b[1][name][1] - a[1][name][1]) / CLK_TCK
        out[name] = {"user_pct": round(100 * u / secs, 1), "sys_pct": round(100 * s / secs, 1),
                     "cpu_sec": round(u + s, 3)}
    return secs, out


INFO_COUNTERS = ["total_commands_processed", "total_error_replies", "keyspace_hits", "keyspace_misses",
                 "expired_keys", "evicted_keys", "total_net_input_bytes", "total_net_output_bytes",
                 "rejected_connections", "io_threaded_reads_processed", "io_threaded_writes_processed"]
INFO_GAUGES = ["used_memory", "used_memory_rss", "mem_fragmentation_ratio", "connected_clients",
               "db0", "aof_enabled"]


def loadgen_cmd(spec, wl, ports, tool):
    if tool == "rsclient":
        if len(ports) != 1:
            raise RuntimeError("rsclient runs need one endpoint")
        return ["taskset", "-c", CLIENT_CPUS, RSCLIENT, "--port", str(ports[0]), "--tasks", str(spec["conns"]),
                "--conns", str(wl.get("mux") or spec["conns"]), "--threads", "2",
                "--duration", str(spec["duration"]), "--warmup", str(spec["warmup"]),
                "--keyspace", str(wl["keyspace"]), "--zipf", str(wl["zipf"]), "--get-ratio", str(wl["get_ratio"]),
                "--value-size", str(wl["value_size"]), "--cache", str(wl.get("cache", 0)),
                "--label", f'{spec["topo"]}/{wl["name"]}']
    if tool == "valkey-benchmark":
        if len(ports) != 1 or wl["get_ratio"] != 1.0:
            raise RuntimeError("valkey-benchmark runs need one endpoint and a GET-only workload")
        return ["taskset", "-c", CLIENT_CPUS, VALKEY_BENCH, "-p", str(ports[0]), "-c", str(spec["conns"]),
                "--threads", "2", "--duration", str(spec["duration"]), "--warmup", str(spec["warmup"]),
                "-r", str(wl["keyspace"]), "-d", str(wl["value_size"]), "-P", str(wl["pipeline"]),
                "--precision", "3", "-k", "1" if wl["keepalive"] else "0", "-t", "get"]
    cmd = ["taskset", "-c", CLIENT_CPUS, LOADGEN, "--port", ",".join(map(str, ports)),
           "--conns", str(spec["conns"]), "--threads", "2", "--duration", str(spec["duration"]),
           "--warmup", str(spec["warmup"]), "--pipeline", str(wl["pipeline"]),
           "--keyspace", str(wl["keyspace"]), "--zipf", str(wl["zipf"]),
           "--get-ratio", str(wl["get_ratio"]), "--value-size", str(wl["value_size"]),
           "--ttl", str(wl["ttl"]), "--seed", str(1000 + spec["trial"]),
           "--label", f'{spec["topo"]}/{wl["name"]}']
    if spec["rate"]:
        cmd += ["--rate", str(spec["rate"])]
        if spec.get("pacing") == "spin":
            cmd += ["--spin-pacing"]
    return cmd


def parse_valkey_benchmark(text):
    """Extracts throughput and percentiles from valkey-benchmark's full output."""
    text = text.replace("\r", "\n")
    m = re.search(r"throughput summary: ([\d.]+) requests per second", text)
    s = re.search(r"avg\s+min\s+p50\s+p95\s+p99\s+max\s*\n\s*([\d.\s]+)\n", text)
    avg, mn, p50, p95, p99, mx = map(float, s.group(1).split())
    # The percentile table is HDR-bucketed; take the first row at or above
    # 99.9% as an upper bound for p99.9.
    p999 = None
    for pct, ms in re.findall(r"([\d.]+)% <= ([\d.]+) milliseconds", text.split("Cumulative distribution")[0]):
        if float(pct) >= 99.9:
            p999 = float(ms)
            break
    us = lambda v: round(v * 1000, 2)
    return {"tool": "valkey-benchmark", "ops_per_sec": float(m.group(1)),
            "lat_us": {"mean": us(avg), "p50": us(p50), "p95": us(p95), "p99": us(p99),
                       "p999_upper": us(p999) if p999 else None, "max": us(mx)},
            "errors": None}


def run_one(spec, out, tool="loadgen"):
    wl = dict(W[spec["workload"]])
    workdir = tempfile.mkdtemp(prefix="vbench-")
    topo = TOPOLOGIES[spec["topo"]](workdir, wl["valkey_args"])
    rec = {"ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), **spec, "tool": tool,
           "topo_desc": topo.description, "workload_def": wl}
    try:
        topo.start()
        if wl["preload"]:
            rec["preload"] = preload(topo, wl)
        for p in topo.data_ports:
            cli(p, "CONFIG", "RESETSTAT")
        info_a = {p: info(p) for p in topo.data_ports}
        lg = subprocess.Popen(loadgen_cmd(spec, wl, topo.client_ports, tool),
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        pids = {**topo.processes(), "client": lg.pid}
        # loadgen opens connections for this long before its warmup starts.
        grace = 0.0 if tool == "valkey-benchmark" else 0.5 + spec["conns"] / 2000.0
        time.sleep(grace + spec["warmup"] + 0.25)
        a = sample(pids)
        cmds_a = sum(int(info(p)["total_commands_processed"]) for p in topo.data_ports)
        time.sleep(spec["duration"] - 0.5)
        b = sample(pids)
        cmds_b = sum(int(info(p)["total_commands_processed"]) for p in topo.data_ports)
        rss = {name: rss_bytes(pid) for name, pid in pids.items()}
        stdout, stderr = lg.communicate(timeout=spec["duration"] + 120)
        if lg.returncode != 0:
            raise RuntimeError(f"load generator failed: {stderr[-2000:]}")
        client = parse_valkey_benchmark(stdout) if tool == "valkey-benchmark" else json.loads(stdout)
        info_b = {p: info(p) for p in topo.data_ports}
        window, cpu = cpu_delta(a, b)

        server = {}
        for k in INFO_COUNTERS:
            server[k] = sum(float(info_b[p].get(k, 0)) - float(info_a[p].get(k, 0)) for p in topo.data_ports)
        for p in topo.data_ports:
            server[f"gauges-{p}"] = {k: info_b[p].get(k) for k in INFO_GAUGES}
        server_side = [n for n in cpu if n != "client"]
        server_cpu_pct = sum(cpu[n]["user_pct"] + cpu[n]["sys_pct"] for n in server_side)
        client_cpu_pct = cpu["client"]["user_pct"] + cpu["client"]["sys_pct"]
        ops = client["ops_per_sec"]
        rec.update({
            "client": client,
            "cpu_window_sec": round(window, 3),
            "cpu": cpu,
            "rss_bytes": rss,
            "server": server,
            "derived": {
                "server_side_cpu_pct": round(server_cpu_pct, 1),
                "client_cpu_pct": round(client_cpu_pct, 1),
                "server_side_cpu_us_per_op": round(server_cpu_pct / 100 / ops * 1e6, 3) if ops else None,
                "total_cpu_us_per_op": round((server_cpu_pct + client_cpu_pct) / 100 / ops * 1e6, 3) if ops else None,
                "client_saturated": client_cpu_pct > 170.0,
                # Independent of the load generator; includes the INFO calls.
                "server_measured_cmds_per_sec": round((cmds_b - cmds_a) / window, 1),
                "server_side_rss_bytes": sum(v for n, v in rss.items() if n != "client" and v),
            },
        })
    except Exception as e:  # keep the suite going; the failure is recorded
        rec["error"] = repr(e)
    finally:
        try:
            rec["proxy_stats"] = topo.stop()
        except Exception as e:
            rec.setdefault("error", repr(e))
        shutil.rmtree(workdir, ignore_errors=True)
    out.write(json.dumps(rec) + "\n")
    out.flush()
    return rec


def brief(rec):
    if "error" in rec:
        return f'ERROR {rec["error"]}'
    c = rec["client"]
    d = rec["derived"]
    lat = c["lat_us"]
    return (f'{c["ops_per_sec"]:>10.0f} ops/s  p50={lat["p50"]:.0f}us p99={lat["p99"]:.0f}us  '
            f'server_cpu={d["server_side_cpu_pct"]:.0f}% client_cpu={d["client_cpu_pct"]:.0f}%  '
            f'{d["server_side_cpu_us_per_op"]}us/op  err={c.get("errors")}')


def raw_dir(suite_name):
    return os.path.join(RESULTS, "raw2" if suite_name.startswith("p2-") else "raw")


def cmd_run(args):
    for name in args.suites:
        specs = SUITES[name]
        os.makedirs(raw_dir(name), exist_ok=True)
        path = os.path.join(raw_dir(name), f"{name}.jsonl")
        print(f"== suite {name}: {len(specs)} runs -> {path}", flush=True)
        with open(path, "a") as out:
            for i, spec in enumerate(specs, 1):
                rec = run_one(spec, out, tool=spec.get("tool", "loadgen"))
                print(f'[{i}/{len(specs)}] {spec["topo"]:<14} {spec["workload"]:<14} c={spec["conns"]:<5} '
                      f'rate={spec["rate"]:<6} t{spec["trial"]}  {brief(rec)}', flush=True)


def cmd_crosscheck(args):
    """GET-only on valkey-1t with both load generators, for independent verification."""
    os.makedirs(os.path.join(RESULTS, "raw"), exist_ok=True)
    path = os.path.join(RESULTS, "raw", "crosscheck.jsonl")
    with open(path, "a") as out:
        for trial in range(3):
            for tool in ("valkey-benchmark", "loadgen"):
                spec = dict(topo="valkey-1t", workload="get100", conns=50, rate=0, trial=trial,
                            duration=15, warmup=3)
                rec = run_one(spec, out, tool=tool)
                print(f"crosscheck {tool:<16} t{trial} {brief(rec)}", flush=True)


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def sh(cmd):
    try:
        return subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=30).stdout.strip()
    except Exception as e:
        return repr(e)


def cmd_env(args):
    env = {
        "captured": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "uname": platform.uname()._asdict(),
        "os_release": sh("grep PRETTY_NAME /etc/os-release"),
        "lscpu": sh("lscpu"),
        "meminfo_total": sh("grep MemTotal /proc/meminfo"),
        "thp": sh("cat /sys/kernel/mm/transparent_hugepage/enabled"),
        "somaxconn": sh("cat /proc/sys/net/core/somaxconn"),
        "overcommit_memory": sh("cat /proc/sys/vm/overcommit_memory"),
        "nofile_limit": sh("ulimit -n"),
        "valkey_version": sh(f"{VALKEY_SERVER} --version"),
        "git_head": sh(f"git -C {REPO} rev-parse HEAD"),
        "valkey_build_cflags": sh(f"grep -E '^OPTIMIZATION' {REPO}/src/Makefile"),
        "gcc": sh("gcc --version | head -1"),
        "rustc": sh("rustc --version"),
        "cargo": sh("cargo --version"),
        "python": sys.version.split()[0],
        "binaries_sha256": {os.path.basename(p): sha256(p) for p in
                            (VALKEY_SERVER, VALKEY_BENCH, LOADGEN, PROXY, MAPMEM, RVK, RSCLIENT)
                            if os.path.exists(p)},
        "cpu_layout": {"server_side": SERVER_CPUS, "client": CLIENT_CPUS},
    }
    os.makedirs(RESULTS, exist_ok=True)
    # Part I's environment.json records the binaries its raw data came from; Part II gets its own file.
    name = "environment_part2.json" if args.part2 else "environment.json"
    with open(os.path.join(RESULTS, name), "w") as f:
        json.dump(env, f, indent=2)
    print(json.dumps({k: v for k, v in env.items() if k != "lscpu"}, indent=2))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("suites", nargs="+", choices=sorted(SUITES))
    sub.add_parser("crosscheck")
    e = sub.add_parser("env")
    e.add_argument("--part2", action="store_true")
    sub.add_parser("list")
    args = ap.parse_args()
    if args.cmd == "run":
        cmd_run(args)
    elif args.cmd == "crosscheck":
        cmd_crosscheck(args)
    elif args.cmd == "env":
        cmd_env(args)
    else:
        for k, v in SUITES.items():
            print(f"{k:<10} {len(v):>4} runs")


if __name__ == "__main__":
    main()
