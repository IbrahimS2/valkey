#!/usr/bin/env python3
"""Startup, recovery and memory-overhead measurements (run after harness.py).

Sections (each appends JSON lines to ../results/raw/recovery.jsonl):
  memory   - Valkey used_memory/RSS per key vs. mapmem (naive Rust map)
  rdb      - RDB save size/time and load time on restart
  aof      - AOF (no rewrite) load time on restart
  proxy    - proxy cold start, and time to serve again after Valkey restarts
  failover - in-flight request outcome when Valkey is killed under load,
             direct vs. through the proxy

Usage: bench/recovery.py [section ...]   (default: all)
"""

import json
import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import harness as h  # noqa: E402

OUT = os.path.join(h.RESULTS, "raw", "recovery.jsonl")
TRIALS = 3


def emit(out, rec):
    rec["ts"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    out.write(json.dumps(rec) + "\n")
    out.flush()
    print(json.dumps(rec), flush=True)


def used_memory(port):
    i = h.info(port)
    return int(i["used_memory"]), int(i["used_memory_rss"])


def preload(port, keys, value_size, ports=None):
    cmd = ["taskset", "-c", h.CLIENT_CPUS, h.LOADGEN, "--preload", "--port", str(port), "--conns", "8",
           "--threads", "2", "--keyspace", str(keys), "--value-size", str(value_size), "--pipeline", "64"]
    return json.loads(subprocess.run(cmd, capture_output=True, text=True, check=True).stdout)


def section_memory(out):
    for keys, vsize in ((1_000_000, 100), (100_000, 16384)):
        for trial in range(TRIALS):
            wd = tempfile.mkdtemp(prefix="vrec-")
            v = h.Valkey(h.BASE_PORT, h.SERVER_CPUS[0], wd, [])
            try:
                m0, r0 = used_memory(v.port)
                preload(v.port, keys, vsize)
                time.sleep(1.0)
                m1, r1 = used_memory(v.port)
                emit(out, {"section": "memory", "impl": "valkey", "keys": keys, "value_size": vsize, "trial": trial,
                           "used_memory_per_key": round((m1 - m0) / keys, 1),
                           "rss_per_key": round((r1 - r0) / keys, 1),
                           "payload_per_key": 16 + vsize})
            finally:
                v.stop()
                shutil.rmtree(wd, ignore_errors=True)
            r = subprocess.run(["taskset", "-c", h.SERVER_CPUS[0], h.MAPMEM, str(keys), str(vsize)],
                               capture_output=True, text=True, check=True)
            m = json.loads(r.stdout)
            emit(out, {"section": "memory", "impl": "rust-naive-hashmap", "keys": keys, "value_size": vsize,
                       "trial": trial, "rss_per_key": m["bytes_per_key"], "payload_per_key": m["payload_bytes_per_key"]})


def restart_and_time(wd, port, args):
    """Starts Valkey on an existing data dir; returns (seconds to PONG and loading:0, log load time)."""
    t0 = time.monotonic()
    v = h.Valkey(port, h.SERVER_CPUS[0], wd, args)
    while h.info(port).get("loading") != "0":
        time.sleep(0.005)
    ready = time.monotonic() - t0
    with open(v.log) as f:
        m = re.findall(r"(?:DB loaded from (?:disk|append only file|base file)[^:]*): ([\d.]+) seconds", f.read())
    return v, ready, (float(m[-1]) if m else None)


def section_rdb(out):
    for keys in (1_000_000, 5_000_000):
        for trial in range(TRIALS):
            wd = tempfile.mkdtemp(prefix="vrec-")
            v = h.Valkey(h.BASE_PORT, h.SERVER_CPUS[0], wd, [])
            try:
                preload(v.port, keys, 100)
                t0 = time.monotonic()
                h.cli(v.port, "SAVE")
                save_s = time.monotonic() - t0
                size = os.path.getsize(os.path.join(wd, f"data-{v.port}", "dump.rdb"))
                v.stop()
                v, ready, logged = restart_and_time(wd, h.BASE_PORT, ["--dbfilename", "dump.rdb"])
                dbsize = int(h.cli(v.port, "DBSIZE"))
                emit(out, {"section": "rdb", "keys": keys, "value_size": 100, "trial": trial,
                           "save_seconds": round(save_s, 3), "rdb_bytes": size,
                           "restart_to_ready_seconds": round(ready, 3), "log_load_seconds": logged,
                           "dbsize_after": dbsize})
            finally:
                v.stop()
                shutil.rmtree(wd, ignore_errors=True)


def section_aof(out):
    keys = 1_000_000
    args = ["--appendonly", "yes", "--appendfsync", "everysec", "--auto-aof-rewrite-percentage", "0"]
    for trial in range(TRIALS):
        wd = tempfile.mkdtemp(prefix="vrec-")
        v = h.Valkey(h.BASE_PORT, h.SERVER_CPUS[0], wd, args)
        try:
            preload(v.port, keys, 100)
            time.sleep(1.5)  # let everysec fsync complete
            v.stop()
            v, ready, logged = restart_and_time(wd, h.BASE_PORT, args)
            emit(out, {"section": "aof", "keys": keys, "value_size": 100, "trial": trial,
                       "restart_to_ready_seconds": round(ready, 3), "log_load_seconds": logged,
                       "dbsize_after": int(h.cli(v.port, "DBSIZE"))})
        finally:
            v.stop()
            shutil.rmtree(wd, ignore_errors=True)


def ping_until_ok(port, timeout=30.0):
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        r = subprocess.run([h.VALKEY_CLI, "-p", str(port), "PING"], capture_output=True, text=True)
        if r.stdout.strip() == "PONG":
            return time.monotonic() - t0
        time.sleep(0.002)
    return None


def section_proxy(out):
    for trial in range(TRIALS):
        wd = tempfile.mkdtemp(prefix="vrec-")
        v = h.Valkey(h.BASE_PORT, h.SERVER_CPUS[0], wd, [])
        p = None
        try:
            t0 = time.monotonic()
            p = h.Proxy(",".join(h.SERVER_CPUS), h.BASE_PORT, 2, 1, wd)
            cold = time.monotonic() - t0
            # Restart Valkey behind a running proxy and measure how long after
            # Valkey is ready the proxy serves requests again.
            v.stop()
            v = h.Valkey(h.BASE_PORT, h.SERVER_CPUS[0], wd, [])
            extra = ping_until_ok(h.PROXY_PORT)
            emit(out, {"section": "proxy", "trial": trial, "proxy_cold_start_to_pong_seconds": round(cold, 4),
                       "proxy_serving_after_valkey_ready_seconds": round(extra, 4) if extra is not None else None})
        finally:
            if p:
                p.stop()
            v.stop()
            shutil.rmtree(wd, ignore_errors=True)


def section_failover(out):
    """Kill Valkey (SIGKILL) 4 s into a 10 s open-loop run, restart it at once."""
    for via in ("direct", "proxy"):
        for trial in range(TRIALS):
            wd = tempfile.mkdtemp(prefix="vrec-")
            v = h.Valkey(h.BASE_PORT, h.SERVER_CPUS[0], wd, [])
            p = None
            try:
                preload(v.port, 100_000, 100)
                port = h.BASE_PORT
                if via == "proxy":
                    p = h.Proxy(",".join(h.SERVER_CPUS), h.BASE_PORT, 2, 1, wd)
                    port = h.PROXY_PORT
                lg = subprocess.Popen(["taskset", "-c", h.CLIENT_CPUS, h.LOADGEN, "--port", str(port),
                                       "--conns", "50", "--threads", "2", "--duration", "10", "--warmup", "0",
                                       "--keyspace", "100000", "--get-ratio", "0.9", "--rate", "20000",
                                       "--timeout-ms", "2000"],
                                      stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                time.sleep(0.5 + 4.0)
                v.proc.send_signal(signal.SIGKILL)
                v.proc.wait()
                t_kill = time.monotonic()
                v = h.Valkey(h.BASE_PORT, h.SERVER_CPUS[0], wd, [])
                valkey_back = time.monotonic() - t_kill
                stdout, stderr = lg.communicate(timeout=60)
                res = json.loads(stdout)
                emit(out, {"section": "failover", "via": via, "trial": trial,
                           "valkey_restart_seconds": round(valkey_back, 3),
                           "ops_completed": res["ops"], "ops_target": 200_000, "errors": res["errors"],
                           "timeouts": res["timeouts"], "conn_failures": res["conn_failures"],
                           "hit_rate": res["hit_rate"],
                           "proxy_stats": p.stop() if p else None})
                p = None
            finally:
                if p:
                    p.stop()
                v.stop()
                shutil.rmtree(wd, ignore_errors=True)


SECTIONS = {"memory": section_memory, "rdb": section_rdb, "aof": section_aof, "proxy": section_proxy,
            "failover": section_failover}


def main():
    names = sys.argv[1:] or list(SECTIONS)
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "a") as out:
        for n in names:
            SECTIONS[n](out)


if __name__ == "__main__":
    main()
