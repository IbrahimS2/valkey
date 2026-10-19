//! RESP load generator used by the assessment benchmarks (benchmark tooling,
//! not production code).
//!
//! valkey-benchmark cannot mix GET/SET at a ratio without pipelining the mix,
//! cannot generate skewed (Zipf) key access, and reports p99.9 only as an HDR
//! bucket bound. This tool fills those gaps. Its GET-only results are
//! cross-checked against valkey-benchmark in BENCHMARK_RESULTS.md.
//!
//! Modes:
//! * closed loop (default): each connection sends `--pipeline` requests and
//!   waits for all replies before sending more.
//! * open loop (`--rate R`): connections send on a fixed schedule independent
//!   of replies, so latency is measured at a fixed offered load.
//! * preload (`--preload`): SET every key once, then exit.
//!
//! `--port A,B,...` treats each port as one shard: connection `c` talks to
//! port `c % n` and only uses keys with `index % n == c % n`, which is how a
//! cluster-aware client routes keys to their owning node.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;
use resp_common::{parse_reply_from, ReplyKind};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep_until, timeout};

const MAX_INFLIGHT_OPEN_LOOP: usize = 10_000;

#[derive(Clone, Debug)]
struct Config {
    host: String,
    ports: Vec<u16>,
    conns: usize,
    threads: usize,
    duration: f64,
    warmup: f64,
    pipeline: usize,
    keyspace: u64,
    zipf: f64,
    get_ratio: f64,
    value_size: usize,
    ttl: u64,
    rate: f64,
    timeout_ms: u64,
    seed: u64,
    label: String,
    preload: bool,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            host: "127.0.0.1".into(),
            ports: vec![6379],
            conns: 50,
            threads: 1,
            duration: 10.0,
            warmup: 2.0,
            pipeline: 1,
            keyspace: 1_000_000,
            zipf: 0.0,
            get_ratio: 0.9,
            value_size: 100,
            ttl: 0,
            rate: 0.0,
            timeout_ms: 5000,
            seed: 1,
            label: String::new(),
            preload: false,
        }
    }
}

fn usage() -> ! {
    eprintln!(
        "usage: loadgen [--host H] [--port P[,P...]] [--conns N] [--threads T] [--duration S]\n\
         \t[--warmup S] [--pipeline P] [--keyspace K] [--zipf THETA (0=uniform)]\n\
         \t[--get-ratio R] [--value-size B] [--ttl SECONDS] [--rate OPS_PER_SEC]\n\
         \t[--timeout-ms MS] [--seed N] [--label TEXT] [--preload]"
    );
    std::process::exit(2);
}

fn parse_args() -> Config {
    let mut c = Config::default();
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < args.len() {
        let k = args[i].as_str();
        if k == "--preload" {
            c.preload = true;
            i += 1;
            continue;
        }
        let v = args.get(i + 1).cloned().unwrap_or_else(|| usage());
        fn num<T: std::str::FromStr>(s: &str) -> T {
            s.parse().unwrap_or_else(|_| usage())
        }
        match k {
            "--host" => c.host = v,
            "--port" => c.ports = v.split(',').map(num).collect(),
            "--conns" => c.conns = num(&v),
            "--threads" => c.threads = num(&v),
            "--duration" => c.duration = num(&v),
            "--warmup" => c.warmup = num(&v),
            "--pipeline" => c.pipeline = num(&v),
            "--keyspace" => c.keyspace = num(&v),
            "--zipf" => c.zipf = num(&v),
            "--get-ratio" => c.get_ratio = num(&v),
            "--value-size" => c.value_size = num(&v),
            "--ttl" => c.ttl = num(&v),
            "--rate" => c.rate = num(&v),
            "--timeout-ms" => c.timeout_ms = num(&v),
            "--seed" => c.seed = num(&v),
            "--label" => c.label = v,
            _ => usage(),
        }
        i += 2;
    }
    if c.conns < c.ports.len() || c.ports.is_empty() || c.threads == 0 || c.pipeline == 0 || c.keyspace == 0 {
        usage();
    }
    if !(0.0..=1.0).contains(&c.get_ratio) {
        usage();
    }
    c.threads = c.threads.min(c.conns);
    c
}

/// SplitMix64: small, fast, and good enough for workload generation.
struct Rng(u64);

impl Rng {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 * (1.0 / (1u64 << 53) as f64)
    }
    fn below(&mut self, n: u64) -> u64 {
        ((self.next_u64() as u128 * n as u128) >> 64) as u64
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Op {
    Get,
    Set,
}

struct Shared {
    cfg: Config,
    /// Cumulative Zipf distribution over key ranks; rank 0 is the hottest key.
    zipf_cdf: Option<Vec<f64>>,
    set_suffix: Vec<u8>,
    start: Instant,
    measure_start: Instant,
    end: Instant,
}

impl Shared {
    fn sample_index(&self, rng: &mut Rng) -> u64 {
        match &self.zipf_cdf {
            None => rng.below(self.cfg.keyspace),
            Some(cdf) => {
                let u = rng.unit();
                (cdf.partition_point(|&c| c < u) as u64).min(self.cfg.keyspace - 1)
            }
        }
    }

    /// Draws from the global key distribution, keeping only keys owned by
    /// `shard` so per-shard traffic follows the same distribution.
    fn key_index(&self, rng: &mut Rng, shard: usize) -> u64 {
        let n = self.cfg.ports.len() as u64;
        loop {
            let idx = self.sample_index(rng);
            if n == 1 || idx % n == shard as u64 {
                return idx;
            }
        }
    }

    /// Appends one request; key format matches valkey-benchmark's
    /// `key:__rand_int__` so both tools share preloaded datasets.
    fn gen_op(&self, rng: &mut Rng, shard: usize, out: &mut Vec<u8>) -> Op {
        let idx = self.key_index(rng, shard);
        if rng.unit() < self.cfg.get_ratio {
            self.encode(Op::Get, idx, out)
        } else {
            self.encode(Op::Set, idx, out)
        }
    }

    fn encode(&self, op: Op, idx: u64, out: &mut Vec<u8>) -> Op {
        let mut key = *b"key:000000000000";
        let mut v = idx;
        for i in (4..16).rev() {
            key[i] = b'0' + (v % 10) as u8;
            v /= 10;
        }
        if op == Op::Get {
            out.extend_from_slice(b"*2\r\n$3\r\nGET\r\n$16\r\n");
            out.extend_from_slice(&key);
            out.extend_from_slice(b"\r\n");
            Op::Get
        } else {
            out.extend_from_slice(if self.cfg.ttl > 0 { b"*5\r\n$3\r\nSET\r\n$16\r\n" } else { b"*3\r\n$3\r\nSET\r\n$16\r\n" });
            out.extend_from_slice(&key);
            out.extend_from_slice(&self.set_suffix);
            Op::Set
        }
    }
}

fn new_hist() -> Histogram<u64> {
    Histogram::new_with_bounds(1, 120_000_000_000, 3).expect("histogram bounds")
}

struct Stats {
    all: Histogram<u64>,
    get: Histogram<u64>,
    set: Histogram<u64>,
    lag: Histogram<u64>,
    ops: u64,
    gets: u64,
    sets: u64,
    hits: u64,
    misses: u64,
    errors: u64,
    timeouts: u64,
    conn_failures: u64,
    send_skipped: u64,
    bytes_tx: u64,
    bytes_rx: u64,
}

impl Stats {
    fn new() -> Self {
        Stats {
            all: new_hist(),
            get: new_hist(),
            set: new_hist(),
            lag: new_hist(),
            ops: 0,
            gets: 0,
            sets: 0,
            hits: 0,
            misses: 0,
            errors: 0,
            timeouts: 0,
            conn_failures: 0,
            send_skipped: 0,
            bytes_tx: 0,
            bytes_rx: 0,
        }
    }

    fn merge(&mut self, o: &Stats) {
        self.all.add(&o.all).unwrap();
        self.get.add(&o.get).unwrap();
        self.set.add(&o.set).unwrap();
        self.lag.add(&o.lag).unwrap();
        self.ops += o.ops;
        self.gets += o.gets;
        self.sets += o.sets;
        self.hits += o.hits;
        self.misses += o.misses;
        self.errors += o.errors;
        self.timeouts += o.timeouts;
        self.conn_failures += o.conn_failures;
        self.send_skipped += o.send_skipped;
        self.bytes_tx += o.bytes_tx;
        self.bytes_rx += o.bytes_rx;
    }

    /// Records one reply if it completed inside the measurement window.
    fn record(&mut self, sh: &Shared, op: Op, kind: ReplyKind, sent: Instant, now: Instant) {
        if now < sh.measure_start || now > sh.end {
            return;
        }
        let ns = now.saturating_duration_since(sent).as_nanos().max(1) as u64;
        self.ops += 1;
        self.all.saturating_record(ns);
        match op {
            Op::Get => {
                self.gets += 1;
                self.get.saturating_record(ns);
                match kind {
                    ReplyKind::Bulk => self.hits += 1,
                    ReplyKind::Null => self.misses += 1,
                    _ => {}
                }
            }
            Op::Set => {
                self.sets += 1;
                self.set.saturating_record(ns);
            }
        }
        if kind == ReplyKind::Error {
            self.errors += 1;
        }
    }
}

type ConnResult = Result<(), String>;

async fn connect(cfg: &Config, shard: usize) -> Result<TcpStream, String> {
    let s = TcpStream::connect((cfg.host.as_str(), cfg.ports[shard])).await.map_err(|e| format!("connect: {e}"))?;
    s.set_nodelay(true).map_err(|e| e.to_string())?;
    Ok(s)
}

async fn closed_loop_conn(sh: Arc<Shared>, st: Rc<RefCell<Stats>>, mut rng: Rng, shard: usize) -> ConnResult {
    let mut sock = connect(&sh.cfg, shard).await?;
    sleep_until(sh.start.into()).await;
    let pipeline = sh.cfg.pipeline;
    let to = Duration::from_millis(sh.cfg.timeout_ms);
    let mut wbuf = Vec::with_capacity(pipeline * (64 + sh.cfg.value_size));
    let mut rbuf: Vec<u8> = Vec::with_capacity(64 * 1024);
    let mut rpos = 0usize;
    let mut ops = Vec::with_capacity(pipeline);
    while Instant::now() < sh.end {
        wbuf.clear();
        ops.clear();
        for _ in 0..pipeline {
            ops.push(sh.gen_op(&mut rng, shard, &mut wbuf));
        }
        let sent = Instant::now();
        sock.write_all(&wbuf).await.map_err(|e| format!("write: {e}"))?;
        st.borrow_mut().bytes_tx += wbuf.len() as u64;
        let mut got = 0;
        let mut now = sent;
        while got < ops.len() {
            match parse_reply_from(&rbuf, rpos).map_err(|e| e.to_string())? {
                Some((end, kind)) => {
                    rpos = end;
                    st.borrow_mut().record(&sh, ops[got], kind, sent, now);
                    got += 1;
                }
                None => {
                    if rpos > 0 {
                        rbuf.drain(..rpos);
                        rpos = 0;
                    }
                    rbuf.reserve(16 * 1024);
                    match timeout(to, sock.read_buf(&mut rbuf)).await {
                        Err(_) => {
                            st.borrow_mut().timeouts += (ops.len() - got) as u64;
                            return Err("timeout".into());
                        }
                        Ok(Ok(0)) => return Err("connection closed by peer".into()),
                        Ok(Ok(n)) => {
                            now = Instant::now();
                            st.borrow_mut().bytes_rx += n as u64;
                        }
                        Ok(Err(e)) => return Err(format!("read: {e}")),
                    }
                }
            }
        }
    }
    Ok(())
}

async fn open_loop_conn(sh: Arc<Shared>, st: Rc<RefCell<Stats>>, mut rng: Rng, shard: usize) -> ConnResult {
    let sock = connect(&sh.cfg, shard).await?;
    let (mut rd, mut wr) = sock.into_split();
    let inflight: Rc<RefCell<VecDeque<(Instant, Op)>>> = Rc::new(RefCell::new(VecDeque::new()));
    let writer_done = Rc::new(Cell::new(false));
    let interval = Duration::from_secs_f64(sh.cfg.conns as f64 / sh.cfg.rate);

    let reader = {
        let sh = sh.clone();
        let st = st.clone();
        let inflight = inflight.clone();
        let writer_done = writer_done.clone();
        async move {
            let to = Duration::from_millis(sh.cfg.timeout_ms);
            let mut rbuf: Vec<u8> = Vec::with_capacity(64 * 1024);
            let mut rpos = 0usize;
            loop {
                let now = Instant::now();
                while let Some((end, kind)) = parse_reply_from(&rbuf, rpos).map_err(|e| e.to_string())? {
                    rpos = end;
                    let (sent, op) = inflight.borrow_mut().pop_front().ok_or("reply without request")?;
                    st.borrow_mut().record(&sh, op, kind, sent, now);
                }
                if writer_done.get() && inflight.borrow().is_empty() {
                    return Ok::<(), String>(());
                }
                if rpos > 0 {
                    rbuf.drain(..rpos);
                    rpos = 0;
                }
                rbuf.reserve(16 * 1024);
                match timeout(Duration::from_millis(100), rd.read_buf(&mut rbuf)).await {
                    Err(_) => {
                        let oldest = inflight.borrow().front().map(|(t, _)| *t);
                        if let Some(t) = oldest {
                            if t.elapsed() > to {
                                let n = inflight.borrow().len() as u64;
                                st.borrow_mut().timeouts += n;
                                return Err("timeout".into());
                            }
                        }
                    }
                    Ok(Ok(0)) => return Err("connection closed by peer".into()),
                    Ok(Ok(n)) => st.borrow_mut().bytes_rx += n as u64,
                    Ok(Err(e)) => return Err(format!("read: {e}")),
                }
            }
        }
    };

    let writer = {
        let sh = sh.clone();
        let st = st.clone();
        async move {
            let mut next = sh.start + interval.mul_f64(rng.unit());
            let mut wbuf = Vec::new();
            sleep_until(next.into()).await;
            while next < sh.end {
                sleep_until(next.into()).await;
                let now = Instant::now();
                wbuf.clear();
                while next <= now && next < sh.end {
                    if inflight.borrow().len() >= MAX_INFLIGHT_OPEN_LOOP {
                        st.borrow_mut().send_skipped += 1;
                    } else {
                        let op = sh.gen_op(&mut rng, shard, &mut wbuf);
                        inflight.borrow_mut().push_back((now, op));
                        if now >= sh.measure_start {
                            st.borrow_mut().lag.saturating_record(now.duration_since(next).as_nanos().max(1) as u64);
                        }
                    }
                    next += interval;
                }
                if !wbuf.is_empty() {
                    wr.write_all(&wbuf).await.map_err(|e| format!("write: {e}"))?;
                    st.borrow_mut().bytes_tx += wbuf.len() as u64;
                }
            }
            writer_done.set(true);
            Ok::<(), String>(())
        }
    };

    let (r, w) = tokio::join!(reader, writer);
    w?;
    r
}

/// SETs every key owned by (`shard`, `slot`): keys with `idx % n == shard`
/// and `(idx / n) % per_shard == slot`, `pipeline` at a time.
async fn preload_conn(sh: Arc<Shared>, st: Rc<RefCell<Stats>>, shard: usize, slot: u64, per_shard: u64) -> ConnResult {
    let mut sock = connect(&sh.cfg, shard).await?;
    let n = sh.cfg.ports.len() as u64;
    let mut next = shard as u64 + slot * n;
    let stride = n * per_shard;
    let mut wbuf = Vec::new();
    let mut rbuf: Vec<u8> = Vec::with_capacity(64 * 1024);
    while next < sh.cfg.keyspace {
        wbuf.clear();
        let mut sent = 0;
        while sent < sh.cfg.pipeline && next < sh.cfg.keyspace {
            sh.encode(Op::Set, next, &mut wbuf);
            next += stride;
            sent += 1;
        }
        sock.write_all(&wbuf).await.map_err(|e| format!("write: {e}"))?;
        let mut got = 0;
        let mut rpos = 0;
        while got < sent {
            match parse_reply_from(&rbuf, rpos).map_err(|e| e.to_string())? {
                Some((end, kind)) => {
                    rpos = end;
                    got += 1;
                    let mut s = st.borrow_mut();
                    s.sets += 1;
                    if kind == ReplyKind::Error {
                        s.errors += 1;
                    }
                }
                None => {
                    rbuf.drain(..rpos);
                    rpos = 0;
                    rbuf.reserve(16 * 1024);
                    if sock.read_buf(&mut rbuf).await.map_err(|e| format!("read: {e}"))? == 0 {
                        return Err("connection closed by peer".into());
                    }
                }
            }
        }
        rbuf.drain(..rpos);
    }
    Ok(())
}

fn run_thread(sh: Arc<Shared>, first_conn: usize, conns: usize, tid: usize) -> Stats {
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("runtime");
    let local = tokio::task::LocalSet::new();
    let st = Rc::new(RefCell::new(Stats::new()));
    local.block_on(&rt, async {
        let mut handles = Vec::new();
        let nports = sh.cfg.ports.len();
        for c in first_conn..first_conn + conns {
            let seed = sh.cfg.seed ^ ((tid as u64) << 32) ^ (c as u64).wrapping_mul(0x9E37_79B9);
            let rng = Rng(seed);
            let shard = c % nports;
            let fut_sh = sh.clone();
            let fut_st = st.clone();
            handles.push(tokio::task::spawn_local(async move {
                if fut_sh.cfg.preload {
                    let total = fut_sh.cfg.conns;
                    let per_shard = (total / nports + usize::from(shard < total % nports)) as u64;
                    preload_conn(fut_sh, fut_st, shard, (c / nports) as u64, per_shard).await
                } else if fut_sh.cfg.rate > 0.0 {
                    open_loop_conn(fut_sh, fut_st, rng, shard).await
                } else {
                    closed_loop_conn(fut_sh, fut_st, rng, shard).await
                }
            }));
        }
        for h in handles {
            if let Err(e) = h.await.expect("task panicked") {
                eprintln!("loadgen: connection ended early: {e}");
                st.borrow_mut().conn_failures += 1;
            }
        }
    });
    Rc::try_unwrap(st).ok().expect("stats still shared").into_inner()
}

fn pct_json(h: &Histogram<u64>) -> String {
    if h.is_empty() {
        return "null".into();
    }
    let us = |v: u64| v as f64 / 1000.0;
    format!(
        "{{\"mean\":{:.2},\"p50\":{:.2},\"p90\":{:.2},\"p95\":{:.2},\"p99\":{:.2},\"p999\":{:.2},\"p9999\":{:.2},\"max\":{:.2}}}",
        h.mean() / 1000.0,
        us(h.value_at_quantile(0.50)),
        us(h.value_at_quantile(0.90)),
        us(h.value_at_quantile(0.95)),
        us(h.value_at_quantile(0.99)),
        us(h.value_at_quantile(0.999)),
        us(h.value_at_quantile(0.9999)),
        us(h.max())
    )
}

fn main() {
    let cfg = parse_args();

    let zipf_cdf = if cfg.zipf > 0.0 {
        let n = cfg.keyspace as usize;
        let mut cdf = Vec::with_capacity(n);
        let mut acc = 0.0f64;
        for k in 1..=n {
            acc += 1.0 / (k as f64).powf(cfg.zipf);
            cdf.push(acc);
        }
        for c in cdf.iter_mut() {
            *c /= acc;
        }
        Some(cdf)
    } else {
        None
    };

    let mut set_suffix = Vec::new();
    set_suffix.extend_from_slice(format!("\r\n${}\r\n", cfg.value_size).as_bytes());
    set_suffix.extend(std::iter::repeat_n(b'x', cfg.value_size));
    set_suffix.extend_from_slice(b"\r\n");
    if cfg.ttl > 0 {
        let t = cfg.ttl.to_string();
        set_suffix.extend_from_slice(format!("$2\r\nEX\r\n${}\r\n{}\r\n", t.len(), t).as_bytes());
    }

    // Connections are opened during the grace period before `start`.
    let start = if cfg.preload { Instant::now() } else { Instant::now() + Duration::from_millis(500 + cfg.conns as u64 / 2) };
    let measure_start = start + Duration::from_secs_f64(cfg.warmup);
    let end = measure_start + Duration::from_secs_f64(cfg.duration);
    let sh = Arc::new(Shared { cfg: cfg.clone(), zipf_cdf, set_suffix, start, measure_start, end });

    let began = Instant::now();
    let mut threads = Vec::new();
    let mut first = 0;
    for t in 0..cfg.threads {
        let n = cfg.conns / cfg.threads + usize::from(t < cfg.conns % cfg.threads);
        let sh = sh.clone();
        threads.push(std::thread::spawn(move || run_thread(sh, first, n, t)));
        first += n;
    }
    let mut total = Stats::new();
    for t in threads {
        total.merge(&t.join().expect("thread panicked"));
    }

    if cfg.preload {
        println!(
            "{{\"tool\":\"loadgen-preload\",\"keys\":{},\"value_size\":{},\"seconds\":{:.3},\"errors\":{},\"conn_failures\":{}}}",
            total.sets,
            cfg.value_size,
            began.elapsed().as_secs_f64(),
            total.errors,
            total.conn_failures
        );
        return;
    }

    let secs = cfg.duration;
    let hit_rate = if total.hits + total.misses > 0 {
        format!("{:.4}", total.hits as f64 / (total.hits + total.misses) as f64)
    } else {
        "null".into()
    };
    println!(
        "{{\"tool\":\"loadgen\",\"label\":\"{}\",\"conns\":{},\"threads\":{},\"pipeline\":{},\"keyspace\":{},\"zipf\":{},\
\"get_ratio\":{},\"value_size\":{},\"ttl\":{},\"rate_target\":{},\"duration\":{},\"warmup\":{},\
\"ops\":{},\"ops_per_sec\":{:.1},\"gets\":{},\"sets\":{},\"hits\":{},\"misses\":{},\"hit_rate\":{},\
\"errors\":{},\"timeouts\":{},\"conn_failures\":{},\"send_skipped\":{},\"bytes_tx\":{},\"bytes_rx\":{},\
\"lat_us\":{},\"get_lat_us\":{},\"set_lat_us\":{},\"sched_lag_us\":{}}}",
        cfg.label.replace('"', "'"),
        cfg.conns,
        cfg.threads,
        cfg.pipeline,
        cfg.keyspace,
        cfg.zipf,
        cfg.get_ratio,
        cfg.value_size,
        cfg.ttl,
        cfg.rate,
        cfg.duration,
        cfg.warmup,
        total.ops,
        total.ops as f64 / secs,
        total.gets,
        total.sets,
        total.hits,
        total.misses,
        hit_rate,
        total.errors,
        total.timeouts,
        total.conn_failures,
        total.send_skipped,
        total.bytes_tx,
        total.bytes_rx,
        pct_json(&total.all),
        pct_json(&total.get),
        pct_json(&total.set),
        pct_json(&total.lag),
    );
}
