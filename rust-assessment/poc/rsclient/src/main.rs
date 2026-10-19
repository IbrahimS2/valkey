//! Measures established Rust client functionality (Candidate C) instead of
//! building new code: the `redis` crate's MultiplexedConnection (many logical
//! clients pipelined over few sockets) and its experimental server-assisted
//! client-side cache (RESP3 + CLIENT TRACKING).
//!
//! Every SET writes a value carrying a global sequence number. Before each GET
//! the client notes the newest sequence already acknowledged for that key; a
//! GET that returns an older sequence is a *stale read* (another logical
//! client's acknowledged write was not visible). Without a client cache a
//! single Valkey never produces one; a near cache can, until its
//! invalidation arrives.
//!
//! usage: rsclient --port P [--tasks N] [--conns M] [--threads T]
//!        [--duration S] [--warmup S] [--keyspace K] [--zipf THETA]
//!        [--get-ratio R] [--value-size B] [--cache ENTRIES] [--label TEXT]

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;
use redis::aio::MultiplexedConnection;
use redis::{AsyncCommands, AsyncConnectionConfig};

#[derive(Clone)]
struct Cfg {
    port: u16,
    tasks: usize,
    conns: usize,
    threads: usize,
    duration: f64,
    warmup: f64,
    keyspace: u64,
    zipf: f64,
    get_ratio: f64,
    value_size: usize,
    cache: usize,
    label: String,
}

fn usage() -> ! {
    eprintln!("see the header of rsclient/src/main.rs for usage");
    std::process::exit(2);
}

fn parse() -> Cfg {
    let mut c = Cfg {
        port: 6379,
        tasks: 50,
        conns: 1,
        threads: 2,
        duration: 10.0,
        warmup: 2.0,
        keyspace: 1_000_000,
        zipf: 0.0,
        get_ratio: 0.9,
        value_size: 100,
        cache: 0,
        label: String::new(),
    };
    let a: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i + 1 < a.len() {
        let v = &a[i + 1];
        let n = |s: &str| s.parse::<f64>().unwrap_or_else(|_| usage());
        match a[i].as_str() {
            "--port" => c.port = n(v) as u16,
            "--tasks" => c.tasks = n(v) as usize,
            "--conns" => c.conns = n(v) as usize,
            "--threads" => c.threads = n(v) as usize,
            "--duration" => c.duration = n(v),
            "--warmup" => c.warmup = n(v),
            "--keyspace" => c.keyspace = n(v) as u64,
            "--zipf" => c.zipf = n(v),
            "--get-ratio" => c.get_ratio = n(v),
            "--value-size" => c.value_size = n(v) as usize,
            "--cache" => c.cache = n(v) as usize,
            "--label" => c.label = v.clone(),
            _ => usage(),
        }
        i += 2;
    }
    if c.tasks == 0 || c.conns == 0 || c.keyspace == 0 || c.value_size < 24 {
        usage();
    }
    c
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
}

struct Shared {
    cfg: Cfg,
    cdf: Option<Vec<f64>>,
    seq: AtomicU64,
    /// Newest acknowledged write sequence per key index.
    acked: Vec<AtomicU64>,
    measure_start: Instant,
    end: Instant,
}

#[derive(Default)]
struct TaskStats {
    ops: u64,
    gets: u64,
    hits: u64,
    misses: u64,
    errors: u64,
    stale: u64,
    max_stale_gap: u64,
}

fn key_of(idx: u64) -> String {
    format!("key:{idx:012}")
}

fn value_of(seq: u64, size: usize) -> Vec<u8> {
    let mut v = format!("seq:{seq:020}").into_bytes();
    v.resize(size, b'x');
    v
}

fn seq_of(v: &[u8]) -> Option<u64> {
    std::str::from_utf8(v.get(4..24)?).ok()?.parse().ok()
}

async fn task(sh: Arc<Shared>, mut con: MultiplexedConnection, id: usize) -> (TaskStats, Histogram<u64>) {
    let mut rng = Rng(0x1234_5678 ^ (id as u64).wrapping_mul(0x9E37_79B9));
    let mut st = TaskStats::default();
    let mut h = Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 2).unwrap();
    let cfg = &sh.cfg;
    loop {
        let now = Instant::now();
        if now >= sh.end {
            break;
        }
        let idx = match &sh.cdf {
            None => rng.next() % cfg.keyspace,
            Some(cdf) => {
                let u = rng.unit();
                (cdf.partition_point(|&c| c < u) as u64).min(cfg.keyspace - 1)
            }
        };
        let key = key_of(idx);
        let in_window = now >= sh.measure_start;
        if rng.unit() < cfg.get_ratio {
            let floor = sh.acked[idx as usize].load(Ordering::Acquire);
            let t0 = Instant::now();
            let r: redis::RedisResult<Option<Vec<u8>>> = con.get(&key).await;
            let dt = t0.elapsed();
            if in_window && Instant::now() <= sh.end {
                st.ops += 1;
                st.gets += 1;
                h.saturating_record(dt.as_nanos().max(1) as u64);
                match r {
                    Ok(Some(v)) => {
                        st.hits += 1;
                        if let Some(s) = seq_of(&v) {
                            if s < floor {
                                st.stale += 1;
                                st.max_stale_gap = st.max_stale_gap.max(floor - s);
                            }
                        }
                    }
                    Ok(None) => {
                        st.misses += 1;
                        if floor > 0 {
                            // A write was acknowledged before this read began.
                            st.stale += 1;
                        }
                    }
                    Err(_) => st.errors += 1,
                }
            }
        } else {
            let seq = sh.seq.fetch_add(1, Ordering::Relaxed) + 1;
            let t0 = Instant::now();
            let r: redis::RedisResult<()> = con.set(&key, value_of(seq, cfg.value_size)).await;
            let dt = t0.elapsed();
            if r.is_ok() {
                sh.acked[idx as usize].fetch_max(seq, Ordering::AcqRel);
            }
            if in_window && Instant::now() <= sh.end {
                st.ops += 1;
                h.saturating_record(dt.as_nanos().max(1) as u64);
                if r.is_err() {
                    st.errors += 1;
                }
            }
        }
    }
    (st, h)
}

fn pct(h: &Histogram<u64>) -> String {
    let us = |q: f64| h.value_at_quantile(q) as f64 / 1000.0;
    format!(
        "{{\"mean\":{:.2},\"p50\":{:.2},\"p90\":{:.2},\"p95\":{:.2},\"p99\":{:.2},\"p999\":{:.2},\"p9999\":{:.2},\"max\":{:.2}}}",
        h.mean() / 1000.0,
        us(0.5),
        us(0.9),
        us(0.95),
        us(0.99),
        us(0.999),
        us(0.9999),
        h.max() as f64 / 1000.0
    )
}

fn main() {
    let cfg = parse();
    let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(cfg.threads).enable_all().build().unwrap();
    rt.block_on(async move {
        let url = if cfg.cache > 0 {
            format!("redis://127.0.0.1:{}/?protocol=resp3", cfg.port)
        } else {
            format!("redis://127.0.0.1:{}/", cfg.port)
        };
        let client = redis::Client::open(url).expect("client");
        let mut acfg = AsyncConnectionConfig::new().set_response_timeout(Some(Duration::from_secs(5)));
        if cfg.cache > 0 {
            acfg = acfg.set_cache_config(redis::caching::CacheConfig::new().set_size(NonZeroUsize::new(cfg.cache).unwrap()));
        }
        let mut conns = Vec::new();
        for _ in 0..cfg.conns {
            conns.push(client.get_multiplexed_async_connection_with_config(&acfg).await.expect("connect"));
        }
        let cdf = (cfg.zipf > 0.0).then(|| {
            let mut v = Vec::with_capacity(cfg.keyspace as usize);
            let mut acc = 0.0;
            for k in 1..=cfg.keyspace {
                acc += 1.0 / (k as f64).powf(cfg.zipf);
                v.push(acc);
            }
            v.iter_mut().for_each(|c| *c /= acc);
            v
        });
        let start = Instant::now() + Duration::from_millis(200);
        let measure_start = start + Duration::from_secs_f64(cfg.warmup);
        let sh = Arc::new(Shared {
            acked: (0..cfg.keyspace).map(|_| AtomicU64::new(0)).collect(),
            seq: AtomicU64::new(0),
            cdf,
            measure_start,
            end: measure_start + Duration::from_secs_f64(cfg.duration),
            cfg: cfg.clone(),
        });
        tokio::time::sleep_until(start.into()).await;
        let mut hs = Vec::new();
        for i in 0..cfg.tasks {
            hs.push(tokio::spawn(task(sh.clone(), conns[i % conns.len()].clone(), i)));
        }
        let mut tot = TaskStats::default();
        let mut hist = Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 2).unwrap();
        for h in hs {
            let (s, hh) = h.await.unwrap();
            tot.ops += s.ops;
            tot.gets += s.gets;
            tot.hits += s.hits;
            tot.misses += s.misses;
            tot.errors += s.errors;
            tot.stale += s.stale;
            tot.max_stale_gap = tot.max_stale_gap.max(s.max_stale_gap);
            hist.add(&hh).unwrap();
        }
        let (mut ch, mut cm, mut ci) = (0usize, 0usize, 0usize);
        for c in &conns {
            if let Some(s) = c.get_cache_statistics() {
                ch += s.hit;
                cm += s.miss;
                ci += s.invalidate;
            }
        }
        let hit_rate = if tot.hits + tot.misses > 0 { tot.hits as f64 / (tot.hits + tot.misses) as f64 } else { 0.0 };
        println!(
            "{{\"tool\":\"rsclient\",\"label\":\"{}\",\"tasks\":{},\"mux_conns\":{},\"threads\":{},\"cache_entries\":{},\"keyspace\":{},\"zipf\":{},\"get_ratio\":{},\"value_size\":{},\"duration\":{},\"warmup\":{},\
\"ops\":{},\"ops_per_sec\":{:.1},\"gets\":{},\"hits\":{},\"misses\":{},\"hit_rate\":{:.4},\"errors\":{},\"timeouts\":0,\"conn_failures\":0,\
\"stale_reads\":{},\"max_stale_seq_gap\":{},\"cache_hits\":{},\"cache_misses\":{},\"cache_invalidations\":{},\"bytes_tx\":0,\"bytes_rx\":0,\"lat_us\":{}}}",
            cfg.label, cfg.tasks, cfg.conns, cfg.threads, cfg.cache, cfg.keyspace, cfg.zipf, cfg.get_ratio, cfg.value_size,
            cfg.duration, cfg.warmup, tot.ops, tot.ops as f64 / cfg.duration, tot.gets, tot.hits, tot.misses, hit_rate,
            tot.errors, tot.stale, tot.max_stale_gap, ch, cm, ci, pct(&hist)
        );
    });
}
