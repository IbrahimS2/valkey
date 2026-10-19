//! rvk-server: proof-of-concept Rust-native server for a declared subset of
//! the Valkey protocol (see ../../FEATURE_MATRIX.md). Not production code.
//!
//! Command-line options use Valkey's names where one exists, so the benchmark
//! harness can start it like valkey-server:
//!   rvk-server --port 6379 --bind 127.0.0.1 --threads 2 --maxmemory 256mb
//!              --maxmemory-policy allkeys-lru --maxclients 10000 --requirepass x
//! Valkey options that do not apply (persistence, logging, directories) are
//! accepted and ignored only where ignoring them cannot change semantics:
//! `--save ""` and `--appendonly no` are accepted; any request to enable
//! persistence is rejected at start-up.

mod cmd;
mod resp;
mod server;
mod store;

use server::{Config, Policy};

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// Valkey's `memtoull`: k/m/g are powers of 1000, kb/mb/gb powers of 1024.
pub(crate) fn parse_memory(s: &str) -> Option<usize> {
    let s = s.trim().to_ascii_lowercase();
    let (num, mul) = if let Some(n) = s.strip_suffix("gb") {
        (n, 1usize << 30)
    } else if let Some(n) = s.strip_suffix("mb") {
        (n, 1 << 20)
    } else if let Some(n) = s.strip_suffix("kb") {
        (n, 1 << 10)
    } else if let Some(n) = s.strip_suffix('g') {
        (n, 1_000_000_000)
    } else if let Some(n) = s.strip_suffix('m') {
        (n, 1_000_000)
    } else if let Some(n) = s.strip_suffix('k') {
        (n, 1_000)
    } else {
        (s.as_str(), 1)
    };
    num.parse::<usize>().ok()?.checked_mul(mul)
}

/// Raises the open-files limit for `maxclients` and, like Valkey's
/// `adjustOpenFilesLimit`, lowers `maxclients` when the limit cannot cover it,
/// so excess clients get an error instead of hanging in the accept queue.
fn adjust_open_files(cfg: &mut Config) {
    const RESERVED: u64 = 32;
    let want = cfg.maxclients as u64 + RESERVED + cfg.threads as u64;
    let mut lim = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
    // SAFETY: getrlimit/setrlimit only read and write the struct passed in.
    unsafe {
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut lim) != 0 {
            return;
        }
        if lim.rlim_cur < want {
            let new = want.min(lim.rlim_max);
            let raised = libc::rlimit { rlim_cur: new, rlim_max: lim.rlim_max };
            if libc::setrlimit(libc::RLIMIT_NOFILE, &raised) == 0 {
                lim.rlim_cur = new;
            }
        }
    }
    let cap = lim.rlim_cur.saturating_sub(RESERVED + cfg.threads as u64) as usize;
    if cap < cfg.maxclients {
        eprintln!("rvk-server: open-files limit {} allows only {} clients; maxclients lowered from {}", lim.rlim_cur, cap, cfg.maxclients);
        cfg.maxclients = cap;
    }
}

fn die(msg: &str) -> ! {
    eprintln!("rvk-server: {msg}");
    std::process::exit(1);
}

fn main() {
    let mut cfg = Config {
        bind: "127.0.0.1".into(),
        port: 6379,
        threads: 1,
        maxclients: 10000,
        requirepass: None,
        hz: 10,
        proto_max_bulk_len: 512 * 1024 * 1024,
        client_query_buffer_limit: 1024 * 1024 * 1024,
        maxmemory_samples: 5,
        enable_debug: false,
    };
    let mut maxmemory = 0usize;
    let mut policy = Policy::NoEviction;
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < args.len() {
        let key = args[i].trim_start_matches("--").to_ascii_lowercase();
        let val = args.get(i + 1).cloned().unwrap_or_else(|| die(&format!("missing value for --{key}")));
        match key.as_str() {
            "port" => cfg.port = val.parse().unwrap_or_else(|_| die("bad --port")),
            "bind" => cfg.bind = val.split_whitespace().next().unwrap_or("127.0.0.1").to_string(),
            "threads" | "io-threads" => cfg.threads = val.parse().unwrap_or_else(|_| die("bad --threads")),
            "maxclients" => cfg.maxclients = val.parse().unwrap_or_else(|_| die("bad --maxclients")),
            "requirepass" => cfg.requirepass = if val.is_empty() { None } else { Some(val.into_bytes()) },
            "hz" => cfg.hz = val.parse().unwrap_or_else(|_| die("bad --hz")),
            "maxmemory" => maxmemory = parse_memory(&val).unwrap_or_else(|| die("bad --maxmemory")),
            "maxmemory-policy" => policy = Policy::parse(val.as_bytes()).unwrap_or_else(|| die("unsupported --maxmemory-policy")),
            "maxmemory-samples" => cfg.maxmemory_samples = val.parse().unwrap_or_else(|_| die("bad --maxmemory-samples")),
            "proto-max-bulk-len" => cfg.proto_max_bulk_len = parse_memory(&val).unwrap_or_else(|| die("bad --proto-max-bulk-len")),
            "client-query-buffer-limit" => {
                cfg.client_query_buffer_limit = parse_memory(&val).unwrap_or_else(|| die("bad --client-query-buffer-limit"))
            }
            "enable-debug-command" => {
                cfg.enable_debug = match val.as_str() {
                    // rvk listens on loopback in every supported setup, so "local" means enabled.
                    "yes" | "local" => true,
                    "no" => false,
                    _ => die("bad --enable-debug-command"),
                }
            }
            // Harmless Valkey options the benchmark harness passes.
            "save" if val.is_empty() => {}
            "appendonly" if val == "no" => {}
            "dir" | "logfile" | "daemonize" if key != "daemonize" || val == "no" => {}
            "save" | "appendonly" => die("persistence is outside this proof of concept's scope"),
            other => die(&format!("unsupported option --{other}")),
        }
        i += 2;
    }
    if cfg.threads == 0 || cfg.threads > 64 {
        die("--threads must be between 1 and 64");
    }
    // Argument positions in the read buffer are 32-bit offsets.
    const MAX_BUF: usize = u32::MAX as usize - 1024;
    if cfg.proto_max_bulk_len > MAX_BUF / 2 || cfg.client_query_buffer_limit > MAX_BUF / 2 {
        die("--proto-max-bulk-len and --client-query-buffer-limit must be below 2gb in this proof of concept");
    }
    adjust_open_files(&mut cfg);
    if let Err(e) = server::run(cfg, maxmemory, policy) {
        die(&format!("{e}"));
    }
}
