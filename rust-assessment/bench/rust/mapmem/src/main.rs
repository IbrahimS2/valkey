//! Lower-bound memory cost of a naive Rust in-memory key/value map, for
//! comparison with Valkey's measured bytes per key (benchmark tooling only).
//!
//! The map stores 16-byte keys in the same `key:%012d` format as the Valkey
//! dataset, plus an 8-byte expiry timestamp per entry. It has no eviction
//! metadata, no concurrency control, no persistence, and no protocol layer,
//! so it understates what a real Rust cache would need.
//!
//! usage: mapmem <keys> <value_size>

use std::collections::HashMap;

fn rss_bytes() -> u64 {
    let s = std::fs::read_to_string("/proc/self/status").expect("status");
    for line in s.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kb: u64 = rest.trim().trim_end_matches(" kB").trim().parse().expect("VmRSS");
            return kb * 1024;
        }
    }
    panic!("VmRSS not found");
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 3 {
        eprintln!("usage: mapmem <keys> <value_size>");
        std::process::exit(2);
    }
    let n: u64 = args[1].parse().expect("keys");
    let vsize: usize = args[2].parse().expect("value_size");
    let value = vec![b'x'; vsize];

    let before = rss_bytes();
    // value bytes plus an expiry timestamp
    type Entry = (Box<[u8]>, u64);
    let mut map: HashMap<Box<[u8]>, Entry> = HashMap::new();
    for i in 0..n {
        let key = format!("key:{i:012}").into_bytes().into_boxed_slice();
        map.insert(key, (value.clone().into_boxed_slice(), 0));
    }
    let after = rss_bytes();
    let per_key = (after - before) as f64 / n as f64;
    let payload = 16 + vsize;
    println!(
        "{{\"tool\":\"mapmem\",\"keys\":{n},\"value_size\":{vsize},\"rss_delta_bytes\":{},\"bytes_per_key\":{per_key:.1},\"payload_bytes_per_key\":{payload},\"overhead_bytes_per_key\":{:.1},\"capacity\":{}}}",
        after - before,
        per_key - payload as f64,
        map.capacity()
    );
}
