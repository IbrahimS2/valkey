//! Minimal RESP2 framing used by the benchmark-only tools in this workspace.
//!
//! This validates frame boundaries only. It is not a general-purpose RESP
//! implementation and does not decode values beyond what the benchmark tools
//! need (reply kind, command name).

/// Largest bulk string accepted, matching Valkey's default `proto-max-bulk-len`.
pub const MAX_BULK: usize = 512 * 1024 * 1024;
/// Largest multibulk length accepted in a request or reply.
pub const MAX_ARRAY: usize = 1024 * 1024;
/// Maximum nesting depth for aggregate replies.
pub const MAX_DEPTH: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplyKind {
    Simple,
    Error,
    Integer,
    Bulk,
    Null,
    Array,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParseError {
    Protocol(&'static str),
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParseError::Protocol(m) => write!(f, "protocol error: {m}"),
        }
    }
}

impl std::error::Error for ParseError {}

/// Index of the '\r' of the first "\r\n" at or after `start`.
fn find_crlf(buf: &[u8], start: usize) -> Option<usize> {
    let mut i = start;
    while i < buf.len() {
        let off = buf[i..].iter().position(|&b| b == b'\r')?;
        let cr = i + off;
        if cr + 1 >= buf.len() {
            return None;
        }
        if buf[cr + 1] == b'\n' {
            return Some(cr);
        }
        i = cr + 1;
    }
    None
}

fn parse_int(line: &[u8]) -> Result<i64, ParseError> {
    if line.is_empty() || line.len() > 20 {
        return Err(ParseError::Protocol("invalid integer"));
    }
    let (neg, digits) = match line[0] {
        b'-' => (true, &line[1..]),
        _ => (false, line),
    };
    if digits.is_empty() {
        return Err(ParseError::Protocol("invalid integer"));
    }
    let mut v: i64 = 0;
    for &d in digits {
        if !d.is_ascii_digit() {
            return Err(ParseError::Protocol("invalid integer"));
        }
        v = v
            .checked_mul(10)
            .and_then(|v| v.checked_add((d - b'0') as i64))
            .ok_or(ParseError::Protocol("integer overflow"))?;
    }
    Ok(if neg { -v } else { v })
}

/// Parses one complete reply starting at `pos`.
///
/// Returns `Ok(Some((end, kind)))` where `end` is the absolute offset just past
/// the reply, or `Ok(None)` when more bytes are needed.
pub fn parse_reply_from(buf: &[u8], pos: usize) -> Result<Option<(usize, ReplyKind)>, ParseError> {
    parse_reply_at(buf, pos, 0)
}

fn parse_reply_at(buf: &[u8], pos: usize, depth: usize) -> Result<Option<(usize, ReplyKind)>, ParseError> {
    if depth > MAX_DEPTH {
        return Err(ParseError::Protocol("reply nesting too deep"));
    }
    if pos >= buf.len() {
        return Ok(None);
    }
    let Some(eol) = find_crlf(buf, pos + 1) else {
        return Ok(None);
    };
    let line = &buf[pos + 1..eol];
    let after = eol + 2;
    match buf[pos] {
        b'+' => Ok(Some((after, ReplyKind::Simple))),
        b'-' => Ok(Some((after, ReplyKind::Error))),
        b':' => {
            parse_int(line)?;
            Ok(Some((after, ReplyKind::Integer)))
        }
        b'$' => {
            let n = parse_int(line)?;
            if n == -1 {
                return Ok(Some((after, ReplyKind::Null)));
            }
            if n < 0 || n as usize > MAX_BULK {
                return Err(ParseError::Protocol("invalid bulk length"));
            }
            let end = after + n as usize + 2;
            if buf.len() < end {
                return Ok(None);
            }
            if &buf[end - 2..end] != b"\r\n" {
                return Err(ParseError::Protocol("missing bulk terminator"));
            }
            Ok(Some((end, ReplyKind::Bulk)))
        }
        b'*' => {
            let n = parse_int(line)?;
            if n == -1 {
                return Ok(Some((after, ReplyKind::Null)));
            }
            if n < 0 || n as usize > MAX_ARRAY {
                return Err(ParseError::Protocol("invalid array length"));
            }
            let mut p = after;
            for _ in 0..n {
                match parse_reply_at(buf, p, depth + 1)? {
                    Some((e, _)) => p = e,
                    None => return Ok(None),
                }
            }
            Ok(Some((p, ReplyKind::Array)))
        }
        _ => Err(ParseError::Protocol("unexpected reply type byte")),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Request {
    /// A command; `name` is the byte range of the first argument.
    Command { end: usize, name: (usize, usize), argc: usize },
    /// `*0` or `*-1`: Valkey consumes these without replying.
    Empty { end: usize },
}

/// Parses one complete RESP multibulk request starting at `pos`.
///
/// Inline commands are rejected because the tools never send them and a proxy
/// that forwarded them would have to reproduce Valkey's inline tokenizer.
pub fn parse_request_from(buf: &[u8], pos: usize) -> Result<Option<Request>, ParseError> {
    if pos >= buf.len() {
        return Ok(None);
    }
    if buf[pos] != b'*' {
        return Err(ParseError::Protocol("inline commands are not supported"));
    }
    let Some(eol) = find_crlf(buf, pos + 1) else {
        return Ok(None);
    };
    let argc = parse_int(&buf[pos + 1..eol])?;
    let mut p = eol + 2;
    if argc <= 0 {
        return Ok(Some(Request::Empty { end: p }));
    }
    if argc as usize > MAX_ARRAY {
        return Err(ParseError::Protocol("invalid multibulk length"));
    }
    let mut name = (0, 0);
    for i in 0..argc as usize {
        if p >= buf.len() {
            return Ok(None);
        }
        if buf[p] != b'$' {
            return Err(ParseError::Protocol("expected '$'"));
        }
        let Some(eol) = find_crlf(buf, p + 1) else {
            return Ok(None);
        };
        let n = parse_int(&buf[p + 1..eol])?;
        if n < 0 || n as usize > MAX_BULK {
            return Err(ParseError::Protocol("invalid bulk length"));
        }
        let start = eol + 2;
        let end = start + n as usize + 2;
        if buf.len() < end {
            return Ok(None);
        }
        if &buf[end - 2..end] != b"\r\n" {
            return Err(ParseError::Protocol("missing bulk terminator"));
        }
        if i == 0 {
            name = (start, start + n as usize);
        }
        p = end;
    }
    Ok(Some(Request::Command { end: p, name, argc: argc as usize }))
}

/// Appends a RESP multibulk encoding of `args` to `out`.
pub fn encode_command(out: &mut Vec<u8>, args: &[&[u8]]) {
    out.push(b'*');
    out.extend_from_slice(args.len().to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
    for a in args {
        out.push(b'$');
        out.extend_from_slice(a.len().to_string().as_bytes());
        out.extend_from_slice(b"\r\n");
        out.extend_from_slice(a);
        out.extend_from_slice(b"\r\n");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replies_complete_and_partial() {
        let cases: &[(&[u8], ReplyKind)] = &[
            (b"+OK\r\n", ReplyKind::Simple),
            (b"-ERR x\r\n", ReplyKind::Error),
            (b":42\r\n", ReplyKind::Integer),
            (b"$3\r\nfoo\r\n", ReplyKind::Bulk),
            (b"$0\r\n\r\n", ReplyKind::Bulk),
            (b"$-1\r\n", ReplyKind::Null),
            (b"*-1\r\n", ReplyKind::Null),
            (b"*2\r\n$1\r\na\r\n*1\r\n:1\r\n", ReplyKind::Array),
            (b"*0\r\n", ReplyKind::Array),
        ];
        for (bytes, kind) in cases {
            assert_eq!(parse_reply_from(bytes, 0), Ok(Some((bytes.len(), *kind))));
            for cut in 0..bytes.len() {
                assert_eq!(parse_reply_from(&bytes[..cut], 0), Ok(None), "cut {cut} of {bytes:?}");
            }
        }
    }

    #[test]
    fn reply_offsets_and_errors() {
        let buf = b"+OK\r\n$2\r\nhi\r\n";
        assert_eq!(parse_reply_from(buf, 5), Ok(Some((buf.len(), ReplyKind::Bulk))));
        assert!(parse_reply_from(b"$2\r\nhiX\n", 0).is_err());
        assert!(parse_reply_from(b"?\r\n", 0).is_err());
        assert!(parse_reply_from(b"$-5\r\n", 0).is_err());
        assert!(parse_reply_from(b":12a\r\n", 0).is_err());
        assert!(parse_reply_from(b":99999999999999999999\r\n", 0).is_err());
    }

    #[test]
    fn deep_nesting_rejected() {
        let mut v = Vec::new();
        for _ in 0..(MAX_DEPTH + 2) {
            v.extend_from_slice(b"*1\r\n");
        }
        v.extend_from_slice(b":1\r\n");
        assert!(parse_reply_from(&v, 0).is_err());
    }

    #[test]
    fn requests() {
        let mut buf = Vec::new();
        encode_command(&mut buf, &[b"SET", b"k", b"value"]);
        let first_len = buf.len();
        encode_command(&mut buf, &[b"GET", b"k"]);
        match parse_request_from(&buf, 0).unwrap().unwrap() {
            Request::Command { end, name, argc } => {
                assert_eq!(end, first_len);
                assert_eq!(&buf[name.0..name.1], b"SET");
                assert_eq!(argc, 3);
            }
            r => panic!("unexpected {r:?}"),
        }
        for cut in first_len..buf.len() {
            assert_eq!(parse_request_from(&buf[..cut], first_len), Ok(None));
        }
        assert_eq!(parse_request_from(b"*0\r\n", 0), Ok(Some(Request::Empty { end: 4 })));
        assert!(parse_request_from(b"PING\r\n", 0).is_err());
        assert!(parse_request_from(b"*1\r\n:1\r\n", 0).is_err());
    }
}
