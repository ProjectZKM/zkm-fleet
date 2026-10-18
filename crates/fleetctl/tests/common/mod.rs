//! A stub control plane and the CLI, for fleetctl's tests.
#![allow(dead_code)]
use std::io::{Read, Write};
use std::net::TcpListener;
use std::process::Command;

/// fleetctl, with none of the environment's connection settings.
pub fn cli() -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_fleetctl"));
    for name in ["FLEET_URL", "FLEET_NAMESPACE", "FLEET_TAG", "FLEET_TOKEN_FILE", "FLEET_ETC"] {
        cmd.env_remove(name);
    }
    cmd.env("HOME", "/nonexistent");
    cmd
}

/// A request the stub control plane got: its request line, headers (lowercase) and JSON body.
pub struct Seen {
    pub line: String,
    pub headers: String,
    pub body: serde_json::Value,
}

/// A stub control plane: it answers `requests` requests with `answer(request line, body)`
/// and returns what it was asked.
pub fn stub(listener: TcpListener, requests: usize, answer: impl Fn(&str, &serde_json::Value) -> (u16, String) + Send + 'static)
    -> std::thread::JoinHandle<Vec<Seen>> {
    listener.set_nonblocking(true).unwrap();
    std::thread::spawn(move || {
        let mut seen = Vec::new();
        for _ in 0..requests {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            let mut stream = loop {
                if let Ok((s, _)) = listener.accept() { break s; }
                assert!(std::time::Instant::now() < deadline, "the CLI stopped after {:?}", seen.iter().map(|s: &Seen| &s.line).collect::<Vec<_>>());
                std::thread::sleep(std::time::Duration::from_millis(10));
            };
            stream.set_nonblocking(false).unwrap();
            stream.set_read_timeout(Some(std::time::Duration::from_secs(5))).unwrap();
            let mut head = Vec::new(); let mut byte = [0];
            while !head.ends_with(b"\r\n\r\n") { stream.read_exact(&mut byte).unwrap(); head.push(byte[0]); }
            let head = String::from_utf8(head).unwrap();
            let headers = head.to_ascii_lowercase();
            let length = headers.lines().find_map(|l| l.strip_prefix("content-length: ")).map_or(0, |v| v.trim().parse().unwrap());
            let mut body = vec![0; length];
            stream.read_exact(&mut body).unwrap();
            let body = serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null);
            let line = head.lines().next().unwrap().to_string();
            let (status, reply) = answer(&line, &body);
            write!(stream, "HTTP/1.1 {status} X\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{reply}", reply.len()).unwrap();
            seen.push(Seen { line, headers, body });
        }
        seen
    })
}

/// A token file for one test.
pub fn token_file(name: &str) -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!("fleet-{name}-{}.token", std::process::id()));
    std::fs::write(&path, "test-admin").unwrap();
    path
}

