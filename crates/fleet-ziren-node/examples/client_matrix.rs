//! Exercises every client-side parameter of the SDK's network prover against a fleet.
//!
//!   client_matrix <elf> <plain-endpoint> <tls-endpoint> <cases.env-dir>
//!
//! `<cases.env-dir>` holds env files written by fleetctl / the test script:
//!   valid.env (plain), valid-tls.env (server-authenticated TLS), revoked.env,
//!   unknown.key (a key never registered). Each case sets the SDK's env,
//!   builds `NetworkProver::from_env()`, proves, and compares with the expected outcome.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use zkm_sdk::{NetworkProver, ZKMProofKind, ZKMStdin};

const SDK_VARS: &[&str] = &[
    "ZKM_PROVER",
    "ENDPOINT",
    "ZKM_PRIVATE_KEY",
    "CA_CERT_PATH",
    "DOMAIN_NAME",
    "ZKM_PROOF_POLL_INTERVAL",
    "SHARD_SIZE",
    "MAX_PROVER_NUM",
    "SINGLE_NODE",
    "ZKM_ALLOW_INSECURE_TEST_CA",
];

#[derive(Clone, Copy, PartialEq, Debug)]
enum Expect {
    Proof,
    Fail,
}

struct Case {
    name: &'static str,
    base: &'static str,
    set: Vec<(&'static str, String)>,
    unset: Vec<&'static str>,
    kind: ZKMProofKind,
    by_elf_id: Option<String>,
    input: u32,
    big_stdin: usize,
    timeout: Duration,
    expect: Expect,
}

fn env_file(path: &Path) -> HashMap<String, String> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter_map(|l| l.split_once('='))
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn case(name: &'static str, base: &'static str, expect: Expect) -> Case {
    Case {
        name,
        base,
        set: vec![],
        unset: vec![],
        kind: ZKMProofKind::Compressed,
        by_elf_id: None,
        input: 10,
        big_stdin: 0,
        timeout: Duration::from_secs(600),
        expect,
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let a: Vec<String> = std::env::args().collect();
    anyhow::ensure!(
        a.len() == 5,
        "usage: client_matrix <elf> <plain-endpoint> <tls-endpoint> <env-dir>"
    );
    let elf = std::fs::read(&a[1])?;
    let (plain, tls, dir) = (a[2].clone(), a[3].clone(), PathBuf::from(&a[4]));
    let elf_id = {
        use sha2::Digest;
        hex::encode(sha2::Sha256::digest(&elf))
    };
    let unknown_key = std::fs::read_to_string(dir.join("unknown.key"))?
        .trim()
        .to_string();
    let kinds_only = std::env::var("MATRIX_ONLY").ok();

    let mut cases = vec![
        // authentication
        case("auth: issued key", "valid", Expect::Proof),
        {
            let mut c = case("auth: revoked key", "revoked", Expect::Fail);
            c.set.push(("ENDPOINT", plain.clone()));
            c
        },
        {
            let mut c = case("auth: unregistered key", "valid", Expect::Fail);
            c.set.push(("ZKM_PRIVATE_KEY", unknown_key.clone()));
            c
        },
        {
            let mut c = case("auth: no ZKM_PRIVATE_KEY", "valid", Expect::Fail);
            c.unset.push("ZKM_PRIVATE_KEY");
            c
        },
        {
            let mut c = case("auth: malformed key", "valid", Expect::Fail);
            c.set.push(("ZKM_PRIVATE_KEY", "not-a-key".into()));
            c
        },
        // transport
        {
            let mut c = case("tls: server-authenticated HTTPS", "valid-tls", Expect::Proof);
            c.set.push(("ENDPOINT", tls.clone()));
            c
        },
        {
            let mut c = case("tls: wrong DOMAIN_NAME", "valid-tls", Expect::Fail);
            c.set.push(("ENDPOINT", tls.replace("https://", "http://")));
            c.set.push(("DOMAIN_NAME", "not-the-server".into()));
            c
        },
        {
            let mut c = case(
                "tls: plaintext client to the HTTPS endpoint",
                "valid-tls",
                Expect::Fail,
            );
            c.set.push(("ENDPOINT", tls.clone()));
            c
        },
        {
            let mut c = case("endpoint: unreachable", "valid", Expect::Fail);
            c.set.push(("ENDPOINT", "http://127.0.0.1:1".into()));
            c
        },
        // programs
        {
            let mut c = case("elf: by known elf_id", "valid", Expect::Proof);
            c.by_elf_id = Some(elf_id.clone());
            c
        },
        {
            let mut c = case("elf: by unknown elf_id", "valid", Expect::Fail);
            c.by_elf_id = Some("ab".repeat(32));
            c
        },
        // proof kinds
        {
            let mut c = case("kind: Groth16", "valid", Expect::Proof);
            c.kind = ZKMProofKind::Groth16;
            c
        },
        {
            let mut c = case(
                "kind: Core (the network does not produce it)",
                "valid",
                Expect::Fail,
            );
            c.kind = ZKMProofKind::Core;
            c
        },
        {
            let mut c = case(
                "kind: Plonk (the network does not produce it)",
                "valid",
                Expect::Fail,
            );
            c.kind = ZKMProofKind::Plonk;
            c
        },
        // SDK knobs
        {
            let mut c = case("poll: ZKM_PROOF_POLL_INTERVAL=100", "valid", Expect::Proof);
            c.set.push(("ZKM_PROOF_POLL_INTERVAL", "100".into()));
            c
        },
        {
            let mut c = case("poll: ZKM_PROOF_POLL_INTERVAL=3000", "valid", Expect::Proof);
            c.set.push(("ZKM_PROOF_POLL_INTERVAL", "3000".into()));
            c
        },
        {
            let mut c = case(
                "poll: ZKM_PROOF_POLL_INTERVAL=10 (clamped to 100)",
                "valid",
                Expect::Proof,
            );
            c.set.push(("ZKM_PROOF_POLL_INTERVAL", "10".into()));
            c
        },
        {
            let mut c = case(
                "SHARD_SIZE=4194304 (signed into the request)",
                "valid",
                Expect::Proof,
            );
            c.set.push(("SHARD_SIZE", "4194304".into()));
            c
        },
        {
            let mut c = case("MAX_PROVER_NUM=1", "valid", Expect::Proof);
            c.set.push(("MAX_PROVER_NUM", "1".into()));
            c
        },
        {
            let mut c = case("SINGLE_NODE=true", "valid", Expect::Proof);
            c.set.push(("SINGLE_NODE", "true".into()));
            c
        },
        // inputs
        {
            let mut c = case("stdin: 24 MB input", "valid", Expect::Proof);
            c.big_stdin = 24 << 20;
            c
        },
        {
            let mut c = case("timeout: 1 ms client deadline", "valid", Expect::Fail);
            c.timeout = Duration::from_millis(1);
            c.input = 300_000;
            c
        },
    ];
    if let Some(only) = &kinds_only {
        cases.retain(|c| c.name.contains(only.as_str()));
    }

    let mut failures = 0;
    println!(
        "{:<58} {:<6} {:<6} {:>8}  detail",
        "CASE", "EXPECT", "GOT", "SECS"
    );
    for c in &cases {
        for v in SDK_VARS {
            std::env::remove_var(v);
        }
        let base = env_file(&dir.join(format!("{}.env", c.base)));
        for (k, v) in &base {
            std::env::set_var(k, v);
        }
        if c.base != "valid-tls" && !base.contains_key("ENDPOINT") {
            std::env::set_var("ENDPOINT", &plain);
        }
        for (k, v) in &c.set {
            std::env::set_var(k, v);
        }
        for k in &c.unset {
            std::env::remove_var(k);
        }
        let t = Instant::now();
        let outcome: anyhow::Result<String> = async {
            let prover = NetworkProver::from_env()?;
            let mut stdin = ZKMStdin::new();
            stdin.write(&c.input);
            if c.big_stdin > 0 {
                stdin.write_vec(vec![7u8; c.big_stdin]);
            }
            let (proof, cycles) = prover
                .prove_with_cycles(&elf, stdin, c.kind, c.by_elf_id.clone(), Some(c.timeout))
                .await?;
            Ok(format!(
                "cycles={cycles} pv={}B {:?}",
                proof.public_values.as_slice().len(),
                std::mem::discriminant(&proof.proof)
            ))
        }
        .await;
        let (got, detail) = match outcome {
            Ok(d) => (Expect::Proof, d),
            Err(e) => (
                Expect::Fail,
                format!("{e:#}")
                    .replace('\n', " ")
                    .chars()
                    .take(110)
                    .collect(),
            ),
        };
        let ok = got == c.expect;
        if !ok {
            failures += 1;
        }
        println!(
            "{:<58} {:<6} {:<6} {:>8.2}  {}{}",
            c.name,
            format!("{:?}", c.expect),
            format!("{got:?}"),
            t.elapsed().as_secs_f64(),
            if ok { "" } else { "MISMATCH " },
            detail
        );
    }
    println!("MATRIX {} cases, {} mismatches", cases.len(), failures);
    Ok(())
}
