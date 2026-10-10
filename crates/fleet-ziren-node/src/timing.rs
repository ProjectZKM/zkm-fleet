/// RPC elapsed time includes payload transfers. Only a local CPU/mock prover
/// can use its execution stopwatch when the prover supplies no timing.
pub fn prove_ms(backend: &str, server_ms: Option<u64>, execution_ms: u64) -> anyhow::Result<u64> {
    match (backend, server_ms) {
        (_, Some(ms)) => Ok(ms),
        ("cpu" | "mock", None) => Ok(execution_ms),
        _ => anyhow::bail!("GPU prover did not report proving time; refusing to use RPC elapsed time"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transfers_do_not_inflate_server_proving_time() {
        assert_eq!(prove_ms("cuda", Some(123), 45_623).unwrap(), 123);
        assert!(prove_ms("cuda", None, 45_623).is_err());
        assert_eq!(prove_ms("cpu", None, 123).unwrap(), 123);
        assert_eq!(prove_ms("mock", None, 123).unwrap(), 123);
    }
}
