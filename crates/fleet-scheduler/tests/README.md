# Scheduler tests

- `flow.rs`, `heartbeat.rs`, `pg.rs`, and `tls.rs` are Cargo integration tests.
- `workloads/mod.rs` tests workload scheduling and lifecycle behavior and provides
  shared scheduler fixtures. `workloads/policy.rs` tests policy admission,
  preflight decisions, and private runtime delivery.
- `workloads/openshell/mod.rs` tests the Fleet, shim, and OpenShell startup path;
  `docker.rs` contains the native Docker acceptance test. `gateway.rs` and
  `route.rs` are reusable process and transport helpers.
- `fixtures/` holds test data and workload programs, including the Python
  HTTP/SSE/WebSocket app and historical database schema.

The workload suites are library test modules loaded by `src/workloads.rs` under
`#[cfg(test)]`. Keeping them as child modules preserves access to private
scheduler state and the existing test names without adding public test APIs.
Small unit tests remain beside the modules they cover in `src/`.

Run all scheduler tests with `cargo test --locked -p fleet-scheduler`, or just
the workload suites with:

```sh
cargo test --locked -p fleet-scheduler --lib workloads::tests
```

The native gateway and Docker acceptance tests remain opt-in. Their prerequisites
and commands are documented in [the reference](../../../docs/reference.md).
