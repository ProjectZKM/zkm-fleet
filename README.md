# zkm-fleet

One scheduler for the GPU / CPU workloads on our machines. Phase 1 runs Ziren proofs: EthProofs and GOAT submit through the Ziren SDK's network prover (`ZKM_PROVER=network`, unchanged), the scheduler places each job on a prover node, and the nodes run as Ray actors next to the GPU servers. Design and discussion: ProjectZKM/Projects#5.

```
SDK network prover ──mTLS──▶ fleet-scheduler ──cluster token──▶ fleet-node ──loopback──▶ GPU server container
  (EthProofs, GOAT)         stage API :51000                     (Ray actor)              (ziren-gpu image)
                            control  :51001 ◀──register/heartbeat──┘
fleetctl ──admin token──▶   http     :9191  (/admin, public values)
```

## Credentials

| what | who holds it | issued by |
|---|---|---|
| tenant signing key (`ZKM_PRIVATE_KEY`) | the client | `fleetctl credential issue`: generated on the operator's machine; only the address is registered; revocable |
| client certificate (`SSL_CERT_PATH` / `SSL_KEY_PATH`) | the client | same command with `--tls-dir`. The SDK only enables TLS when it has a client certificate, so the stage API runs mutual TLS |
| stage server certificate + CA | scheduler / clients (`CA_CERT_PATH`) | `fleetctl tls init` |
| cluster token | scheduler and every node, both directions | `fleetctl token new` |
| admin token | operators (`fleetctl`) | `fleetctl token new` |

What the SDK's scheme leaves open: the request signature covers `proof_id&seg_size` only, not the ELF or the input, so mutual TLS is what protects the payload in transit; a proof id is single-use (a resubmission returns the existing job). `GetStatus` and the public-values URL are unauthenticated, as the SDK sends no credential there; proof ids are random UUIDs.

## Ports

| port | bind | who |
|---|---|---|
| 51000 stage (gRPC) | external, mTLS | SDK clients |
| 9191 http | external | public values (SDK), `/admin` (admin token), `/healthz` |
| 51001 control (gRPC) | cluster | nodes (cluster token) |
| 31000-31099 fleet-node | cluster | scheduler (cluster token) |
| 30100-30199 GPU server | 127.0.0.1 | its own node only |
| Ray 6379, 6380, 6381, 10001, 8080, 52365, 10002-19999 | cluster | Ray (`deploy/ray-start.sh`) |
| Ray dashboard 8265 | 127.0.0.1 / VPN | operators |
| Postgres 5432 | 127.0.0.1 | scheduler |

## Install

See [docs/INSTALL.md](docs/INSTALL.md): the scheduler and Postgres run under docker compose, Ray and the bridge under systemd (or the cron watchdog), and every client gets its credential from `fleetctl`.
