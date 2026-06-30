# Testing

Black Swan uses workspace-level Rust tests and a runnable local signed-packet demo.

## Full Test Pass

Run:

```bash
cargo test --workspace
```

This runs unit tests and integration tests across all workspace crates.

## Security Contract Tests

Security integration tests live in:

```text
crates/security/tests/trust_gate_contract.rs
```

They verify that the ingress trust gate:

- accepts a valid signed packet
- rejects replayed nonces
- rejects expired timestamps
- rejects future timestamps outside the skew window
- rejects invalid signatures
- rejects unauthorized capabilities
- rejects unknown identities

## WAL Contract Tests

WAL integration tests live in:

```text
crates/storage/tests/wal_replay_contract.rs
```

They verify that the WAL:

- replays entries in append order
- preserves term values
- assigns monotonic indexes after restart
- avoids reusing indexes from legacy repeated-zero prototype entries

## Local Runtime Demo

Run:

```bash
cargo run -p black_swan_coordinator --example local_signed_packet_demo
```

The demo starts the coordinator listener, registers a test identity, signs a command, sends it through the TCP transport, validates it at the trust gate, appends it to the WAL, and applies it to deterministic state.