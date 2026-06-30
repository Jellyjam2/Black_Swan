# Black Swan v0.1.0 Release Notes

## Summary

Black Swan v0.1.0 is the first public foundation release.

It establishes a Rust/Tokio authenticated distributed-state runtime foundation with signed command ingress, replay protection, append-only WAL persistence, deterministic state transitions, test coverage, documentation, and a Raft-shaped consensus scaffold.

## What Works

- Signed packet validation
- Sender identity registration
- Capability authorization
- Timestamp expiry checks
- Replay rejection
- TCP framed packet transport
- Append-only WAL writes
- WAL replay on startup
- Monotonic WAL index assignment
- Deterministic state reduction
- Local signed-packet runtime demo
- CI-backed formatting, clippy, and test checks
- AppendEntries RPC contract and follower validation scaffold
- Snapshot metadata and log compaction planning contract

## Verification Commands

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets
cargo test --workspace
cargo run -p black_swan_coordinator --example local_signed_packet_demo
```

## Current Boundary

This release is intentionally scoped as a public foundation.

It is not yet a complete production distributed consensus runtime.

## Next Engineering Targets

- consensus-owned term source
- leader heartbeat loop
- transport wiring for AppendEntries
- majority commit-index advancement
- persistent peer/identity config
- atomic snapshot write path
- crash-safe WAL compaction
- tracing and metrics