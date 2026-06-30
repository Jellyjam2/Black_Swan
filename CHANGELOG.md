# Changelog

## v0.1.0 - Foundation Release

This is the first public foundation release of Black Swan.

### Added

- Professional public README
- Contribution guide
- Security policy
- GitHub Actions Rust CI
- Public issue labels and contributor starter issues
- Runtime architecture documentation
- WAL contract documentation
- Runtime configuration documentation
- Testing contract documentation
- Consensus scaffold documentation
- Snapshot and log compaction design documentation
- Clean coordinator daemon entrypoint
- Local signed-packet demo example
- Environment-variable runtime configuration
- Ed25519 ingress trust gate
- Sender identity registry
- Capability authorization
- Timestamp validation
- HashSet-based replay protection
- Append-only WAL persistence
- WAL replay path
- Monotonic WAL index assignment
- Deterministic state reducer
- AppendEntries request/response scaffold
- AppendEntries follower-side validation logic
- Snapshot metadata contract
- Log compaction planning contract
- Security integration tests
- WAL integration tests
- Consensus AppendEntries tests
- Snapshot compaction contract tests

### Verified

- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets`
- `cargo test --workspace`
- `cargo run -p black_swan_coordinator --example local_signed_packet_demo`

### Known Limitations

Black Swan v0.1.0 is a foundation release, not production Raft.

The following remain future work:

- consensus-owned term changes
- leader heartbeat transport wiring
- peer replication state
- majority commit advancement
- persistent identity/config registry
- atomic snapshot file writing
- crash-safe WAL truncation
- replay from snapshot plus retained WAL tail
- tracing and metrics
- threat model hardening pass