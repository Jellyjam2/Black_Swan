# Black Swan

**Authenticated Distributed State Machine Runtime in Rust**

Black Swan is a Rust/Tokio runtime for authenticated commands, append-only WAL recovery, deterministic state transitions, and future Raft replication.

It accepts signed commands, verifies sender identity and capability, rejects replayed or expired packets, writes accepted commands to an append-only write-ahead log, and applies them through a deterministic reducer.

The goal is to build a small, auditable foundation for secure distributed coordination.

---

## Current Status

Black Swan currently includes:

- Ed25519-based ingress trust gate
- Sender identity registry
- Capability authorization
- Timestamp validation
- Replay protection
- Length-delimited TCP transport
- Append-only WAL persistence
- Crash-recovery replay path
- Deterministic state reducer
- Raft role/log scaffold
- Coordinator daemon prototype
- Local signed-packet demo example

Planned next:

- Consensus-backed term handling
- Leader heartbeat and transport wiring
- Snapshot and log compaction

---

## Architecture

```text
signed packet
     |
     v
transport/tcp
     |
     v
security trust gate
     |
     v
WAL append + fsync
     |
     v
deterministic reducer
     |
     v
immutable state view
```

Workspace layout:

```text
crates/state       deterministic commands and reducer
crates/security    Ed25519 ingress verification and capability checks
crates/transport   Tokio TCP framed packet transport
crates/storage     append-only WAL and replay
crates/consensus   Raft role/log scaffold
crates/scheduler   execution scheduling contracts
apps/coordinator   runtime daemon and local signed-packet demo
```

---

## Why This Exists

Distributed systems often fail because state changes are accepted too freely, logged too late, or replayed inconsistently.

Black Swan experiments with a stricter flow:

1. Authenticate the sender.
2. Authorize the capability.
3. Reject stale or replayed packets.
4. Persist the command before applying it.
5. Apply commands through a deterministic reducer.
6. Prepare the system for replicated consensus.

---

## Running Locally

Build the workspace:

```bash
cargo build
```

Run the coordinator daemon:

```bash
cargo run -p black_swan_coordinator
```

Run the local signed-packet demo:

```bash
cargo run -p black_swan_coordinator --example local_signed_packet_demo
```

The demo starts a listener, registers a test identity, signs a command, sends it over TCP, writes it to the WAL, and applies it to state.

Runtime configuration is controlled through local defaults or environment variables. See docs/RUNTIME_CONFIG.md and .env.example.


---

## Testing

Run all workspace tests:

```bash
cargo test --workspace
```

Run the local signed-packet demo:

```bash
cargo run -p black_swan_coordinator --example local_signed_packet_demo
```

See `docs/TESTING.md` for the full testing contract.

---

## Roadmap

### Phase 1 - Hardening

- [x] Replace string-based nonce registry with `HashSet`
- [ ] Add TTL/window cleanup for replay protection
- [x] Add monotonic WAL index
- [ ] Add real consensus term source
- [x] Move local packet test harness into `examples/`
- [x] Add integration tests for trust gate and WAL replay

### Phase 2 - Replication

- [x] AppendEntries RPC types
- [ ] Leader-to-follower replication path
- [ ] Commit index advancement
- [x] Follower log consistency checks

### Phase 3 - Production Shape

- [ ] Snapshot compaction
- [x] Structured config
- [ ] Metrics and tracing
- [ ] Threat model documentation
- [ ] Release freeze script

---

## Good First Issues

- Replace replay registry string storage with `HashSet<(sender_id, nonce)>`
- Add WAL replay integration test
- Add expired timestamp test
- Add bad signature test
- Add unauthorized capability test

---

## License

License to be selected.

Recommended: MIT or Apache-2.0 for open source adoption.