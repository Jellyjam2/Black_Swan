# Black Swan Architecture

Black Swan is organized as a Rust workspace.

## Modules

```text
crates/state       pure command/state contract
crates/security    ingress authentication and authorization
crates/transport   TCP framed transport
crates/storage     append-only WAL
crates/consensus   Raft scaffold
crates/scheduler   task scheduling contracts
apps/coordinator   runtime daemon
```

## Runtime Flow

```text
WirePacket
  -> Transport
  -> Trust Gate
  -> WAL Append
  -> State Reducer
  -> State View
```

## Core Rule

A command must be written to the WAL before it is applied to state.

This preserves recovery correctness: after restart, the runtime can replay the WAL and reconstruct state.
