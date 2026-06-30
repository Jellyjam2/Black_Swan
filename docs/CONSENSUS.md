# Consensus Scaffold

Black Swan includes a Raft-shaped consensus scaffold.

This is not a complete production Raft implementation yet. It defines the core contracts needed for the next replication layer.

## Current Pieces

- `RaftRole`
- `RaftLogEntry`
- `ConsensusStatus`
- `ActiveRaftEngine`
- `AppendEntriesRequest`
- `AppendEntriesResponse`

## AppendEntries Contract

The AppendEntries handler currently validates:

1. stale term rejection
2. term update when a newer leader is observed
3. follower step-down behavior
4. previous-log index existence
5. previous-log term match
6. contiguous append order
7. conflicting-entry truncation
8. leader commit advancement

## Current Limitation

The scaffold is local and in-memory.

Next steps:

- wire AppendEntries messages into transport
- add leader heartbeat loop
- add peer replication state
- add commit-index majority advancement
- persist replicated entries through the WAL layer