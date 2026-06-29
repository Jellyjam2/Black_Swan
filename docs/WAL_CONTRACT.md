# WAL Contract

The write-ahead log is the recovery foundation of Black Swan.

## Rule

Accepted commands must be appended to disk before they are applied to state.

## Entry Format

Each WAL entry contains:

- index
- term
- command

## Recovery

On startup:

1. open WAL
2. read entries in order
3. deserialize commands
4. apply each command to a fresh state machine

## Current Limitation

The current coordinator prototype uses fixed index and term values.

Next hardening step:

- assign monotonic index
- connect term to consensus state
- reject corrupted or out-of-order WAL entries
