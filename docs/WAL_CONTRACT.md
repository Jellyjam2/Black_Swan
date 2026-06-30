# WAL Contract

The write-ahead log is the recovery foundation of Black Swan.

## Rule

Accepted commands must be appended to disk before they are applied to state.

## Entry Format

Each WAL entry contains:

- index
- term
- command

## Monotonic Index

New commands are appended through `append_command`.

That method derives the next WAL index from the existing log before writing the new entry.

For compatibility with early prototype logs that used repeated `index = 0`, the next index uses the larger value of:

1. last stored index + 1
2. replayed entry count

This prevents newly accepted commands from reusing old indexes.

## Recovery

On startup:

1. open WAL
2. read entries in order
3. deserialize commands
4. apply each command to a fresh state machine

## Current Limitation

The coordinator now assigns monotonic WAL indexes, but the term is still provided by runtime configuration.

Next hardening steps:

- connect term to consensus state
- reject corrupted or out-of-order WAL entries
- add snapshot and log compaction