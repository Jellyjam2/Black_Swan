# Snapshot and Log Compaction Design

Black Swan now defines the metadata contract for snapshot and log-compaction work.

This slice intentionally adds the design and testable boundary logic, not destructive log truncation.

## Why Snapshots Are Needed

A replicated log cannot grow forever.

After enough entries are safely represented in a state snapshot, older entries can eventually be compacted.

## Snapshot Metadata

A snapshot records:

- `node_id`
- `last_included_index`
- `last_included_term`
- `state_hash`

The pair `last_included_index` and `last_included_term` forms the Raft-style boundary between the compacted prefix and the retained log tail.

## Compaction Policy

The default policy is conservative:

```text
snapshot_after_entries = 1024
min_entries_to_keep    = 128
```

That means Black Swan will not even plan a snapshot until the WAL has at least 1024 entries, and the newest 128 entries are preserved as the live tail.

## Compaction Plan

A compaction plan contains:

- snapshot metadata
- compact-through index
- retain-from index

Example:

```text
entries:             0 1 2 3 4 5 6 7 8 9
min_entries_to_keep: 3
snapshot boundary:             6
retained tail:                   7 8 9
```

## Current Limitation

This is a design scaffold.

The code currently builds a compaction plan but does not rewrite, truncate, or delete WAL files.

Future work should add:

1. atomic snapshot write
2. snapshot hash verification
3. crash-safe WAL rewrite
4. replay from snapshot + retained tail
5. consensus-aware compaction boundary