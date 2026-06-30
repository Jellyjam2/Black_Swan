use std::fs::{create_dir_all, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

pub use black_swan_state::LogCommand;

// ======================================================
// WAL ENTRY FORMAT (APPEND-ONLY LOG)
// ======================================================

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WalEntry {
    pub index: usize,
    pub term: u64,
    pub command: LogCommand,
}

// ======================================================
// SNAPSHOT AND COMPACTION CONTRACTS
// ======================================================

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotMetadata {
    pub node_id: String,
    pub last_included_index: usize,
    pub last_included_term: u64,
    pub state_hash: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactionPolicy {
    pub min_entries_to_keep: usize,
    pub snapshot_after_entries: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogCompactionPlan {
    pub snapshot: SnapshotMetadata,
    pub compact_through_index: usize,
    pub retain_from_index: usize,
}

impl CompactionPolicy {
    pub fn conservative_default() -> Self {
        Self {
            min_entries_to_keep: 128,
            snapshot_after_entries: 1024,
        }
    }

    pub fn should_snapshot(&self, entry_count: usize) -> bool {
        entry_count >= self.snapshot_after_entries
    }
}

pub fn build_compaction_plan(
    node_id: impl Into<String>,
    entries: &[WalEntry],
    policy: &CompactionPolicy,
    state_hash: impl Into<String>,
) -> Option<LogCompactionPlan> {
    if entries.is_empty() || !policy.should_snapshot(entries.len()) {
        return None;
    }

    let retain_from_position = entries.len().saturating_sub(policy.min_entries_to_keep);
    let snapshot_position = retain_from_position.saturating_sub(1);
    let snapshot_entry = &entries[snapshot_position];

    Some(LogCompactionPlan {
        snapshot: SnapshotMetadata {
            node_id: node_id.into(),
            last_included_index: snapshot_entry.index,
            last_included_term: snapshot_entry.term,
            state_hash: state_hash.into(),
        },
        compact_through_index: snapshot_entry.index,
        retain_from_index: entries
            .get(retain_from_position)
            .map(|entry| entry.index)
            .unwrap_or(snapshot_entry.index.saturating_add(1)),
    })
}

// ======================================================
// DISK WAL STORAGE ENGINE
// ======================================================

pub struct DiskWAL {
    file_path: PathBuf,
}

impl DiskWAL {
    pub fn new(node_id: &str) -> Self {
        let mut dir = PathBuf::from("storage");
        dir.push(node_id);

        let _ = create_dir_all(&dir);

        let mut file_path = dir;
        file_path.push("wal.log");

        Self { file_path }
    }

    // --------------------------------------------------
    // APPEND ENTRY (WRITE-AHEAD LOG RULE)
    // --------------------------------------------------
    pub fn append(&self, entry: &WalEntry) -> Result<(), String> {
        let serialized = serde_json::to_string(entry).map_err(|e| format!("SERIALIZE_ERR: {e}"))?;

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.file_path)
            .map_err(|e| format!("OPEN_ERR: {e}"))?;

        writeln!(file, "{serialized}").map_err(|e| format!("WRITE_ERR: {e}"))?;

        file.sync_all().map_err(|e| format!("FSYNC_ERR: {e}"))?;

        Ok(())
    }

    // --------------------------------------------------
    // APPEND COMMAND WITH MONOTONIC INDEX
    // --------------------------------------------------
    pub fn append_command(&self, term: u64, command: LogCommand) -> Result<WalEntry, String> {
        let entry = WalEntry {
            index: self.next_index()?,
            term,
            command,
        };

        self.append(&entry)?;

        Ok(entry)
    }

    // --------------------------------------------------
    // NEXT LOG INDEX
    // --------------------------------------------------
    pub fn next_index(&self) -> Result<usize, String> {
        let entries = self.replay()?;

        let next_from_last_entry = entries
            .last()
            .map(|entry| entry.index.saturating_add(1))
            .unwrap_or(0);

        // Legacy WAL files from early prototypes may contain repeated index=0
        // entries. The length fallback prevents new appends from reusing an old
        // index when replaying those early files.
        Ok(next_from_last_entry.max(entries.len()))
    }

    // --------------------------------------------------
    // REPLAY ENTIRE LOG ON STARTUP
    // --------------------------------------------------
    pub fn replay(&self) -> Result<Vec<WalEntry>, String> {
        if !self.file_path.exists() {
            return Ok(vec![]);
        }

        let file = OpenOptions::new()
            .read(true)
            .open(&self.file_path)
            .map_err(|e| format!("OPEN_ERR: {e}"))?;

        let reader = BufReader::new(file);

        let mut entries = Vec::new();

        for line in reader.lines() {
            let line = line.map_err(|e| format!("READ_ERR: {e}"))?;

            if line.trim().is_empty() {
                continue;
            }

            let entry: WalEntry =
                serde_json::from_str(&line).map_err(|e| format!("DESERIALIZE_ERR: {e}"))?;

            entries.push(entry);
        }

        Ok(entries)
    }
}

// ======================================================
// THREAD SAFE WRAPPER
// ======================================================

pub struct SharedWAL {
    pub inner: Arc<RwLock<DiskWAL>>,
}

impl SharedWAL {
    pub fn new(node_id: &str) -> Self {
        Self {
            inner: Arc::new(RwLock::new(DiskWAL::new(node_id))),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs::remove_dir_all;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    fn unique_node_id(prefix: &str) -> String {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();

        format!("{prefix}_{stamp}")
    }

    fn submit_graph(graph_id: &str) -> LogCommand {
        LogCommand::SubmitGraph {
            graph_id: graph_id.to_string(),
            payload: "{\"nodes\":[]}".to_string(),
        }
    }

    #[test]
    fn append_command_assigns_monotonic_indices() {
        let node_id = unique_node_id("wal_monotonic");
        let wal = DiskWAL::new(&node_id);

        let first = wal.append_command(1, submit_graph("g1")).unwrap();
        let second = wal.append_command(1, submit_graph("g2")).unwrap();
        let third = wal.append_command(2, submit_graph("g3")).unwrap();

        assert_eq!(first.index, 0);
        assert_eq!(second.index, 1);
        assert_eq!(third.index, 2);
        assert_eq!(third.term, 2);

        let replayed = wal.replay().unwrap();

        assert_eq!(replayed.len(), 3);
        assert_eq!(replayed[0].index, 0);
        assert_eq!(replayed[1].index, 1);
        assert_eq!(replayed[2].index, 2);

        let _ = remove_dir_all(PathBuf::from("storage").join(node_id));
    }

    #[test]
    fn next_index_handles_legacy_repeated_zero_entries() {
        let node_id = unique_node_id("wal_legacy");
        let wal = DiskWAL::new(&node_id);

        wal.append(&WalEntry {
            index: 0,
            term: 0,
            command: submit_graph("legacy_a"),
        })
        .unwrap();

        wal.append(&WalEntry {
            index: 0,
            term: 0,
            command: submit_graph("legacy_b"),
        })
        .unwrap();

        let next = wal.next_index().unwrap();

        assert_eq!(next, 2);

        let modern = wal.append_command(1, submit_graph("modern")).unwrap();

        assert_eq!(modern.index, 2);

        let _ = remove_dir_all(PathBuf::from("storage").join(node_id));
    }

    #[test]
    fn compaction_policy_waits_until_threshold() {
        let policy = CompactionPolicy {
            min_entries_to_keep: 2,
            snapshot_after_entries: 5,
        };

        assert!(!policy.should_snapshot(4));
        assert!(policy.should_snapshot(5));
    }

    #[test]
    fn compaction_plan_preserves_tail_entries() {
        let entries: Vec<WalEntry> = (0..6)
            .map(|idx| WalEntry {
                index: idx,
                term: 1,
                command: submit_graph(&format!("g{idx}")),
            })
            .collect();

        let policy = CompactionPolicy {
            min_entries_to_keep: 2,
            snapshot_after_entries: 5,
        };

        let plan = build_compaction_plan("node_test", &entries, &policy, "hash123").unwrap();

        assert_eq!(plan.snapshot.node_id, "node_test");
        assert_eq!(plan.snapshot.last_included_index, 3);
        assert_eq!(plan.snapshot.last_included_term, 1);
        assert_eq!(plan.snapshot.state_hash, "hash123");
        assert_eq!(plan.compact_through_index, 3);
        assert_eq!(plan.retain_from_index, 4);
    }
}
