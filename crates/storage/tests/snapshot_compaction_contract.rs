use black_swan_storage::{
    build_compaction_plan, CompactionPolicy, LogCommand, SnapshotMetadata, WalEntry,
};

fn submit_graph(graph_id: &str) -> LogCommand {
    LogCommand::SubmitGraph {
        graph_id: graph_id.to_string(),
        payload: "{\"nodes\":[]}".to_string(),
    }
}

fn wal_entries(count: usize) -> Vec<WalEntry> {
    (0..count)
        .map(|idx| WalEntry {
            index: idx,
            term: if idx < 5 { 1 } else { 2 },
            command: submit_graph(&format!("graph_{idx}")),
        })
        .collect()
}

#[test]
fn snapshot_metadata_records_last_included_boundary() {
    let snapshot = SnapshotMetadata {
        node_id: "node_a".to_string(),
        last_included_index: 7,
        last_included_term: 2,
        state_hash: "state_hash_placeholder".to_string(),
    };

    assert_eq!(snapshot.node_id, "node_a");
    assert_eq!(snapshot.last_included_index, 7);
    assert_eq!(snapshot.last_included_term, 2);
    assert_eq!(snapshot.state_hash, "state_hash_placeholder");
}

#[test]
fn compaction_plan_is_not_created_before_threshold() {
    let entries = wal_entries(3);
    let policy = CompactionPolicy {
        min_entries_to_keep: 2,
        snapshot_after_entries: 4,
    };

    let plan = build_compaction_plan("node_a", &entries, &policy, "hash");

    assert_eq!(plan, None);
}

#[test]
fn compaction_plan_keeps_recent_tail() {
    let entries = wal_entries(10);
    let policy = CompactionPolicy {
        min_entries_to_keep: 3,
        snapshot_after_entries: 5,
    };

    let plan = build_compaction_plan("node_a", &entries, &policy, "hash_abc").unwrap();

    assert_eq!(plan.snapshot.last_included_index, 6);
    assert_eq!(plan.snapshot.last_included_term, 2);
    assert_eq!(plan.compact_through_index, 6);
    assert_eq!(plan.retain_from_index, 7);
}

#[test]
fn conservative_default_requires_large_log_before_snapshot() {
    let policy = CompactionPolicy::conservative_default();

    assert!(!policy.should_snapshot(1023));
    assert!(policy.should_snapshot(1024));
    assert_eq!(policy.min_entries_to_keep, 128);
}
