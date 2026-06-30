use black_swan_consensus::{ActiveRaftEngine, AppendEntriesRequest, RaftLogEntry, RaftRole};
use black_swan_state::LogCommand;

fn submit_graph(graph_id: &str) -> LogCommand {
    LogCommand::SubmitGraph {
        graph_id: graph_id.to_string(),
        payload: "{\"nodes\":[]}".to_string(),
    }
}

fn entry(index: usize, term: u64, graph_id: &str) -> RaftLogEntry {
    RaftLogEntry {
        index,
        term,
        command: submit_graph(graph_id),
    }
}

#[test]
fn append_entries_accepts_heartbeat_with_matching_previous_log() {
    let mut engine = ActiveRaftEngine::new(1, 1);

    let response = engine.handle_append_entries(AppendEntriesRequest {
        term: 1,
        leader_id: "leader_a".to_string(),
        prev_log_index: 0,
        prev_log_term: 0,
        entries: vec![],
        leader_commit: 0,
    });

    assert!(response.success);
    assert_eq!(response.match_index, 0);
    assert_eq!(response.rejection_reason, None);
}

#[test]
fn append_entries_appends_new_entries_and_advances_commit_index() {
    let mut engine = ActiveRaftEngine::new(1, 1);

    let response = engine.handle_append_entries(AppendEntriesRequest {
        term: 1,
        leader_id: "leader_a".to_string(),
        prev_log_index: 0,
        prev_log_term: 0,
        entries: vec![entry(1, 1, "graph_a"), entry(2, 1, "graph_b")],
        leader_commit: 2,
    });

    assert!(response.success);
    assert_eq!(response.match_index, 2);
    assert_eq!(engine.log.len(), 3);
    assert_eq!(engine.commit_index, 2);
    assert_eq!(engine.last_log_index(), 2);
    assert_eq!(engine.last_log_term(), 1);
}

#[test]
fn append_entries_rejects_stale_term() {
    let mut engine = ActiveRaftEngine::new(3, 1);

    let response = engine.handle_append_entries(AppendEntriesRequest {
        term: 2,
        leader_id: "stale_leader".to_string(),
        prev_log_index: 0,
        prev_log_term: 0,
        entries: vec![],
        leader_commit: 0,
    });

    assert!(!response.success);
    assert_eq!(response.term, 3);
    assert_eq!(response.rejection_reason.as_deref(), Some("STALE_TERM"));
}

#[test]
fn append_entries_updates_term_and_steps_down_to_follower() {
    let mut engine = ActiveRaftEngine::new(1, 1);
    engine.role = RaftRole::Candidate;

    let response = engine.handle_append_entries(AppendEntriesRequest {
        term: 2,
        leader_id: "leader_new".to_string(),
        prev_log_index: 0,
        prev_log_term: 0,
        entries: vec![],
        leader_commit: 0,
    });

    assert!(response.success);
    assert_eq!(engine.current_term, 2);
    assert_eq!(engine.role, RaftRole::Follower);
}

#[test]
fn append_entries_rejects_missing_previous_index() {
    let mut engine = ActiveRaftEngine::new(1, 1);

    let response = engine.handle_append_entries(AppendEntriesRequest {
        term: 1,
        leader_id: "leader_a".to_string(),
        prev_log_index: 2,
        prev_log_term: 1,
        entries: vec![],
        leader_commit: 0,
    });

    assert!(!response.success);
    assert_eq!(
        response.rejection_reason.as_deref(),
        Some("MISSING_PREV_LOG_INDEX")
    );
}

#[test]
fn append_entries_rejects_previous_term_mismatch() {
    let mut engine = ActiveRaftEngine::new(1, 1);
    engine.log.push(entry(1, 1, "local_graph"));

    let response = engine.handle_append_entries(AppendEntriesRequest {
        term: 1,
        leader_id: "leader_a".to_string(),
        prev_log_index: 1,
        prev_log_term: 99,
        entries: vec![],
        leader_commit: 0,
    });

    assert!(!response.success);
    assert_eq!(
        response.rejection_reason.as_deref(),
        Some("PREV_LOG_TERM_MISMATCH")
    );
}

#[test]
fn append_entries_truncates_conflicting_entries() {
    let mut engine = ActiveRaftEngine::new(2, 1);
    engine.log.push(entry(1, 1, "old_graph_a"));
    engine.log.push(entry(2, 1, "old_graph_b"));

    let response = engine.handle_append_entries(AppendEntriesRequest {
        term: 2,
        leader_id: "leader_a".to_string(),
        prev_log_index: 1,
        prev_log_term: 1,
        entries: vec![entry(2, 2, "new_graph_b")],
        leader_commit: 2,
    });

    assert!(response.success);
    assert_eq!(engine.log.len(), 3);
    assert_eq!(engine.log[2].term, 2);
    assert_eq!(engine.last_log_index(), 2);
    assert_eq!(engine.commit_index, 2);
}

#[test]
fn append_entries_rejects_non_contiguous_entries() {
    let mut engine = ActiveRaftEngine::new(1, 1);

    let response = engine.handle_append_entries(AppendEntriesRequest {
        term: 1,
        leader_id: "leader_a".to_string(),
        prev_log_index: 0,
        prev_log_term: 0,
        entries: vec![entry(2, 1, "graph_gap")],
        leader_commit: 0,
    });

    assert!(!response.success);
    assert_eq!(
        response.rejection_reason.as_deref(),
        Some("NON_CONTIGUOUS_APPEND")
    );
}
