use std::fs::remove_dir_all;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use black_swan_storage::{DiskWAL, LogCommand, WalEntry};

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

fn assert_submit_graph_id(command: &LogCommand, expected: &str) {
    match command {
        LogCommand::SubmitGraph { graph_id, .. } => assert_eq!(graph_id, expected),
        other => panic!("expected SubmitGraph command, got {other:?}"),
    }
}

#[test]
fn wal_replay_returns_entries_in_append_order() {
    let node_id = unique_node_id("wal_replay_order");
    let wal = DiskWAL::new(&node_id);

    wal.append_command(1, submit_graph("graph_a")).unwrap();
    wal.append_command(1, submit_graph("graph_b")).unwrap();
    wal.append_command(2, submit_graph("graph_c")).unwrap();

    let replayed = wal.replay().unwrap();

    assert_eq!(replayed.len(), 3);

    assert_eq!(replayed[0].index, 0);
    assert_eq!(replayed[1].index, 1);
    assert_eq!(replayed[2].index, 2);

    assert_eq!(replayed[0].term, 1);
    assert_eq!(replayed[1].term, 1);
    assert_eq!(replayed[2].term, 2);

    assert_submit_graph_id(&replayed[0].command, "graph_a");
    assert_submit_graph_id(&replayed[1].command, "graph_b");
    assert_submit_graph_id(&replayed[2].command, "graph_c");

    let _ = remove_dir_all(PathBuf::from("storage").join(node_id));
}

#[test]
fn wal_next_index_survives_restart() {
    let node_id = unique_node_id("wal_restart_index");

    {
        let wal = DiskWAL::new(&node_id);

        wal.append_command(1, submit_graph("before_restart_a"))
            .unwrap();
        wal.append_command(1, submit_graph("before_restart_b"))
            .unwrap();
    }

    {
        let restarted_wal = DiskWAL::new(&node_id);
        let entry = restarted_wal
            .append_command(2, submit_graph("after_restart"))
            .unwrap();

        assert_eq!(entry.index, 2);
        assert_eq!(entry.term, 2);

        let replayed = restarted_wal.replay().unwrap();

        assert_eq!(replayed.len(), 3);
        assert_eq!(replayed[2].index, 2);
        assert_submit_graph_id(&replayed[2].command, "after_restart");
    }

    let _ = remove_dir_all(PathBuf::from("storage").join(node_id));
}

#[test]
fn wal_legacy_repeated_zero_indexes_do_not_get_reused() {
    let node_id = unique_node_id("wal_legacy_zeroes");
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

    let modern = wal.append_command(1, submit_graph("modern")).unwrap();

    assert_eq!(modern.index, 2);

    let replayed = wal.replay().unwrap();

    assert_eq!(replayed.len(), 3);
    assert_eq!(replayed[2].index, 2);
    assert_submit_graph_id(&replayed[2].command, "modern");

    let _ = remove_dir_all(PathBuf::from("storage").join(node_id));
}
