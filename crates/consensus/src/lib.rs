use async_trait::async_trait;
use serde::{Deserialize, Serialize};

pub use black_swan_state::LogCommand;

// ------------------------------------------------------------------
// RAFT ROLE DEFINITIONS
// ------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum RaftRole {
    Leader,
    Follower,
    Candidate,
}

// ------------------------------------------------------------------
// RAFT LOG ENTRY
// ------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RaftLogEntry {
    pub term: u64,
    pub index: usize,
    pub command: LogCommand,
}

// ------------------------------------------------------------------
// APPENDENTRIES RPC CONTRACT
// ------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AppendEntriesRequest {
    pub term: u64,
    pub leader_id: String,
    pub prev_log_index: usize,
    pub prev_log_term: u64,
    pub entries: Vec<RaftLogEntry>,
    pub leader_commit: usize,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AppendEntriesResponse {
    pub term: u64,
    pub success: bool,
    pub match_index: usize,
    pub rejection_reason: Option<String>,
}

// ------------------------------------------------------------------
// CONSENSUS STATUS SNAPSHOT
// ------------------------------------------------------------------

pub struct ConsensusStatus {
    pub current_term: u64,
    pub commit_index: usize,
    pub role: RaftRole,
}

// ------------------------------------------------------------------
// CONSENSUS CONTROLLER TRAIT
// ------------------------------------------------------------------

#[async_trait]
pub trait RaftConsensusController: Send + Sync {
    async fn propose_entry(&mut self, cmd: LogCommand) -> Result<usize, String>;

    async fn step_heartbeat_clock(&mut self) -> Result<(), String>;

    fn append_entries(&mut self, request: AppendEntriesRequest) -> AppendEntriesResponse;

    fn get_status(&self) -> ConsensusStatus;

    fn get_log_len(&self) -> usize;
}

// ------------------------------------------------------------------
// ACTIVE RAFT ENGINE
// ------------------------------------------------------------------

pub struct ActiveRaftEngine {
    pub current_term: u64,
    pub commit_index: usize,
    pub last_applied: usize,
    pub role: RaftRole,
    pub log: Vec<RaftLogEntry>,
    pub peer_count: usize,
}

impl ActiveRaftEngine {
    pub fn new(initial_term: u64, peer_count: usize) -> Self {
        Self {
            current_term: initial_term,
            commit_index: 0,
            last_applied: 0,
            role: RaftRole::Follower,

            // Bootstrap-safe sentinel entry
            log: vec![RaftLogEntry {
                term: 0,
                index: 0,
                command: LogCommand::CompleteTask {
                    graph_id: String::new(),
                    node_id: String::new(),
                    output: String::new(),
                    success: true,
                },
            }],

            peer_count,
        }
    }

    pub fn last_log_index(&self) -> usize {
        self.log.last().map(|entry| entry.index).unwrap_or(0)
    }

    pub fn last_log_term(&self) -> u64 {
        self.log.last().map(|entry| entry.term).unwrap_or(0)
    }

    fn rejection(&self, match_index: usize, reason: &str) -> AppendEntriesResponse {
        AppendEntriesResponse {
            term: self.current_term,
            success: false,
            match_index,
            rejection_reason: Some(reason.to_string()),
        }
    }

    pub fn handle_append_entries(
        &mut self,
        request: AppendEntriesRequest,
    ) -> AppendEntriesResponse {
        if request.term < self.current_term {
            return self.rejection(self.last_log_index(), "STALE_TERM");
        }

        if request.term > self.current_term {
            self.current_term = request.term;
            self.role = RaftRole::Follower;
        }

        if self.role != RaftRole::Follower {
            self.role = RaftRole::Follower;
        }

        if request.prev_log_index >= self.log.len() {
            return self.rejection(self.last_log_index(), "MISSING_PREV_LOG_INDEX");
        }

        let previous_entry = &self.log[request.prev_log_index];

        if previous_entry.term != request.prev_log_term {
            return self.rejection(request.prev_log_index, "PREV_LOG_TERM_MISMATCH");
        }

        let mut expected_index = request.prev_log_index + 1;

        for entry in request.entries {
            if entry.index != expected_index {
                return self.rejection(self.last_log_index(), "NON_CONTIGUOUS_APPEND");
            }

            if entry.index < self.log.len() {
                if self.log[entry.index].term != entry.term {
                    self.log.truncate(entry.index);
                    self.log.push(entry);
                }
            } else if entry.index == self.log.len() {
                self.log.push(entry);
            } else {
                return self.rejection(self.last_log_index(), "LOG_GAP");
            }

            expected_index += 1;
        }

        if request.leader_commit > self.commit_index {
            self.commit_index = request.leader_commit.min(self.last_log_index());
        }

        AppendEntriesResponse {
            term: self.current_term,
            success: true,
            match_index: self.last_log_index(),
            rejection_reason: None,
        }
    }
}

// ------------------------------------------------------------------
// RAFT ENGINE IMPLEMENTATION
// ------------------------------------------------------------------

#[async_trait]
impl RaftConsensusController for ActiveRaftEngine {
    async fn propose_entry(&mut self, cmd: LogCommand) -> Result<usize, String> {
        if self.role != RaftRole::Leader {
            return Err("REJECTION: Node is not Leader.".into());
        }

        let next_index = self.log.len();

        let entry = RaftLogEntry {
            term: self.current_term,
            index: next_index,
            command: cmd,
        };

        self.log.push(entry);

        // Single-node auto commit path
        if self.peer_count == 0 {
            self.commit_index = next_index;
        }

        Ok(next_index)
    }

    async fn step_heartbeat_clock(&mut self) -> Result<(), String> {
        if self.role == RaftRole::Candidate {
            self.role = RaftRole::Leader;
        }

        Ok(())
    }

    fn append_entries(&mut self, request: AppendEntriesRequest) -> AppendEntriesResponse {
        self.handle_append_entries(request)
    }

    fn get_status(&self) -> ConsensusStatus {
        ConsensusStatus {
            current_term: self.current_term,
            commit_index: self.commit_index,
            role: self.role.clone(),
        }
    }

    fn get_log_len(&self) -> usize {
        self.log.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn submit_graph(graph_id: &str) -> LogCommand {
        LogCommand::SubmitGraph {
            graph_id: graph_id.to_string(),
            payload: "{\"nodes\":[]}".to_string(),
        }
    }

    #[test]
    fn append_entries_accepts_matching_previous_log() {
        let mut engine = ActiveRaftEngine::new(1, 1);

        let request = AppendEntriesRequest {
            term: 1,
            leader_id: "leader_a".to_string(),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![RaftLogEntry {
                term: 1,
                index: 1,
                command: submit_graph("graph_a"),
            }],
            leader_commit: 1,
        };

        let response = engine.handle_append_entries(request);

        assert!(response.success);
        assert_eq!(response.match_index, 1);
        assert_eq!(engine.commit_index, 1);
        assert_eq!(engine.log.len(), 2);
    }

    #[test]
    fn append_entries_rejects_stale_term() {
        let mut engine = ActiveRaftEngine::new(5, 1);

        let request = AppendEntriesRequest {
            term: 4,
            leader_id: "leader_old".to_string(),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        };

        let response = engine.handle_append_entries(request);

        assert!(!response.success);
        assert_eq!(response.term, 5);
        assert_eq!(response.rejection_reason.as_deref(), Some("STALE_TERM"));
    }
}
