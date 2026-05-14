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
    async fn propose_entry(
        &mut self,
        cmd: LogCommand,
    ) -> Result<usize, String>;

    async fn step_heartbeat_clock(
        &mut self,
    ) -> Result<(), String>;

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
    pub fn new(
        initial_term: u64,
        peer_count: usize,
    ) -> Self {
        Self {
            current_term: initial_term,
            commit_index: 0,
            last_applied: 0,
            role: RaftRole::Follower,

            // Bootstrap-safe sentinel entry
            log: vec![
                RaftLogEntry {
                    term: 0,
                    index: 0,
                    command: LogCommand::CompleteTask {
                        graph_id: String::new(),
                        node_id: String::new(),
                        output: String::new(),
                        success: true,
                    },
                }
            ],

            peer_count,
        }
    }
}

// ------------------------------------------------------------------
// RAFT ENGINE IMPLEMENTATION
// ------------------------------------------------------------------

#[async_trait]
impl RaftConsensusController for ActiveRaftEngine {
    async fn propose_entry(
        &mut self,
        cmd: LogCommand,
    ) -> Result<usize, String> {
        if self.role != RaftRole::Leader {
            return Err(
                "REJECTION: Node is not Leader.".into()
            );
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

    async fn step_heartbeat_clock(
        &mut self,
    ) -> Result<(), String> {
        if self.role == RaftRole::Candidate {
            self.role = RaftRole::Leader;
        }

        Ok(())
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