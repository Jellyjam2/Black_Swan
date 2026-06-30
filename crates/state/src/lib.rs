use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

// ------------------------------------------------------------------
// REPLICATED COMMAND LOG CONTRACT
// ------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum LogCommand {
    SubmitGraph {
        graph_id: String,
        payload: String,
    },

    DispatchTask {
        graph_id: String,
        node_id: String,
        shard_id: String,
    },

    CompleteTask {
        graph_id: String,
        node_id: String,
        output: String,
        success: bool,
    },
}

// ------------------------------------------------------------------
// PURE DETERMINISTIC STATE MACHINE
// ------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct PureState {
    pub active_graphs: HashMap<String, String>,
    pub running_allocations: HashMap<String, String>,
}

impl PureState {
    // --------------------------------------------------------------
    // STATE METRICS
    // --------------------------------------------------------------

    pub fn log_len(&self) -> u64 {
        self.active_graphs.len() as u64
    }

    pub fn current_term(&self) -> u64 {
        0
    }
}

// ------------------------------------------------------------------
// IMMUTABLE STATE VIEW
// ------------------------------------------------------------------

pub struct StateView {
    pub snapshot: Arc<PureState>,
}

// ------------------------------------------------------------------
// REDUCER TRAIT
// ------------------------------------------------------------------

pub trait StateMachineReducer {
    fn apply(&mut self, cmd: &LogCommand);

    fn create_view(&self) -> StateView;
}

// ------------------------------------------------------------------
// STATE REDUCER IMPLEMENTATION
// ------------------------------------------------------------------

impl StateMachineReducer for PureState {
    fn apply(&mut self, cmd: &LogCommand) {
        match cmd {
            LogCommand::SubmitGraph { graph_id, payload } => {
                self.active_graphs.insert(graph_id.clone(), payload.clone());
            }

            LogCommand::DispatchTask {
                node_id, shard_id, ..
            } => {
                self.running_allocations
                    .insert(node_id.clone(), shard_id.clone());
            }

            LogCommand::CompleteTask { node_id, .. } => {
                self.running_allocations.remove(node_id);
            }
        }
    }

    fn create_view(&self) -> StateView {
        StateView {
            snapshot: Arc::new(self.clone()),
        }
    }
}
