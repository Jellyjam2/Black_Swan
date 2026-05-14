use black_swan_state::{LogCommand, StateView};

pub struct TargetDispatchTask {
    pub node_id: String,
    pub payload: String,
    pub selected_shard: String,
}

pub trait TopologicalGraphScheduler {
    /// Computes ready execution items as pure decisions over a frozen, immutable state snapshot view.
    fn compute_next_execution_steps(&self, view: &StateView) -> Vec<TargetDispatchTask>;
    fn transform_dispatch_to_command(&self, task: TargetDispatchTask) -> LogCommand;
}
