//! Workflow triggers — cron, event-driven, manual, and chained.

use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TriggerType {
    /// A standard cron expression, e.g. `"0 * * * *"` (every hour).
    Cron(String),
    /// Fired when a named event is emitted by the system.
    OnEvent(String),
    /// Triggered explicitly by an operator or API call.
    Manual,
    /// Fires automatically when the named workflow completes successfully.
    OnSuccess(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowTrigger {
    pub workflow_name: String,
    pub trigger: TriggerType,
}
