//! Workflow Module - Task orchestration and workflows

pub mod dag;
pub mod triggers;

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

/// Workflow definition
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Workflow {
    pub id: String,
    pub name: String,
    pub description: String,
    pub steps: Vec<WorkflowStep>,
    pub created_at: i64,
    pub updated_at: i64,
}

/// Workflow step
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkflowStep {
    pub id: String,
    pub name: String,
    pub step_type: StepType,
    pub config: serde_json::Value,
    pub on_success: Option<String>, // next step id
    pub on_failure: Option<String>, // step id or "abort"
    pub timeout_seconds: u64,
    pub retries: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum StepType {
    Query { sql: String },
    Transform { expression: String },
    Condition { expression: String },
    Parallel { steps: Vec<String> },
    Wait { seconds: u64 },
    Webhook { url: String, method: String },
    Script { language: String, code: String },
}

/// Workflow execution
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkflowExecution {
    pub id: String,
    pub workflow_id: String,
    pub status: ExecutionStatus,
    pub current_step: Option<String>,
    pub started_at: i64,
    pub finished_at: Option<i64>,
    pub context: HashMap<String, serde_json::Value>,
    pub step_results: HashMap<String, StepResult>,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum ExecutionStatus {
    Pending,
    Running,
    Paused,
    Completed,
    Failed,
    Cancelled,
}

/// Step execution result
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StepResult {
    pub step_id: String,
    pub status: ExecutionStatus,
    pub output: Option<serde_json::Value>,
    pub error: Option<String>,
    pub started_at: i64,
    pub finished_at: Option<i64>,
    pub retries: u32,
}

/// Workflow engine
pub struct WorkflowEngine {
    workflows: RwLock<HashMap<String, Workflow>>,
    executions: RwLock<HashMap<String, WorkflowExecution>>,
}

impl WorkflowEngine {
    pub fn new() -> Self {
        Self {
            workflows: RwLock::new(HashMap::new()),
            executions: RwLock::new(HashMap::new()),
        }
    }

    /// Register a workflow
    pub async fn register(&self, workflow: Workflow) {
        let mut workflows = self.workflows.write().await;
        workflows.insert(workflow.id.clone(), workflow);
    }

    /// Start a workflow execution
    pub async fn start(&self, workflow_id: &str, context: HashMap<String, serde_json::Value>) -> Result<String, String> {
        let workflows = self.workflows.read().await;
        let workflow = workflows.get(workflow_id)
            .ok_or("Workflow not found")?;

        let first_step = workflow.steps.first()
            .map(|s| s.id.clone());

        let execution = WorkflowExecution {
            id: uuid::Uuid::new_v4().to_string(),
            workflow_id: workflow_id.to_string(),
            status: ExecutionStatus::Running,
            current_step: first_step,
            started_at: chrono::Utc::now().timestamp(),
            finished_at: None,
            context,
            step_results: HashMap::new(),
        };

        let execution_id = execution.id.clone();
        drop(workflows);

        let mut executions = self.executions.write().await;
        executions.insert(execution_id.clone(), execution);

        Ok(execution_id)
    }

    /// Execute the next step
    pub async fn execute_step(&self, execution_id: &str) -> Result<StepResult, String> {
        let mut executions = self.executions.write().await;
        let execution = executions.get_mut(execution_id)
            .ok_or("Execution not found")?;

        let current_step_id = execution.current_step.clone()
            .ok_or("No current step")?;

        let workflows = self.workflows.read().await;
        let workflow = workflows.get(&execution.workflow_id)
            .ok_or("Workflow not found")?;

        let step = workflow.steps.iter()
            .find(|s| s.id == current_step_id)
            .ok_or("Step not found")?;

        // Execute step
        let result = self.run_step(step, &execution.context).await;

        let (output, error) = match result {
            Ok(v) => (Some(v), None),
            Err(e) => (None, Some(e)),
        };

        // Update execution
        let step_result = StepResult {
            step_id: current_step_id.clone(),
            status: if error.is_none() { ExecutionStatus::Completed } else { ExecutionStatus::Failed },
            output,
            error,
            started_at: chrono::Utc::now().timestamp(),
            finished_at: Some(chrono::Utc::now().timestamp()),
            retries: 0,
        };

        execution.step_results.insert(current_step_id.clone(), step_result.clone());

        // Determine next step
        if step_result.status == ExecutionStatus::Completed {
            execution.current_step = step.on_success.clone();
        } else {
            execution.current_step = step.on_failure.clone();
        }

        if execution.current_step.is_none() {
            execution.status = step_result.status;
            execution.finished_at = Some(chrono::Utc::now().timestamp());
        }

        Ok(step_result)
    }

    /// Run a single step
    async fn run_step(&self, step: &WorkflowStep, context: &HashMap<String, serde_json::Value>) -> Result<serde_json::Value, String> {
        match &step.step_type {
            StepType::Query { sql } => {
                // In production: execute SQL query
                Ok(serde_json::json!({"executed": sql}))
            }
            StepType::Transform { expression } => {
                // In production: evaluate expression
                Ok(serde_json::json!({"transformed": true}))
            }
            StepType::Condition { expression } => {
                // In production: evaluate boolean condition
                Ok(serde_json::json!({"result": true}))
            }
            StepType::Parallel { steps } => {
                Ok(serde_json::json!({"parallel_steps": steps.len()}))
            }
            StepType::Wait { seconds } => {
                tokio::time::sleep(std::time::Duration::from_secs(*seconds)).await;
                Ok(serde_json::json!({"waited": seconds}))
            }
            StepType::Webhook { url, method } => {
                // In production: use reqwest
                Ok(serde_json::json!({"called": url}))
            }
            StepType::Script { language, code } => {
                // In production: execute in sandbox
                Ok(serde_json::json!({"executed": language}))
            }
        }
    }

    /// Pause an execution
    pub async fn pause(&self, execution_id: &str) -> Result<(), String> {
        let mut executions = self.executions.write().await;
        let execution = executions.get_mut(execution_id)
            .ok_or("Execution not found")?;
        execution.status = ExecutionStatus::Paused;
        Ok(())
    }

    /// Resume a paused execution
    pub async fn resume(&self, execution_id: &str) -> Result<(), String> {
        let mut executions = self.executions.write().await;
        let execution = executions.get_mut(execution_id)
            .ok_or("Execution not found")?;
        if execution.status == ExecutionStatus::Paused {
            execution.status = ExecutionStatus::Running;
        }
        Ok(())
    }

    /// Cancel an execution
    pub async fn cancel(&self, execution_id: &str) -> Result<(), String> {
        let mut executions = self.executions.write().await;
        let execution = executions.get_mut(execution_id)
            .ok_or("Execution not found")?;
        execution.status = ExecutionStatus::Cancelled;
        execution.finished_at = Some(chrono::Utc::now().timestamp());
        Ok(())
    }

    /// Get execution status
    pub async fn get_execution(&self, execution_id: &str) -> Option<WorkflowExecution> {
        let executions = self.executions.read().await;
        executions.get(execution_id).cloned()
    }

    /// List workflows
    pub async fn list_workflows(&self) -> Vec<Workflow> {
        let workflows = self.workflows.read().await;
        workflows.values().cloned().collect()
    }

    /// List executions for a workflow
    pub async fn list_executions(&self, workflow_id: &str) -> Vec<WorkflowExecution> {
        let executions = self.executions.read().await;
        executions.values()
            .filter(|e| e.workflow_id == workflow_id)
            .cloned()
            .collect()
    }
}

impl Default for WorkflowEngine {
    fn default() -> Self {
        Self::new()
    }
}
