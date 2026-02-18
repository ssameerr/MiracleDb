//! DAG Executor — topological-sort based workflow orchestration.

use std::collections::{HashMap, HashSet, VecDeque};
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TaskStatus {
    Pending,
    Running,
    Success,
    Failed(String),
    Skipped,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowTask {
    pub id: String,
    pub name: String,
    pub depends_on: Vec<String>,
    pub command: String,         // SQL or shell command
    pub retry_count: u32,
    pub timeout_secs: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DagWorkflow {
    pub name: String,
    pub tasks: Vec<WorkflowTask>,
    pub schedule: Option<String>, // cron expression
}

pub struct DagExecutor {
    pub workflow: DagWorkflow,
    pub status: HashMap<String, TaskStatus>,
}

impl DagExecutor {
    pub fn new(workflow: DagWorkflow) -> Self {
        let status = workflow.tasks.iter()
            .map(|t| (t.id.clone(), TaskStatus::Pending))
            .collect();
        Self { workflow, status }
    }

    /// Topological sort — returns task IDs in valid execution order.
    pub fn execution_order(&self) -> Result<Vec<String>, String> {
        let mut in_degree: HashMap<&str, usize> = HashMap::new();
        let mut deps: HashMap<&str, Vec<&str>> = HashMap::new();

        for task in &self.workflow.tasks {
            in_degree.entry(&task.id).or_insert(0);
            for dep in &task.depends_on {
                *in_degree.entry(&task.id).or_insert(0) += 1;
                deps.entry(dep.as_str()).or_default().push(&task.id);
            }
        }

        let mut queue: VecDeque<&str> = in_degree.iter()
            .filter(|(_, &d)| d == 0)
            .map(|(&id, _)| id)
            .collect();
        let mut order = vec![];

        while let Some(id) = queue.pop_front() {
            order.push(id.to_string());
            if let Some(children) = deps.get(id) {
                for &child in children {
                    let deg = in_degree.get_mut(child).unwrap();
                    *deg -= 1;
                    if *deg == 0 {
                        queue.push_back(child);
                    }
                }
            }
        }

        if order.len() != self.workflow.tasks.len() {
            Err("Cycle detected in workflow DAG".to_string())
        } else {
            Ok(order)
        }
    }

    /// Returns tasks whose dependencies have all succeeded and that are still pending.
    pub fn ready_tasks(&self) -> Vec<&WorkflowTask> {
        self.workflow.tasks.iter().filter(|t| {
            matches!(self.status.get(&t.id), Some(TaskStatus::Pending))
                && t.depends_on.iter().all(|dep| {
                    matches!(self.status.get(dep), Some(TaskStatus::Success))
                })
        }).collect()
    }

    /// Update the status of a task.
    pub fn mark(&mut self, task_id: &str, status: TaskStatus) {
        self.status.insert(task_id.to_string(), status);
    }

    /// Returns true when every task has a terminal status (Success, Failed, or Skipped).
    pub fn is_complete(&self) -> bool {
        self.status.values().all(|s| {
            matches!(s, TaskStatus::Success | TaskStatus::Failed(_) | TaskStatus::Skipped)
        })
    }
}
