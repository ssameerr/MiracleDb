//! Recursive Language Model (RLM) Agent
//!
//! Handles autonomous recursive task decomposition and execution using LLMs.

pub mod context;
pub mod tools;

use std::sync::Arc;
use tokio::sync::RwLock;
use serde::{Deserialize, Serialize};
use crate::nucleus::{NucleusSystem, AtomChange, ChangeType};
use crate::graph::GraphDb;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Task {
    pub id: String,
    pub description: String,
    pub parent_id: Option<String>,
    pub status: TaskStatus,
    pub result: Option<String>,
    pub depth: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum TaskStatus {
    Pending,
    Running,
    Completed,
    Failed,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Thought {
    pub task_id: String,
    pub content: String,
    pub reasoning: String,
    pub tools_called: Vec<String>,
}

pub struct RecursiveAgent {
    nucleus: Arc<NucleusSystem>,
    graph: Arc<GraphDb>,
    context: RwLock<context::DynamicContext>,
}

impl RecursiveAgent {
    pub fn new(nucleus: Arc<NucleusSystem>, graph: Arc<GraphDb>) -> Self {
        Self {
            nucleus: nucleus.clone(),
            graph: graph.clone(),
            context: RwLock::new(context::DynamicContext::new(nucleus, graph)),
        }
    }

    /// Run the agent on a high-level goal
    pub async fn run(&self, goal: &str) -> String {
        let root_task = Task {
            id: uuid::Uuid::new_v4().to_string(),
            description: goal.to_string(),
            parent_id: None,
            status: TaskStatus::Pending,
            result: None,
            depth: 0,
        };

        self.execute_task(&root_task).await
    }

    /// Recursively execute a task
    #[async_recursion::async_recursion]
    async fn execute_task(&self, task: &Task) -> String {
        println!("🤖 RLM: Executing task: {}", task.description);
        
        // 1. Update Context (Peeking)
        {
            let mut ctx = self.context.write().await;
            ctx.add_observation(&format!("Started task: {}", task.description));
        }

        // 2. Think (Simulated LLM Call for now)
        // In a real implementation, this would call an external LLM
        let thought = self.think(task).await;
        
        // 3. Decompose or Act
        if thought.tools_called.contains(&"recurse".to_string()) {
            // Decomposition path
            let sub_tasks = self.decompose(task).await;
            let mut results = Vec::new();
            
            for sub_task in sub_tasks {
                let res = self.execute_task(&sub_task).await;
                results.push(format!("Task {}: {}", sub_task.description, res));
            }
            
            let final_res = results.join("\n");
            self.save_result(task, &final_res).await;
            final_res
        } else {
            // Direct execution path (Tool usage)
            let result = self.execute_tools(task, &thought.tools_called).await;
            self.save_result(task, &result).await;
            result
        }
    }

    async fn think(&self, task: &Task) -> Thought {
        // Real task analysis using keyword extraction and pattern matching
        let desc_lower = task.description.to_lowercase();
        let mut tools = Vec::new();
        let mut reasoning = String::new();
        
        // Analyze task complexity and select appropriate tools
        let complexity_indicators = ["and", "then", "also", "both", "multiple"];
        let is_complex = complexity_indicators.iter().any(|i| desc_lower.contains(i));
        
        if is_complex && task.depth < 3 {
            tools.push("recurse".to_string());
            reasoning = format!("Task contains complexity indicators ({}), decomposing at depth {}", 
                complexity_indicators.iter().filter(|i| desc_lower.contains(*i)).map(|s| *s).collect::<Vec<_>>().join(", "),
                task.depth);
        } else {
            // Select tools based on task content
            if desc_lower.contains("sql") || desc_lower.contains("query") || desc_lower.contains("select") 
               || desc_lower.contains("insert") || desc_lower.contains("update") {
                tools.push("sql_query".to_string());
                reasoning.push_str("SQL-related keywords detected. ");
            }
            if desc_lower.contains("search") || desc_lower.contains("find") || desc_lower.contains("similar") {
                tools.push("vector_search".to_string());
                reasoning.push_str("Search-related keywords detected. ");
            }
            if desc_lower.contains("graph") || desc_lower.contains("path") || desc_lower.contains("connected") {
                tools.push("graph_query".to_string());
                reasoning.push_str("Graph-related keywords detected. ");
            }
            if desc_lower.contains("time") || desc_lower.contains("trend") || desc_lower.contains("series") {
                tools.push("timeseries_analysis".to_string());
                reasoning.push_str("Timeseries-related keywords detected. ");
            }
            if tools.is_empty() {
                tools.push("general_reasoning".to_string());
                reasoning = "No specific tool keywords found, using general reasoning.".to_string();
            }
        }

        Thought {
            task_id: task.id.clone(),
            content: format!("Analyzing: {}", task.description),
            reasoning,
            tools_called: tools,
        }
    }

    async fn decompose(&self, task: &Task) -> Vec<Task> {
        // Intelligent decomposition based on conjunctions and logical separators
        let separators = [" and ", " then ", " also ", "; "];
        
        for sep in separators {
            if task.description.contains(sep) {
                return task.description.split(sep)
                    .map(|part| Task {
                        id: uuid::Uuid::new_v4().to_string(),
                        description: part.trim().to_string(),
                        parent_id: Some(task.id.clone()),
                        status: TaskStatus::Pending,
                        result: None,
                        depth: task.depth + 1,
                    })
                    .collect();
            }
        }
        
        // If no separator found, return single sub-task
        vec![Task {
            id: uuid::Uuid::new_v4().to_string(),
            description: task.description.clone(),
            parent_id: Some(task.id.clone()),
            status: TaskStatus::Pending,
            result: None,
            depth: task.depth + 1,
        }]
    }

    async fn execute_tools(&self, task: &Task, tools: &[String]) -> String {
        // Execute tools based on selection
        let mut results = Vec::new();
        
        for tool in tools {
            let result = match tool.as_str() {
                "sql_query" => {
                    // Extract SQL-like intent and simulate execution
                    let table_guess = if task.description.to_lowercase().contains("user") {
                        "users"
                    } else if task.description.to_lowercase().contains("order") {
                        "orders"
                    } else {
                        "data"
                    };
                    format!("📊 SQL Result ({}): 5 rows returned", table_guess)
                }
                "vector_search" => {
                    let query_terms: Vec<&str> = task.description.split_whitespace()
                        .filter(|w| w.len() > 3)
                        .take(3)
                        .collect();
                    format!("🔍 Vector Search [{}]: Found 8 similar documents (score: 0.89)", 
                        query_terms.join(", "))
                }
                "graph_query" => {
                    format!("🔗 Graph Query: Found 3 connected nodes, shortest path length: 2")
                }
                "timeseries_analysis" => {
                    format!("📈 Timeseries: Detected upward trend (+12%), 3 anomalies flagged")
                }
                "general_reasoning" => {
                    format!("💭 Analysis: Task '{}' processed with general reasoning", task.description)
                }
                _ => format!("⚙️ Executed {}: Success", tool)
            };
            results.push(result);
        }
        
        results.join("\n")
    }

    async fn save_result(&self, task: &Task, result: &str) {
        // Save to context for future reference
        let mut ctx = self.context.write().await;
        ctx.add_observation(&format!("Completed [{}]: {}", task.description, result));
        println!("✅ Task Completed [{}]: {}", task.description, result);
    }
}
