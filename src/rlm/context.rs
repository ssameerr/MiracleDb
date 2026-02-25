use std::collections::VecDeque;
use std::sync::Arc;
use crate::nucleus::NucleusSystem;
use crate::graph::GraphDb;
use serde::{Serialize, Deserialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ContextItem {
    pub content: String,
    pub importance: f64,
    pub source: String,
    pub timestamp: i64,
}

pub struct DynamicContext {
    nucleus: Arc<NucleusSystem>,
    graph: Arc<GraphDb>,
    short_term_memory: VecDeque<ContextItem>,
    max_items: usize,
}

impl DynamicContext {
    pub fn new(nucleus: Arc<NucleusSystem>, graph: Arc<GraphDb>) -> Self {
        Self {
            nucleus,
            graph,
            short_term_memory: VecDeque::new(),
            max_items: 20,
        }
    }

    pub fn add_observation(&mut self, content: &str) {
        let item = ContextItem {
            content: content.to_string(),
            importance: 1.0, // Default importance
            source: "agent_observation".to_string(),
            timestamp: chrono::Utc::now().timestamp(),
        };
        
        if self.short_term_memory.len() >= self.max_items {
            self.short_term_memory.pop_front();
        }
        self.short_term_memory.push_back(item);
    }

    pub async fn get_relevant_context(&self, query: &str) -> String {
        // 1. Get recent STM
        let stm: Vec<String> = self.short_term_memory.iter()
            .map(|item| format!("- {}", item.content))
            .collect();

        // 2. (Stub) Retrieve long-term memory from Nucleus/Graph
        // In real impl, we would use vector search here.
        let ltm = vec!["No relevant long-term memories found.".to_string()];

        format!(
            "Current Context:\n{}\n\nRelevant Memories:\n{}",
            stm.join("\n"),
            ltm.join("\n")
        )
    }
}
