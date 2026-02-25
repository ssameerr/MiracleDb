//! Profiler Module - Query and system profiling

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

/// Query profile
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueryProfile {
    pub query_id: String,
    pub sql: String,
    pub start_time: i64,
    pub end_time: Option<i64>,
    pub duration_ms: Option<u64>,
    pub rows_scanned: u64,
    pub rows_returned: u64,
    pub bytes_processed: u64,
    pub stages: Vec<StageProfile>,
}

/// Execution stage profile
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StageProfile {
    pub name: String,
    pub start_ms: u64,
    pub duration_ms: u64,
    pub rows_in: u64,
    pub rows_out: u64,
    pub memory_bytes: u64,
}

/// Explain plan node
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExplainNode {
    pub node_type: String,
    pub description: String,
    pub estimated_rows: u64,
    pub estimated_cost: f64,
    pub children: Vec<ExplainNode>,
}

/// Active query info
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ActiveQuery {
    pub query_id: String,
    pub sql: String,
    pub user_id: String,
    pub started_at: i64,
    pub state: QueryState,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum QueryState {
    Parsing,
    Planning,
    Executing,
    Fetching,
    Complete,
    Failed,
}

/// Query profiler
pub struct Profiler {
    profiles: RwLock<HashMap<String, QueryProfile>>,
    active_queries: RwLock<HashMap<String, ActiveQuery>>,
    max_history: usize,
}

impl Profiler {
    pub fn new(max_history: usize) -> Self {
        Self {
            profiles: RwLock::new(HashMap::new()),
            active_queries: RwLock::new(HashMap::new()),
            max_history,
        }
    }

    /// Start profiling a query
    pub async fn start_query(&self, query_id: &str, sql: &str, user_id: &str) {
        let profile = QueryProfile {
            query_id: query_id.to_string(),
            sql: sql.to_string(),
            start_time: chrono::Utc::now().timestamp_millis(),
            end_time: None,
            duration_ms: None,
            rows_scanned: 0,
            rows_returned: 0,
            bytes_processed: 0,
            stages: vec![],
        };

        let active = ActiveQuery {
            query_id: query_id.to_string(),
            sql: sql.to_string(),
            user_id: user_id.to_string(),
            started_at: chrono::Utc::now().timestamp(),
            state: QueryState::Parsing,
        };

        let mut profiles = self.profiles.write().await;
        let mut active_queries = self.active_queries.write().await;

        profiles.insert(query_id.to_string(), profile);
        active_queries.insert(query_id.to_string(), active);
    }

    /// Update query state
    pub async fn update_state(&self, query_id: &str, state: QueryState) {
        let mut active_queries = self.active_queries.write().await;
        if let Some(query) = active_queries.get_mut(query_id) {
            query.state = state;
        }
    }

    /// Add stage to profile
    pub async fn add_stage(&self, query_id: &str, stage: StageProfile) {
        let mut profiles = self.profiles.write().await;
        if let Some(profile) = profiles.get_mut(query_id) {
            profile.stages.push(stage);
        }
    }

    /// Complete query profiling
    pub async fn complete_query(&self, query_id: &str, rows_returned: u64) {
        let end_time = chrono::Utc::now().timestamp_millis();

        let mut profiles = self.profiles.write().await;
        if let Some(profile) = profiles.get_mut(query_id) {
            profile.end_time = Some(end_time);
            profile.duration_ms = Some((end_time - profile.start_time) as u64);
            profile.rows_returned = rows_returned;
        }

        let mut active_queries = self.active_queries.write().await;
        active_queries.remove(query_id);

        // Trim old profiles
        if profiles.len() > self.max_history {
            let oldest = profiles.values()
                .min_by_key(|p| p.start_time)
                .map(|p| p.query_id.clone());
            if let Some(id) = oldest {
                profiles.remove(&id);
            }
        }
    }

    /// Get query profile
    pub async fn get_profile(&self, query_id: &str) -> Option<QueryProfile> {
        let profiles = self.profiles.read().await;
        profiles.get(query_id).cloned()
    }

    /// Get all active queries
    pub async fn get_active_queries(&self) -> Vec<ActiveQuery> {
        let active = self.active_queries.read().await;
        active.values().cloned().collect()
    }

    /// Get slow queries
    pub async fn get_slow_queries(&self, threshold_ms: u64) -> Vec<QueryProfile> {
        let profiles = self.profiles.read().await;
        profiles.values()
            .filter(|p| p.duration_ms.unwrap_or(0) >= threshold_ms)
            .cloned()
            .collect()
    }

    /// Generate EXPLAIN plan
    pub fn explain(&self, sql: &str) -> ExplainNode {
        // In production: would parse SQL and generate actual plan
        let sql_lower = sql.to_lowercase();
        
        let mut root = ExplainNode {
            node_type: "Query".to_string(),
            description: "Query Plan".to_string(),
            estimated_rows: 1000,
            estimated_cost: 100.0,
            children: vec![],
        };

        if sql_lower.contains("join") {
            root.children.push(ExplainNode {
                node_type: "HashJoin".to_string(),
                description: "Hash join on key".to_string(),
                estimated_rows: 500,
                estimated_cost: 50.0,
                children: vec![
                    ExplainNode {
                        node_type: "SeqScan".to_string(),
                        description: "Sequential scan".to_string(),
                        estimated_rows: 1000,
                        estimated_cost: 20.0,
                        children: vec![],
                    },
                    ExplainNode {
                        node_type: "IndexScan".to_string(),
                        description: "Index scan".to_string(),
                        estimated_rows: 500,
                        estimated_cost: 10.0,
                        children: vec![],
                    },
                ],
            });
        } else if sql_lower.contains("where") {
            root.children.push(ExplainNode {
                node_type: "Filter".to_string(),
                description: "Filter with predicate".to_string(),
                estimated_rows: 100,
                estimated_cost: 15.0,
                children: vec![
                    ExplainNode {
                        node_type: "SeqScan".to_string(),
                        description: "Sequential scan".to_string(),
                        estimated_rows: 1000,
                        estimated_cost: 10.0,
                        children: vec![],
                    },
                ],
            });
        } else {
            root.children.push(ExplainNode {
                node_type: "SeqScan".to_string(),
                description: "Sequential scan".to_string(),
                estimated_rows: 1000,
                estimated_cost: 10.0,
                children: vec![],
            });
        }

        root
    }

    /// Analyze query for optimization suggestions
    pub fn analyze(&self, sql: &str) -> Vec<String> {
        let sql_lower = sql.to_lowercase();
        let mut suggestions = Vec::new();

        if sql_lower.contains("select *") {
            suggestions.push("Consider selecting only needed columns instead of SELECT *".to_string());
        }

        if !sql_lower.contains("limit") && sql_lower.contains("select") {
            suggestions.push("Consider adding LIMIT to prevent fetching too many rows".to_string());
        }

        if sql_lower.contains("like '%") {
            suggestions.push("Leading wildcard in LIKE prevents index usage".to_string());
        }

        if sql_lower.contains("or") && sql_lower.contains("where") {
            suggestions.push("OR conditions may prevent index usage, consider UNION".to_string());
        }

        suggestions
    }
}

impl Default for Profiler {
    fn default() -> Self {
        Self::new(10000)
    }
}
