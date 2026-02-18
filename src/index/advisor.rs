use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IndexRecommendation {
    CreateIndex { table: String, columns: Vec<String>, reason: String },
    DropIndex { name: String, reason: String },
    CreatePartialIndex { table: String, columns: Vec<String>, filter: String, reason: String },
}

pub struct IndexAdvisor {
    pub slow_query_threshold_ms: u64,
}

impl IndexAdvisor {
    pub fn new(slow_query_threshold_ms: u64) -> Self {
        Self { slow_query_threshold_ms }
    }

    /// Analyze a query and recommend indexes
    pub fn analyze(&self, sql: &str) -> Vec<IndexRecommendation> {
        let mut recs = vec![];
        let sql_lower = sql.to_lowercase();

        // Simple heuristic: WHERE clause columns without indexes
        if sql_lower.contains("where") {
            // Extract table and condition columns (simplified)
            if let Some(table) = self.extract_table(&sql_lower) {
                if sql_lower.contains("=") || sql_lower.contains("like") {
                    recs.push(IndexRecommendation::CreateIndex {
                        table,
                        columns: self.extract_filter_columns(&sql_lower),
                        reason: "WHERE clause column without index".to_string(),
                    });
                }
            }
        }
        recs
    }

    fn extract_table(&self, sql: &str) -> Option<String> {
        let from_pos = sql.find("from ")?;
        let rest = &sql[from_pos + 5..].trim_start();
        Some(rest.split_whitespace().next()?.to_string())
    }

    fn extract_filter_columns(&self, sql: &str) -> Vec<String> {
        // Simplified: extract word before '=' in WHERE clause
        if let Some(where_pos) = sql.find("where ") {
            let where_clause = &sql[where_pos + 6..];
            where_clause.split_whitespace()
                .filter(|w| !["=", "and", "or", "like", "in", "not"].contains(w))
                .take(2)
                .map(|s| s.trim_matches(|c: char| !c.is_alphanumeric() && c != '_').to_string())
                .filter(|s| !s.is_empty())
                .collect()
        } else {
            vec![]
        }
    }
}
