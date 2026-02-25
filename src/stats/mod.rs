//! Stats Module - Table and database statistics

use std::collections::HashMap;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

/// Column statistics
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ColumnStats {
    pub name: String,
    pub data_type: String,
    pub null_count: u64,
    pub distinct_count: u64,
    pub min_value: Option<serde_json::Value>,
    pub max_value: Option<serde_json::Value>,
    pub avg_length: Option<f64>,
    pub histogram: Option<Histogram>,
}

/// Histogram for value distribution
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Histogram {
    pub buckets: Vec<HistogramBucket>,
    pub num_distinct: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HistogramBucket {
    pub lower_bound: serde_json::Value,
    pub upper_bound: serde_json::Value,
    pub count: u64,
    pub distinct_count: u64,
}

/// Table statistics
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TableStats {
    pub name: String,
    pub row_count: u64,
    pub size_bytes: u64,
    pub columns: Vec<ColumnStats>,
    pub last_analyzed: i64,
    pub sample_percent: Option<f32>,
}

/// Database statistics
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DatabaseStats {
    pub total_tables: u64,
    pub total_rows: u64,
    pub total_size_bytes: u64,
    pub index_count: u64,
    pub cache_hit_rate: f64,
    pub avg_query_time_ms: f64,
}

/// Statistics manager
pub struct StatsManager {
    table_stats: RwLock<HashMap<String, TableStats>>,
    db_stats: RwLock<DatabaseStats>,
    sample_size: usize,
}

impl StatsManager {
    pub fn new(sample_size: usize) -> Self {
        Self {
            table_stats: RwLock::new(HashMap::new()),
            db_stats: RwLock::new(DatabaseStats::default()),
            sample_size,
        }
    }

    /// Analyze a table to gather statistics
    pub async fn analyze_table(&self, table: &str, rows: &[serde_json::Value]) -> TableStats {
        let row_count = rows.len() as u64;
        let mut columns = HashMap::new();

        for row in rows {
            if let Some(obj) = row.as_object() {
                for (key, value) in obj {
                    let col = columns.entry(key.clone()).or_insert_with(|| ColumnStats {
                        name: key.clone(),
                        data_type: Self::infer_type(value),
                        null_count: 0,
                        distinct_count: 0,
                        min_value: None,
                        max_value: None,
                        avg_length: None,
                        histogram: None,
                    });

                    if value.is_null() {
                        col.null_count += 1;
                    }

                    // Update min/max
                    if col.min_value.is_none() || Self::compare_values(value, col.min_value.as_ref().unwrap()) < 0 {
                        col.min_value = Some(value.clone());
                    }
                    if col.max_value.is_none() || Self::compare_values(value, col.max_value.as_ref().unwrap()) > 0 {
                        col.max_value = Some(value.clone());
                    }
                }
            }
        }

        let stats = TableStats {
            name: table.to_string(),
            row_count,
            size_bytes: rows.len() as u64 * 100, // Estimate
            columns: columns.into_values().collect(),
            last_analyzed: chrono::Utc::now().timestamp(),
            sample_percent: Some(100.0),
        };

        // Store stats
        let mut table_stats = self.table_stats.write().await;
        table_stats.insert(table.to_string(), stats.clone());

        stats
    }

    fn infer_type(value: &serde_json::Value) -> String {
        match value {
            serde_json::Value::Null => "null".to_string(),
            serde_json::Value::Bool(_) => "boolean".to_string(),
            serde_json::Value::Number(n) => {
                if n.is_i64() { "integer".to_string() }
                else { "float".to_string() }
            }
            serde_json::Value::String(_) => "string".to_string(),
            serde_json::Value::Array(_) => "array".to_string(),
            serde_json::Value::Object(_) => "object".to_string(),
        }
    }

    fn compare_values(a: &serde_json::Value, b: &serde_json::Value) -> i32 {
        match (a, b) {
            (serde_json::Value::Number(a), serde_json::Value::Number(b)) => {
                let a = a.as_f64().unwrap_or(0.0);
                let b = b.as_f64().unwrap_or(0.0);
                a.partial_cmp(&b).map(|o| o as i32).unwrap_or(0)
            }
            (serde_json::Value::String(a), serde_json::Value::String(b)) => {
                a.cmp(b) as i32
            }
            _ => 0,
        }
    }

    /// Get table statistics
    pub async fn get_table_stats(&self, table: &str) -> Option<TableStats> {
        let stats = self.table_stats.read().await;
        stats.get(table).cloned()
    }

    /// Get all table statistics
    pub async fn get_all_table_stats(&self) -> Vec<TableStats> {
        let stats = self.table_stats.read().await;
        stats.values().cloned().collect()
    }

    /// Update database-level statistics
    pub async fn update_db_stats(&self, stats: DatabaseStats) {
        let mut db_stats = self.db_stats.write().await;
        *db_stats = stats;
    }

    /// Get database statistics
    pub async fn get_db_stats(&self) -> DatabaseStats {
        let stats = self.db_stats.read().await;
        stats.clone()
    }

    /// Estimate selectivity for a predicate
    pub async fn estimate_selectivity(&self, table: &str, column: &str, operator: &str, value: &serde_json::Value) -> f64 {
        let stats = self.table_stats.read().await;
        let table_stats = match stats.get(table) {
            Some(s) => s,
            None => return 0.1, // Default estimate
        };

        let col_stats = table_stats.columns.iter().find(|c| c.name == column);
        let col_stats = match col_stats {
            Some(s) => s,
            None => return 0.1,
        };

        match operator {
            "=" => {
                if col_stats.distinct_count > 0 {
                    1.0 / col_stats.distinct_count as f64
                } else {
                    0.1
                }
            }
            "<" | "<=" | ">" | ">=" => 0.33,
            "!=" => {
                if col_stats.distinct_count > 0 {
                    1.0 - (1.0 / col_stats.distinct_count as f64)
                } else {
                    0.9
                }
            }
            "LIKE" => 0.1,
            "IN" => 0.2,
            _ => 0.1,
        }
    }

    /// Build histogram for a column
    pub async fn build_histogram(&self, table: &str, column: &str, num_buckets: usize, values: &[serde_json::Value]) -> Histogram {
        let mut sorted = values.to_vec();
        sorted.sort_by(|a, b| Self::compare_values(a, b).cmp(&0));

        let bucket_size = (sorted.len() / num_buckets).max(1);
        let mut buckets = Vec::new();

        for chunk in sorted.chunks(bucket_size) {
            if chunk.is_empty() { continue; }
            let bucket = HistogramBucket {
                lower_bound: chunk.first().unwrap().clone(),
                upper_bound: chunk.last().unwrap().clone(),
                count: chunk.len() as u64,
                distinct_count: chunk.iter().collect::<std::collections::HashSet<_>>().len() as u64,
            };
            buckets.push(bucket);
        }

        Histogram {
            buckets,
            num_distinct: sorted.iter().collect::<std::collections::HashSet<_>>().len() as u64,
        }
    }
}

impl Default for StatsManager {
    fn default() -> Self {
        Self::new(10000)
    }
}
