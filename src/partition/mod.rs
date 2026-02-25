//! Partitioning Module - Table partitioning strategies

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

/// Partition strategy
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PartitionStrategy {
    Range {
        column: String,
        boundaries: Vec<PartitionBoundary>,
    },
    List {
        column: String,
        values: HashMap<String, Vec<serde_json::Value>>,
    },
    Hash {
        column: String,
        num_partitions: u32,
    },
    None,
}

/// Range partition boundary
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PartitionBoundary {
    pub partition_name: String,
    pub lower_bound: Option<serde_json::Value>,
    pub upper_bound: Option<serde_json::Value>,
}

/// Partition metadata
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Partition {
    pub name: String,
    pub table: String,
    pub strategy: PartitionStrategy,
    pub created_at: i64,
    pub row_count: u64,
    pub size_bytes: u64,
}

/// Partition manager
pub struct PartitionManager {
    partitions: RwLock<HashMap<String, Vec<Partition>>>, // table -> partitions
    strategies: RwLock<HashMap<String, PartitionStrategy>>,
}

impl PartitionManager {
    pub fn new() -> Self {
        Self {
            partitions: RwLock::new(HashMap::new()),
            strategies: RwLock::new(HashMap::new()),
        }
    }

    /// Set partition strategy for a table
    pub async fn set_strategy(&self, table: &str, strategy: PartitionStrategy) {
        let mut strategies = self.strategies.write().await;
        strategies.insert(table.to_string(), strategy);
    }

    /// Create a partition
    pub async fn create_partition(&self, table: &str, name: &str) -> Result<(), String> {
        let strategies = self.strategies.read().await;
        let strategy = strategies.get(table)
            .cloned()
            .unwrap_or(PartitionStrategy::None);
        drop(strategies);

        let partition = Partition {
            name: name.to_string(),
            table: table.to_string(),
            strategy,
            created_at: chrono::Utc::now().timestamp(),
            row_count: 0,
            size_bytes: 0,
        };

        let mut partitions = self.partitions.write().await;
        partitions.entry(table.to_string())
            .or_insert_with(Vec::new)
            .push(partition);

        Ok(())
    }

    /// Drop a partition
    pub async fn drop_partition(&self, table: &str, name: &str) -> Result<(), String> {
        let mut partitions = self.partitions.write().await;
        if let Some(parts) = partitions.get_mut(table) {
            parts.retain(|p| p.name != name);
        }
        Ok(())
    }

    /// Route a row to the correct partition
    pub async fn route(&self, table: &str, row: &serde_json::Value) -> Option<String> {
        let strategies = self.strategies.read().await;
        let strategy = strategies.get(table)?;

        match strategy {
            PartitionStrategy::Range { column, boundaries } => {
                let value = row.get(column)?;
                for boundary in boundaries {
                    let in_range = match (&boundary.lower_bound, &boundary.upper_bound) {
                        (Some(lower), Some(upper)) => 
                            Self::compare_values(value, lower).map_or(false, |o| o.is_ge()) && 
                            Self::compare_values(value, upper).map_or(false, |o| o.is_lt()),
                        (Some(lower), None) => Self::compare_values(value, lower).map_or(false, |o| o.is_ge()),
                        (None, Some(upper)) => Self::compare_values(value, upper).map_or(false, |o| o.is_lt()),
                        (None, None) => true,
                    };
                    if in_range {
                        return Some(boundary.partition_name.clone());
                    }
                }
                None
            }
            PartitionStrategy::List { column, values } => {
                let value = row.get(column)?;
                for (partition_name, partition_values) in values {
                    if partition_values.contains(value) {
                        return Some(partition_name.clone());
                    }
                }
                None
            }
            PartitionStrategy::Hash { column, num_partitions } => {
                let value = row.get(column)?;
                let hash = Self::hash_value(value);
                let partition_num = hash % (*num_partitions as u64);
                Some(format!("{}_{}", table, partition_num))
            }
            PartitionStrategy::None => None,
        }
    }

    fn hash_value(value: &serde_json::Value) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        value.to_string().hash(&mut hasher);
        hasher.finish()
    }

    /// Get partitions for a table
    pub async fn get_partitions(&self, table: &str) -> Vec<Partition> {
        let partitions = self.partitions.read().await;
        partitions.get(table).cloned().unwrap_or_default()
    }

    /// Prune partitions for a query predicate
    pub async fn prune(&self, table: &str, predicate: &serde_json::Value) -> Vec<String> {
        let partitions = self.get_partitions(table).await;
        let strategies = self.strategies.read().await;

        match strategies.get(table) {
            Some(PartitionStrategy::Range { column, boundaries }) => {
                // Simple pruning: check if predicate value falls in range
                if let Some(value) = predicate.get(column) {
                    boundaries.iter()
                        .filter(|b| {
                            let in_range = match (&b.lower_bound, &b.upper_bound) {
                                (Some(lower), Some(upper)) => 
                                    Self::compare_values(value, lower).map_or(false, |o| o.is_ge()) && 
                                    Self::compare_values(value, upper).map_or(false, |o| o.is_lt()),
                                (Some(lower), None) => Self::compare_values(value, lower).map_or(false, |o| o.is_ge()),
                                (None, Some(upper)) => Self::compare_values(value, upper).map_or(false, |o| o.is_lt()),
                                (None, None) => true,
                            };
                            in_range
                        })
                        .map(|b| b.partition_name.clone())
                        .collect()
                } else {
                    partitions.iter().map(|p| p.name.clone()).collect()
                }
            }
            _ => partitions.iter().map(|p| p.name.clone()).collect(),
        }
    }

    /// Update partition statistics
    pub async fn update_stats(&self, table: &str, partition: &str, rows: u64, bytes: u64) {
        let mut partitions = self.partitions.write().await;
        if let Some(parts) = partitions.get_mut(table) {
            if let Some(p) = parts.iter_mut().find(|p| p.name == partition) {
                p.row_count = rows;
                p.size_bytes = bytes;
            }
        }
    }
    /// Compare two JSON values
    fn compare_values(a: &serde_json::Value, b: &serde_json::Value) -> Option<std::cmp::Ordering> {
        match (a, b) {
            (serde_json::Value::Number(n1), serde_json::Value::Number(n2)) => {
                if let (Some(f1), Some(f2)) = (n1.as_f64(), n2.as_f64()) {
                    f1.partial_cmp(&f2)
                } else if let (Some(i1), Some(i2)) = (n1.as_i64(), n2.as_i64()) {
                    Some(i1.cmp(&i2))
                } else {
                    None
                }
            },
            (serde_json::Value::String(s1), serde_json::Value::String(s2)) => Some(s1.cmp(s2)),
            _ => None
        }
    }
}

impl Default for PartitionManager {
    fn default() -> Self {
        Self::new()
    }
}
