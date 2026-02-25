//! Pipeline Module - Data processing pipelines

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

/// Pipeline stage
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PipelineStage {
    Filter { expression: String },
    Map { expression: String },
    FlatMap { expression: String },
    GroupBy { keys: Vec<String>, aggregations: Vec<Aggregation> },
    Sort { by: Vec<(String, SortOrder)> },
    Limit { n: usize },
    Skip { n: usize },
    Distinct { keys: Vec<String> },
    Join { right: String, on_left: String, on_right: String, join_type: JoinType },
    Window { partition_by: Vec<String>, order_by: Vec<String>, frame: WindowFrame },
    Sample { n: usize, method: String },
    Collect,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum SortOrder {
    Asc,
    Desc,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WindowFrame {
    pub start: FrameBound,
    pub end: FrameBound,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum FrameBound {
    UnboundedPreceding,
    Preceding(i64),
    CurrentRow,
    Following(i64),
    UnboundedFollowing,
}

/// Aggregation
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Aggregation {
    Count,
    Sum(String),
    Avg(String),
    Min(String),
    Max(String),
    First(String),
    Last(String),
    Collect(String),
}

/// Pipeline definition
#[derive(Clone, Debug)]
pub struct Pipeline {
    pub name: String,
    pub source: PipelineSource,
    pub stages: Vec<PipelineStage>,
    pub sink: PipelineSink,
}

#[derive(Clone, Debug)]
pub enum PipelineSource {
    Table(String),
    Query(String),
    Stream(String),
}

#[derive(Clone, Debug)]
pub enum PipelineSink {
    Table(String),
    Stream(String),
    Stdout,
    Null,
}

/// Pipeline executor
pub struct PipelineExecutor {
    pipelines: RwLock<HashMap<String, Pipeline>>,
}

impl PipelineExecutor {
    pub fn new() -> Self {
        Self {
            pipelines: RwLock::new(HashMap::new()),
        }
    }

    /// Register a pipeline
    pub async fn register(&self, pipeline: Pipeline) {
        let mut pipelines = self.pipelines.write().await;
        pipelines.insert(pipeline.name.clone(), pipeline);
    }

    /// Execute a pipeline
    pub async fn execute(&self, name: &str, input: Vec<serde_json::Value>) -> Result<Vec<serde_json::Value>, String> {
        let pipelines = self.pipelines.read().await;
        let pipeline = pipelines.get(name)
            .ok_or("Pipeline not found")?;

        let mut data = input;

        for stage in &pipeline.stages {
            data = self.execute_stage(stage, data)?;
        }

        Ok(data)
    }

    fn execute_stage(&self, stage: &PipelineStage, input: Vec<serde_json::Value>) -> Result<Vec<serde_json::Value>, String> {
        match stage {
            PipelineStage::Filter { expression } => {
                // Simple filter implementation
                Ok(input.into_iter()
                    .filter(|row| self.evaluate_filter(row, expression))
                    .collect())
            }
            PipelineStage::Map { expression } => {
                Ok(input.into_iter()
                    .map(|row| self.transform(row, expression))
                    .collect())
            }
            PipelineStage::Sort { by } => {
                let mut data = input;
                data.sort_by(|a, b| {
                    for (field, order) in by {
                        let cmp = self.compare_values(
                            a.get(field),
                            b.get(field),
                        );
                        if cmp != std::cmp::Ordering::Equal {
                            return match order {
                                SortOrder::Asc => cmp,
                                SortOrder::Desc => cmp.reverse(),
                            };
                        }
                    }
                    std::cmp::Ordering::Equal
                });
                Ok(data)
            }
            PipelineStage::Limit { n } => {
                Ok(input.into_iter().take(*n).collect())
            }
            PipelineStage::Skip { n } => {
                Ok(input.into_iter().skip(*n).collect())
            }
            PipelineStage::Distinct { keys } => {
                use std::collections::HashSet;
                let mut seen = HashSet::new();
                let mut result = Vec::new();

                for row in input {
                    let key: Vec<String> = keys.iter()
                        .filter_map(|k| row.get(k).map(|v| v.to_string()))
                        .collect();
                    let key_str = key.join("|");

                    if seen.insert(key_str) {
                        result.push(row);
                    }
                }

                Ok(result)
            }
            PipelineStage::GroupBy { keys, aggregations } => {
                self.group_by(input, keys, aggregations)
            }
            PipelineStage::Sample { n, .. } => {
                use rand::seq::SliceRandom;
                let mut data = input;
                let mut rng = rand::thread_rng();
                data.shuffle(&mut rng);
                Ok(data.into_iter().take(*n).collect())
            }
            _ => Ok(input),
        }
    }

    fn evaluate_filter(&self, row: &serde_json::Value, expression: &str) -> bool {
        // Simple expression evaluation
        // In production: use proper expression parser
        if expression.contains(" > ") {
            let parts: Vec<&str> = expression.split(" > ").collect();
            if parts.len() == 2 {
                if let Some(val) = row.get(parts[0].trim()) {
                    if let (Some(a), Ok(b)) = (val.as_f64(), parts[1].trim().parse::<f64>()) {
                        return a > b;
                    }
                }
            }
        }
        true
    }

    fn transform(&self, row: serde_json::Value, _expression: &str) -> serde_json::Value {
        // Placeholder transformation
        row
    }

    fn compare_values(&self, a: Option<&serde_json::Value>, b: Option<&serde_json::Value>) -> std::cmp::Ordering {
        match (a, b) {
            (Some(serde_json::Value::Number(a)), Some(serde_json::Value::Number(b))) => {
                a.as_f64().unwrap_or(0.0).partial_cmp(&b.as_f64().unwrap_or(0.0)).unwrap_or(std::cmp::Ordering::Equal)
            }
            (Some(serde_json::Value::String(a)), Some(serde_json::Value::String(b))) => {
                a.cmp(b)
            }
            _ => std::cmp::Ordering::Equal,
        }
    }

    fn group_by(&self, input: Vec<serde_json::Value>, keys: &[String], aggregations: &[Aggregation]) -> Result<Vec<serde_json::Value>, String> {
        use std::collections::HashMap;

        let mut groups: HashMap<String, Vec<serde_json::Value>> = HashMap::new();

        for row in input {
            let key: Vec<String> = keys.iter()
                .filter_map(|k| row.get(k).map(|v| v.to_string()))
                .collect();
            let key_str = key.join("|");
            groups.entry(key_str).or_insert_with(Vec::new).push(row);
        }

        let mut result = Vec::new();

        for (_, rows) in groups {
            if rows.is_empty() { continue; }

            let mut group_result = serde_json::Map::new();

            // Add key values
            if let Some(first) = rows.first() {
                for key in keys {
                    if let Some(val) = first.get(key) {
                        group_result.insert(key.clone(), val.clone());
                    }
                }
            }

            // Calculate aggregations
            for agg in aggregations {
                match agg {
                    Aggregation::Count => {
                        group_result.insert("count".to_string(), serde_json::json!(rows.len()));
                    }
                    Aggregation::Sum(field) => {
                        let sum: f64 = rows.iter()
                            .filter_map(|r| r.get(field).and_then(|v| v.as_f64()))
                            .sum();
                        group_result.insert(format!("sum_{}", field), serde_json::json!(sum));
                    }
                    Aggregation::Avg(field) => {
                        let vals: Vec<f64> = rows.iter()
                            .filter_map(|r| r.get(field).and_then(|v| v.as_f64()))
                            .collect();
                        if !vals.is_empty() {
                            let avg = vals.iter().sum::<f64>() / vals.len() as f64;
                            group_result.insert(format!("avg_{}", field), serde_json::json!(avg));
                        }
                    }
                    Aggregation::Min(field) => {
                        if let Some(min) = rows.iter()
                            .filter_map(|r| r.get(field).and_then(|v| v.as_f64()))
                            .reduce(f64::min) {
                            group_result.insert(format!("min_{}", field), serde_json::json!(min));
                        }
                    }
                    Aggregation::Max(field) => {
                        if let Some(max) = rows.iter()
                            .filter_map(|r| r.get(field).and_then(|v| v.as_f64()))
                            .reduce(f64::max) {
                            group_result.insert(format!("max_{}", field), serde_json::json!(max));
                        }
                    }
                    _ => {}
                }
            }

            result.push(serde_json::Value::Object(group_result));
        }

        Ok(result)
    }

    /// List all pipelines
    pub async fn list(&self) -> Vec<String> {
        let pipelines = self.pipelines.read().await;
        pipelines.keys().cloned().collect()
    }
}

impl Default for PipelineExecutor {
    fn default() -> Self {
        Self::new()
    }
}
