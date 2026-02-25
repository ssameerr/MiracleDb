//! Aggregate Module - Custom aggregate functions

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

/// Aggregate state
pub trait AggregateState: Send + Sync {
    fn init() -> Self where Self: Sized;
    fn update(&mut self, value: &serde_json::Value);
    fn merge(&mut self, other: &Self) where Self: Sized;
    fn finalize(&self) -> serde_json::Value;
}

/// Sum aggregate
#[derive(Clone, Debug, Default)]
pub struct SumAggregate {
    sum: f64,
    count: usize,
}

impl AggregateState for SumAggregate {
    fn init() -> Self { Self::default() }
    
    fn update(&mut self, value: &serde_json::Value) {
        if let Some(n) = value.as_f64() {
            self.sum += n;
            self.count += 1;
        }
    }
    
    fn merge(&mut self, other: &Self) {
        self.sum += other.sum;
        self.count += other.count;
    }
    
    fn finalize(&self) -> serde_json::Value {
        serde_json::json!(self.sum)
    }
}

/// Average aggregate
#[derive(Clone, Debug, Default)]
pub struct AvgAggregate {
    sum: f64,
    count: usize,
}

impl AggregateState for AvgAggregate {
    fn init() -> Self { Self::default() }
    
    fn update(&mut self, value: &serde_json::Value) {
        if let Some(n) = value.as_f64() {
            self.sum += n;
            self.count += 1;
        }
    }
    
    fn merge(&mut self, other: &Self) {
        self.sum += other.sum;
        self.count += other.count;
    }
    
    fn finalize(&self) -> serde_json::Value {
        if self.count == 0 {
            serde_json::Value::Null
        } else {
            serde_json::json!(self.sum / self.count as f64)
        }
    }
}

/// Min aggregate
#[derive(Clone, Debug)]
pub struct MinAggregate {
    min: Option<f64>,
}

impl AggregateState for MinAggregate {
    fn init() -> Self { Self { min: None } }
    
    fn update(&mut self, value: &serde_json::Value) {
        if let Some(n) = value.as_f64() {
            self.min = Some(self.min.map(|m| m.min(n)).unwrap_or(n));
        }
    }
    
    fn merge(&mut self, other: &Self) {
        if let Some(other_min) = other.min {
            self.min = Some(self.min.map(|m| m.min(other_min)).unwrap_or(other_min));
        }
    }
    
    fn finalize(&self) -> serde_json::Value {
        self.min.map(|m| serde_json::json!(m)).unwrap_or(serde_json::Value::Null)
    }
}

/// Max aggregate
#[derive(Clone, Debug)]
pub struct MaxAggregate {
    max: Option<f64>,
}

impl AggregateState for MaxAggregate {
    fn init() -> Self { Self { max: None } }
    
    fn update(&mut self, value: &serde_json::Value) {
        if let Some(n) = value.as_f64() {
            self.max = Some(self.max.map(|m| m.max(n)).unwrap_or(n));
        }
    }
    
    fn merge(&mut self, other: &Self) {
        if let Some(other_max) = other.max {
            self.max = Some(self.max.map(|m| m.max(other_max)).unwrap_or(other_max));
        }
    }
    
    fn finalize(&self) -> serde_json::Value {
        self.max.map(|m| serde_json::json!(m)).unwrap_or(serde_json::Value::Null)
    }
}

/// Count aggregate
#[derive(Clone, Debug, Default)]
pub struct CountAggregate {
    count: usize,
}

impl AggregateState for CountAggregate {
    fn init() -> Self { Self::default() }
    
    fn update(&mut self, _value: &serde_json::Value) {
        self.count += 1;
    }
    
    fn merge(&mut self, other: &Self) {
        self.count += other.count;
    }
    
    fn finalize(&self) -> serde_json::Value {
        serde_json::json!(self.count)
    }
}

/// String aggregate (concatenation)
#[derive(Clone, Debug, Default)]
pub struct StringAggAggregate {
    values: Vec<String>,
    separator: String,
}

impl StringAggAggregate {
    pub fn with_separator(separator: &str) -> Self {
        Self {
            values: vec![],
            separator: separator.to_string(),
        }
    }
}

impl AggregateState for StringAggAggregate {
    fn init() -> Self { Self::default() }
    
    fn update(&mut self, value: &serde_json::Value) {
        if let Some(s) = value.as_str() {
            self.values.push(s.to_string());
        }
    }
    
    fn merge(&mut self, other: &Self) {
        self.values.extend(other.values.clone());
    }
    
    fn finalize(&self) -> serde_json::Value {
        serde_json::json!(self.values.join(&self.separator))
    }
}

/// Array aggregate
#[derive(Clone, Debug, Default)]
pub struct ArrayAggAggregate {
    values: Vec<serde_json::Value>,
}

impl AggregateState for ArrayAggAggregate {
    fn init() -> Self { Self::default() }
    
    fn update(&mut self, value: &serde_json::Value) {
        self.values.push(value.clone());
    }
    
    fn merge(&mut self, other: &Self) {
        self.values.extend(other.values.clone());
    }
    
    fn finalize(&self) -> serde_json::Value {
        serde_json::json!(self.values)
    }
}

/// Standard deviation aggregate
#[derive(Clone, Debug, Default)]
pub struct StdDevAggregate {
    values: Vec<f64>,
}

impl AggregateState for StdDevAggregate {
    fn init() -> Self { Self::default() }
    
    fn update(&mut self, value: &serde_json::Value) {
        if let Some(n) = value.as_f64() {
            self.values.push(n);
        }
    }
    
    fn merge(&mut self, other: &Self) {
        self.values.extend(other.values.clone());
    }
    
    fn finalize(&self) -> serde_json::Value {
        if self.values.is_empty() {
            return serde_json::Value::Null;
        }
        
        let mean = self.values.iter().sum::<f64>() / self.values.len() as f64;
        let variance = self.values.iter()
            .map(|v| (v - mean).powi(2))
            .sum::<f64>() / self.values.len() as f64;
        
        serde_json::json!(variance.sqrt())
    }
}

/// Percentile aggregate
#[derive(Clone, Debug)]
pub struct PercentileAggregate {
    values: Vec<f64>,
    percentile: f64,
}

impl PercentileAggregate {
    pub fn new(percentile: f64) -> Self {
        Self { values: vec![], percentile }
    }
}

impl AggregateState for PercentileAggregate {
    fn init() -> Self { Self::new(0.5) }
    
    fn update(&mut self, value: &serde_json::Value) {
        if let Some(n) = value.as_f64() {
            self.values.push(n);
        }
    }
    
    fn merge(&mut self, other: &Self) {
        self.values.extend(other.values.clone());
    }
    
    fn finalize(&self) -> serde_json::Value {
        if self.values.is_empty() {
            return serde_json::Value::Null;
        }
        
        let mut sorted = self.values.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        
        let idx = (self.percentile * sorted.len() as f64) as usize;
        let idx = idx.min(sorted.len() - 1);
        
        serde_json::json!(sorted[idx])
    }
}

/// Aggregate registry
pub struct AggregateRegistry {
    aggregates: RwLock<HashMap<String, AggregateInfo>>,
}

#[derive(Clone, Debug)]
pub struct AggregateInfo {
    pub name: String,
    pub description: String,
    pub input_types: Vec<String>,
    pub return_type: String,
}

impl AggregateRegistry {
    pub fn new() -> Self {
        Self {
            aggregates: RwLock::new(HashMap::new()),
        }
    }

    pub async fn register(&self, info: AggregateInfo) {
        let mut aggregates = self.aggregates.write().await;
        aggregates.insert(info.name.clone(), info);
    }

    pub async fn get(&self, name: &str) -> Option<AggregateInfo> {
        let aggregates = self.aggregates.read().await;
        aggregates.get(name).cloned()
    }

    pub async fn list(&self) -> Vec<AggregateInfo> {
        let aggregates = self.aggregates.read().await;
        aggregates.values().cloned().collect()
    }
}

impl Default for AggregateRegistry {
    fn default() -> Self {
        Self::new()
    }
}
