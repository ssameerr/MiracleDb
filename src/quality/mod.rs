//! Quality Module - Data validation and profiling

use std::collections::HashMap;
use serde::{Serialize, Deserialize};

/// Data profile statistics
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ColumnProfile {
    pub column_name: String,
    pub data_type: String,
    pub row_count: u64,
    pub null_count: u64,
    pub distinct_count: u64,
    pub min_value: Option<String>,
    pub max_value: Option<String>,
    pub avg_length: Option<f64>,
    pub patterns: Vec<(String, u32)>, // pattern -> count
}

/// Validation rule
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ValidationRule {
    pub name: String,
    pub column: String,
    pub rule_type: RuleType,
    pub enabled: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum RuleType {
    NotNull,
    Unique,
    Range { min: f64, max: f64 },
    Regex { pattern: String },
    ForeignKey { table: String, column: String },
    Custom { expression: String },
}

/// Validation result
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ValidationResult {
    pub rule_name: String,
    pub passed: bool,
    pub violations: u64,
    pub sample_violations: Vec<String>,
}

/// Data quality engine
pub struct QualityEngine {
    rules: tokio::sync::RwLock<Vec<ValidationRule>>,
}

impl QualityEngine {
    pub fn new() -> Self {
        Self {
            rules: tokio::sync::RwLock::new(Vec::new()),
        }
    }

    /// Profile column data
    pub fn profile_column(&self, column_name: &str, values: &[String]) -> ColumnProfile {
        let row_count = values.len() as u64;
        let null_count = values.iter().filter(|v| v.is_empty() || v.as_str() == "null").count() as u64;
        
        let mut distinct: std::collections::HashSet<&str> = std::collections::HashSet::new();
        let mut total_length = 0usize;
        let mut min_val: Option<&str> = None;
        let mut max_val: Option<&str> = None;

        for value in values {
            if !value.is_empty() {
                distinct.insert(value);
                total_length += value.len();
                
                min_val = Some(min_val.map_or(value.as_str(), |m| if value.as_str() < m { value } else { m }));
                max_val = Some(max_val.map_or(value.as_str(), |m| if value.as_str() > m { value } else { m }));
            }
        }

        let non_null_count = row_count - null_count;
        let avg_length = if non_null_count > 0 {
            Some(total_length as f64 / non_null_count as f64)
        } else {
            None
        };

        ColumnProfile {
            column_name: column_name.to_string(),
            data_type: "string".to_string(),
            row_count,
            null_count,
            distinct_count: distinct.len() as u64,
            min_value: min_val.map(|s| s.to_string()),
            max_value: max_val.map(|s| s.to_string()),
            avg_length,
            patterns: vec![],
        }
    }

    /// Add validation rule
    pub async fn add_rule(&self, rule: ValidationRule) {
        let mut rules = self.rules.write().await;
        rules.push(rule);
    }

    /// Validate data against rules
    pub async fn validate(&self, column: &str, values: &[String]) -> Vec<ValidationResult> {
        let rules = self.rules.read().await;
        let mut results = Vec::new();

        for rule in rules.iter().filter(|r| r.column == column && r.enabled) {
            let result = match &rule.rule_type {
                RuleType::NotNull => {
                    let violations = values.iter().filter(|v| v.is_empty()).count() as u64;
                    ValidationResult {
                        rule_name: rule.name.clone(),
                        passed: violations == 0,
                        violations,
                        sample_violations: vec![],
                    }
                }
                RuleType::Unique => {
                    let mut seen = std::collections::HashSet::new();
                    let mut duplicates = Vec::new();
                    for v in values {
                        if !seen.insert(v) {
                            duplicates.push(v.clone());
                        }
                    }
                    ValidationResult {
                        rule_name: rule.name.clone(),
                        passed: duplicates.is_empty(),
                        violations: duplicates.len() as u64,
                        sample_violations: duplicates.into_iter().take(5).collect(),
                    }
                }
                RuleType::Range { min, max } => {
                    let mut violations = 0u64;
                    let mut samples = Vec::new();
                    for v in values {
                        if let Ok(num) = v.parse::<f64>() {
                            if num < *min || num > *max {
                                violations += 1;
                                if samples.len() < 5 {
                                    samples.push(v.clone());
                                }
                            }
                        }
                    }
                    ValidationResult {
                        rule_name: rule.name.clone(),
                        passed: violations == 0,
                        violations,
                        sample_violations: samples,
                    }
                }
                RuleType::Regex { pattern } => {
                    let re = regex::Regex::new(pattern).unwrap();
                    let mut violations = 0u64;
                    let mut samples = Vec::new();
                    for v in values {
                        if !re.is_match(v) {
                            violations += 1;
                            if samples.len() < 5 {
                                samples.push(v.clone());
                            }
                        }
                    }
                    ValidationResult {
                        rule_name: rule.name.clone(),
                        passed: violations == 0,
                        violations,
                        sample_violations: samples,
                    }
                }
                _ => {
                    ValidationResult {
                        rule_name: rule.name.clone(),
                        passed: true,
                        violations: 0,
                        sample_violations: vec![],
                    }
                }
            };
            results.push(result);
        }

        results
    }

    /// Detect outliers using IQR method
    pub fn detect_outliers(&self, values: &[f64]) -> Vec<usize> {
        if values.len() < 4 {
            return vec![];
        }

        let mut sorted = values.to_vec();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let q1_idx = sorted.len() / 4;
        let q3_idx = 3 * sorted.len() / 4;
        
        let q1 = sorted[q1_idx];
        let q3 = sorted[q3_idx];
        let iqr = q3 - q1;
        
        let lower = q1 - 1.5 * iqr;
        let upper = q3 + 1.5 * iqr;

        values.iter()
            .enumerate()
            .filter(|(_, &v)| v < lower || v > upper)
            .map(|(i, _)| i)
            .collect()
    }

    /// Calculate completeness score
    pub fn completeness_score(&self, profile: &ColumnProfile) -> f64 {
        if profile.row_count == 0 {
            return 1.0;
        }
        1.0 - (profile.null_count as f64 / profile.row_count as f64)
    }
}

impl Default for QualityEngine {
    fn default() -> Self {
        Self::new()
    }
}
