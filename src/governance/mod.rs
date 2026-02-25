//! Data Governance Module - PII detection, retention policies, GDPR compliance

use std::collections::{HashMap, HashSet};
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};
use regex::Regex;

/// Data classification levels
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DataClassification {
    Public,
    Internal,
    Confidential,
    Restricted,
    PII,
    PHI,
    PCI,
}

/// Column classification
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ColumnClassification {
    pub table: String,
    pub column: String,
    pub classification: DataClassification,
    pub detected_patterns: Vec<String>,
    pub retention_days: Option<i64>,
}

/// Retention policy
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RetentionPolicy {
    pub name: String,
    pub table_pattern: String,
    pub retention_days: i64,
    pub action: RetentionAction,
    pub enabled: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum RetentionAction {
    Delete,
    Archive,
    Anonymize,
}

/// Consent record for GDPR
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConsentRecord {
    pub user_id: String,
    pub consent_type: String,
    pub granted: bool,
    pub granted_at: Option<i64>,
    pub revoked_at: Option<i64>,
    pub purpose: String,
}

/// Data subject request (GDPR)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DataSubjectRequest {
    pub id: String,
    pub user_id: String,
    pub request_type: DSRType,
    pub status: DSRStatus,
    pub created_at: i64,
    pub completed_at: Option<i64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum DSRType {
    Access,
    Rectification,
    Erasure,
    Portability,
    Restriction,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum DSRStatus {
    Pending,
    InProgress,
    Completed,
    Rejected,
}

/// PII detector patterns
pub struct PIIDetector {
    patterns: Vec<(String, Regex, DataClassification)>,
}

impl PIIDetector {
    pub fn new() -> Self {
        let patterns = vec![
            ("email".to_string(), Regex::new(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}").unwrap(), DataClassification::PII),
            ("ssn".to_string(), Regex::new(r"\d{3}-\d{2}-\d{4}").unwrap(), DataClassification::PII),
            ("phone".to_string(), Regex::new(r"\+?1?[-.\s]?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}|\d{3}[-.\s]?\d{4}").unwrap(), DataClassification::PII),
            ("credit_card".to_string(), Regex::new(r"\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}").unwrap(), DataClassification::PCI),
            ("ip_address".to_string(), Regex::new(r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}").unwrap(), DataClassification::PII),
        ];
        Self { patterns }
    }

    /// Detect PII in text
    pub fn detect(&self, text: &str) -> Vec<(String, DataClassification)> {
        let mut detected = Vec::new();
        for (name, pattern, classification) in &self.patterns {
            if pattern.is_match(text) {
                detected.push((name.clone(), classification.clone()));
            }
        }
        detected
    }

    /// Scan column data and classify
    pub fn scan_column(&self, values: &[String]) -> Option<DataClassification> {
        let mut classifications = HashMap::new();
        
        for value in values.iter().take(100) { // Sample first 100 values
            for (name, class) in self.detect(value) {
                *classifications.entry(class).or_insert(0) += 1;
            }
        }

        // Return highest classification if any detected
        if classifications.contains_key(&DataClassification::PCI) {
            Some(DataClassification::PCI)
        } else if classifications.contains_key(&DataClassification::PHI) {
            Some(DataClassification::PHI)
        } else if classifications.contains_key(&DataClassification::PII) {
            Some(DataClassification::PII)
        } else {
            None
        }
    }
}

impl Default for PIIDetector {
    fn default() -> Self {
        Self::new()
    }
}

/// Data governance manager
pub struct GovernanceManager {
    classifications: RwLock<HashMap<String, ColumnClassification>>,
    retention_policies: RwLock<Vec<RetentionPolicy>>,
    consent_records: RwLock<HashMap<String, Vec<ConsentRecord>>>,
    dsr_requests: RwLock<Vec<DataSubjectRequest>>,
    pii_detector: PIIDetector,
}

impl GovernanceManager {
    pub fn new() -> Self {
        Self {
            classifications: RwLock::new(HashMap::new()),
            retention_policies: RwLock::new(Vec::new()),
            consent_records: RwLock::new(HashMap::new()),
            dsr_requests: RwLock::new(Vec::new()),
            pii_detector: PIIDetector::new(),
        }
    }

    /// Classify a column
    pub async fn classify_column(&self, table: &str, column: &str, data_sample: &[String]) -> ColumnClassification {
        let classification = self.pii_detector.scan_column(data_sample)
            .unwrap_or(DataClassification::Internal);
        
        let col_class = ColumnClassification {
            table: table.to_string(),
            column: column.to_string(),
            classification,
            detected_patterns: vec![],
            retention_days: None,
        };

        let key = format!("{}.{}", table, column);
        let mut classifications = self.classifications.write().await;
        classifications.insert(key, col_class.clone());

        col_class
    }

    /// Add retention policy
    pub async fn add_retention_policy(&self, policy: RetentionPolicy) {
        let mut policies = self.retention_policies.write().await;
        policies.push(policy);
    }

    /// Get applicable retention policy for a table
    pub async fn get_retention_policy(&self, table: &str) -> Option<RetentionPolicy> {
        let policies = self.retention_policies.read().await;
        for policy in policies.iter() {
            if table.contains(&policy.table_pattern) || policy.table_pattern == "*" {
                return Some(policy.clone());
            }
        }
        None
    }

    /// Record user consent
    pub async fn record_consent(&self, user_id: &str, consent: ConsentRecord) {
        let mut records = self.consent_records.write().await;
        records.entry(user_id.to_string())
            .or_insert_with(Vec::new)
            .push(consent);
    }

    /// Check if user has given consent
    pub async fn has_consent(&self, user_id: &str, consent_type: &str) -> bool {
        let records = self.consent_records.read().await;
        if let Some(user_consents) = records.get(user_id) {
            for consent in user_consents.iter().rev() {
                if consent.consent_type == consent_type {
                    return consent.granted && consent.revoked_at.is_none();
                }
            }
        }
        false
    }

    /// Create data subject request (GDPR)
    pub async fn create_dsr(&self, user_id: &str, request_type: DSRType) -> DataSubjectRequest {
        let request = DataSubjectRequest {
            id: uuid::Uuid::new_v4().to_string(),
            user_id: user_id.to_string(),
            request_type,
            status: DSRStatus::Pending,
            created_at: chrono::Utc::now().timestamp(),
            completed_at: None,
        };

        let mut requests = self.dsr_requests.write().await;
        requests.push(request.clone());
        request
    }

    /// Handle right to erasure (GDPR Article 17)
    pub async fn execute_erasure(&self, user_id: &str) -> Result<Vec<String>, String> {
        // In production: would iterate through all tables and delete user data
        let affected_tables = vec!["users".to_string(), "orders".to_string(), "sessions".to_string()];
        
        // Mark DSR as completed
        let mut requests = self.dsr_requests.write().await;
        for request in requests.iter_mut() {
            if request.user_id == user_id && matches!(request.request_type, DSRType::Erasure) {
                request.status = DSRStatus::Completed;
                request.completed_at = Some(chrono::Utc::now().timestamp());
            }
        }

        Ok(affected_tables)
    }

    /// Export user data (GDPR Article 20 - Data Portability)
    pub async fn export_user_data(&self, user_id: &str) -> Result<serde_json::Value, String> {
        // In production: would gather all user data from all tables
        let export = serde_json::json!({
            "user_id": user_id,
            "exported_at": chrono::Utc::now().to_rfc3339(),
            "data": {
                "profile": {},
                "preferences": {},
                "activity": []
            }
        });

        Ok(export)
    }

    /// Get all classified columns
    pub async fn get_all_classifications(&self) -> Vec<ColumnClassification> {
        let classifications = self.classifications.read().await;
        classifications.values().cloned().collect()
    }

    /// Get columns by classification
    pub async fn get_columns_by_classification(&self, class: DataClassification) -> Vec<ColumnClassification> {
        let classifications = self.classifications.read().await;
        classifications.values()
            .filter(|c| c.classification == class)
            .cloned()
            .collect()
    }
}

impl Default for GovernanceManager {
    fn default() -> Self {
        Self::new()
    }
}
