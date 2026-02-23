//! CDC (Change Data Capture) event types and webhook delivery.
//!
//! Provides:
//! - `ChangeOperation` enum: Insert, Update, Delete
//! - `ChangeEvent` struct with LSN and typed before/after rows
//! - `WebhookDelivery` with exponential backoff retry logic

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// The type of DML operation captured.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ChangeOperation {
    Insert,
    Update,
    Delete,
}

/// A single captured change event emitted by a CDC source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeEvent {
    /// Name of the table that changed.
    pub table: String,
    /// The DML operation (Insert / Update / Delete).
    pub operation: ChangeOperation,
    /// Column values before the change (None for Insert).
    pub before: Option<HashMap<String, serde_json::Value>>,
    /// Column values after the change (None for Delete).
    pub after: Option<HashMap<String, serde_json::Value>>,
    /// Wall-clock timestamp of the event.
    pub timestamp: chrono::DateTime<chrono::Utc>,
    /// Log Sequence Number from the source WAL, if available.
    pub lsn: Option<u64>,
}

/// Delivers `ChangeEvent`s to an HTTP webhook endpoint with
/// exponential-backoff retries.
pub struct WebhookDelivery {
    /// Target URL to POST events to.
    pub url: String,
    /// Maximum number of retry attempts after the first failure.
    pub max_retries: u32,
}

impl WebhookDelivery {
    /// Create a new `WebhookDelivery` targeting `url` with 3 retries.
    pub fn new(url: &str) -> Self {
        Self {
            url: url.to_string(),
            max_retries: 3,
        }
    }

    /// POST `event` as JSON to the configured URL.
    ///
    /// On failure the delivery is retried up to `max_retries` times with
    /// exponential backoff (100 ms × 2^attempt).  Returns `Ok(())` on the
    /// first successful response (2xx), or an `Err` with the last failure
    /// message after all retries are exhausted.
    pub async fn deliver(&self, event: &ChangeEvent) -> Result<(), String> {
        let client = reqwest::Client::new();
        let mut last_err = String::new();

        for attempt in 0..=self.max_retries {
            if attempt > 0 {
                let backoff = std::time::Duration::from_millis(100 * (1 << attempt));
                tokio::time::sleep(backoff).await;
            }

            match client.post(&self.url).json(event).send().await {
                Ok(resp) if resp.status().is_success() => return Ok(()),
                Ok(resp) => last_err = format!("HTTP {}", resp.status()),
                Err(e) => last_err = e.to_string(),
            }
        }

        Err(format!(
            "Failed after {} retries: {}",
            self.max_retries, last_err
        ))
    }
}

/// CDC event type from a PostgreSQL WAL source.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CdcEventType {
    Insert,
    Update,
    Delete,
    Truncate,
}

/// A CDC event as emitted by the PostgreSQL WAL decoder.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcEvent {
    pub event_type: CdcEventType,
    pub table: String,
    /// Row state before the change (None for inserts).
    pub before: Option<serde_json::Value>,
    /// Row state after the change (None for deletes).
    pub after: Option<serde_json::Value>,
    /// Unix timestamp (seconds since epoch).
    pub timestamp: i64,
}

/// WAL operation decoded from PostgreSQL logical replication.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum WalOperation {
    Insert,
    Update,
    Delete,
    Begin,
    Commit,
}

/// A single decoded WAL record from PostgreSQL logical replication.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalRecord {
    /// Log Sequence Number.
    pub lsn: u64,
    pub operation: WalOperation,
    pub table: String,
    pub data: serde_json::Value,
}

/// Configuration for connecting to a PostgreSQL source for CDC via logical replication.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresCdcConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub username: String,
    pub password: String,
    /// Logical replication slot name.
    pub slot_name: String,
    /// Publication name (pg_publication).
    pub publication: String,
}

#[cfg(test)]
mod cdc_tests {
    use super::*;

    #[test]
    fn test_cdc_event_insert_creation() {
        let event = CdcEvent {
            event_type: CdcEventType::Insert,
            table: "users".to_string(),
            before: None,
            after: Some(serde_json::json!({"id": 1, "name": "Alice"})),
            timestamp: 1700000000,
        };
        assert_eq!(event.event_type, CdcEventType::Insert);
        assert!(event.after.is_some());
        assert!(event.before.is_none());
    }

    #[test]
    fn test_cdc_event_update_has_before_after() {
        let event = CdcEvent {
            event_type: CdcEventType::Update,
            table: "users".to_string(),
            before: Some(serde_json::json!({"id": 1, "name": "Alice"})),
            after: Some(serde_json::json!({"id": 1, "name": "Bob"})),
            timestamp: 1700000001,
        };
        assert_eq!(event.event_type, CdcEventType::Update);
        assert!(event.before.is_some());
        assert!(event.after.is_some());
    }

    #[test]
    fn test_cdc_event_delete_has_no_after() {
        let event = CdcEvent {
            event_type: CdcEventType::Delete,
            table: "users".to_string(),
            before: Some(serde_json::json!({"id": 1})),
            after: None,
            timestamp: 1700000002,
        };
        assert_eq!(event.event_type, CdcEventType::Delete);
        assert!(event.before.is_some());
        assert!(event.after.is_none());
    }

    #[test]
    fn test_cdc_source_config() {
        let config = PostgresCdcConfig {
            host: "localhost".to_string(),
            port: 5432,
            database: "mydb".to_string(),
            username: "replicator".to_string(),
            password: "secret".to_string(),
            slot_name: "miracle_slot".to_string(),
            publication: "miracle_pub".to_string(),
        };
        assert_eq!(config.host, "localhost");
        assert_eq!(config.port, 5432);
        assert_eq!(config.slot_name, "miracle_slot");
    }

    #[test]
    fn test_wal_decode_insert() {
        let record = WalRecord {
            lsn: 12345,
            operation: WalOperation::Insert,
            table: "orders".to_string(),
            data: serde_json::json!({"id": 42, "amount": 99.99}),
        };
        assert_eq!(record.operation, WalOperation::Insert);
        assert_eq!(record.table, "orders");
        assert_eq!(record.lsn, 12345);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn change_event_serializes() {
        let event = ChangeEvent {
            table: "users".to_string(),
            operation: ChangeOperation::Insert,
            before: None,
            after: Some({
                let mut m = HashMap::new();
                m.insert("id".to_string(), serde_json::json!(1));
                m.insert("name".to_string(), serde_json::json!("Alice"));
                m
            }),
            timestamp: chrono::Utc::now(),
            lsn: Some(42),
        };

        let json = serde_json::to_string(&event).expect("serialize");
        assert!(json.contains("Insert"));
        assert!(json.contains("users"));
    }

    #[test]
    fn webhook_delivery_new() {
        let wd = WebhookDelivery::new("http://localhost:9000/hook");
        assert_eq!(wd.url, "http://localhost:9000/hook");
        assert_eq!(wd.max_retries, 3);
    }

    #[test]
    fn change_operation_equality() {
        assert_eq!(ChangeOperation::Insert, ChangeOperation::Insert);
        assert_ne!(ChangeOperation::Insert, ChangeOperation::Delete);
    }
}
