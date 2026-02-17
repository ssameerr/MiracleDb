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
