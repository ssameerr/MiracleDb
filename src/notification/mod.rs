//! Notification Module - Alert and notification system

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{RwLock, mpsc};
use serde::{Serialize, Deserialize};

/// Notification priority
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum Priority {
    Low,
    Normal,
    High,
    Critical,
}

/// Notification channel type
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ChannelType {
    Email { address: String },
    Webhook { url: String, headers: HashMap<String, String> },
    Slack { webhook_url: String, channel: String },
    PagerDuty { service_key: String },
    InApp { user_id: String },
}

/// Alert rule
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AlertRule {
    pub id: String,
    pub name: String,
    pub condition: AlertCondition,
    pub channels: Vec<String>,
    pub priority: Priority,
    pub enabled: bool,
    pub cooldown_seconds: u64,
    pub last_triggered: Option<i64>,
}

/// Alert condition
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum AlertCondition {
    MetricAbove { metric: String, threshold: f64 },
    MetricBelow { metric: String, threshold: f64 },
    ErrorRate { threshold: f64, window_seconds: u64 },
    Latency { percentile: u8, threshold_ms: u64 },
    Custom { expression: String },
}

/// Notification
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Notification {
    pub id: String,
    pub title: String,
    pub message: String,
    pub priority: Priority,
    pub source: String,
    pub timestamp: i64,
    pub metadata: HashMap<String, String>,
}

/// Channel config
#[derive(Clone, Debug)]
pub struct Channel {
    pub id: String,
    pub channel_type: ChannelType,
    pub enabled: bool,
}

/// Notification manager
pub struct NotificationManager {
    channels: RwLock<HashMap<String, Channel>>,
    rules: RwLock<HashMap<String, AlertRule>>,
    queue: mpsc::Sender<Notification>,
    history: RwLock<Vec<Notification>>,
    max_history: usize,
}

impl NotificationManager {
    pub fn new() -> (Self, mpsc::Receiver<Notification>) {
        let (tx, rx) = mpsc::channel(1000);
        let manager = Self {
            channels: RwLock::new(HashMap::new()),
            rules: RwLock::new(HashMap::new()),
            queue: tx,
            history: RwLock::new(Vec::new()),
            max_history: 1000,
        };
        (manager, rx)
    }

    /// Register a notification channel
    pub async fn register_channel(&self, id: &str, channel_type: ChannelType) {
        let channel = Channel {
            id: id.to_string(),
            channel_type,
            enabled: true,
        };
        let mut channels = self.channels.write().await;
        channels.insert(id.to_string(), channel);
    }

    /// Create an alert rule
    pub async fn create_rule(&self, rule: AlertRule) {
        let mut rules = self.rules.write().await;
        rules.insert(rule.id.clone(), rule);
    }

    /// Send a notification
    pub async fn send(&self, notification: Notification) -> Result<(), String> {
        // Add to history
        let mut history = self.history.write().await;
        history.push(notification.clone());
        if history.len() > self.max_history {
            history.remove(0);
        }
        drop(history);

        // Queue for delivery
        self.queue.send(notification).await
            .map_err(|e| format!("Failed to queue notification: {}", e))
    }

    /// Send to specific channels
    pub async fn send_to_channels(&self, notification: Notification, channel_ids: &[String]) -> Result<(), String> {
        let channels = self.channels.read().await;
        
        for channel_id in channel_ids {
            if let Some(channel) = channels.get(channel_id) {
                if channel.enabled {
                    self.deliver(&notification, &channel.channel_type).await?;
                }
            }
        }

        Ok(())
    }

    /// Deliver notification to channel
    async fn deliver(&self, notification: &Notification, channel_type: &ChannelType) -> Result<(), String> {
        match channel_type {
            ChannelType::Email { address } => {
                // In production: use lettre or similar
                tracing::info!("Would send email to {}: {}", address, notification.title);
                Ok(())
            }
            ChannelType::Webhook { url, headers } => {
                // In production: use reqwest
                tracing::info!("Would POST to webhook {}: {}", url, notification.title);
                Ok(())
            }
            ChannelType::Slack { webhook_url, channel } => {
                tracing::info!("Would post to Slack #{}: {}", channel, notification.title);
                Ok(())
            }
            ChannelType::PagerDuty { service_key } => {
                tracing::info!("Would trigger PagerDuty: {}", notification.title);
                Ok(())
            }
            ChannelType::InApp { user_id } => {
                tracing::info!("Would show in-app notification to {}: {}", user_id, notification.title);
                Ok(())
            }
        }
    }

    /// Check alert rules against metrics
    pub async fn check_rules(&self, metrics: &HashMap<String, f64>) {
        let rules = self.rules.read().await;
        let now = chrono::Utc::now().timestamp();

        for (id, rule) in rules.iter() {
            if !rule.enabled {
                continue;
            }

            // Check cooldown
            if let Some(last) = rule.last_triggered {
                if now - last < rule.cooldown_seconds as i64 {
                    continue;
                }
            }

            let triggered = match &rule.condition {
                AlertCondition::MetricAbove { metric, threshold } => {
                    metrics.get(metric).map(|v| v > threshold).unwrap_or(false)
                }
                AlertCondition::MetricBelow { metric, threshold } => {
                    metrics.get(metric).map(|v| v < threshold).unwrap_or(false)
                }
                _ => false,
            };

            if triggered {
                let notification = Notification {
                    id: uuid::Uuid::new_v4().to_string(),
                    title: format!("Alert: {}", rule.name),
                    message: format!("Rule {} triggered", rule.name),
                    priority: rule.priority,
                    source: "alert_manager".to_string(),
                    timestamp: now,
                    metadata: HashMap::new(),
                };

                let _ = self.send_to_channels(notification, &rule.channels).await;
            }
        }
    }

    /// Get notification history
    pub async fn get_history(&self, limit: usize) -> Vec<Notification> {
        let history = self.history.read().await;
        history.iter().rev().take(limit).cloned().collect()
    }

    /// List alert rules
    pub async fn list_rules(&self) -> Vec<AlertRule> {
        let rules = self.rules.read().await;
        rules.values().cloned().collect()
    }
}
