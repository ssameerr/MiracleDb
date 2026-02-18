use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tokio::sync::broadcast;
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PubSubMessage {
    pub channel: String,
    pub payload: serde_json::Value,
    pub sender: Option<String>,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

pub struct PubSubBroker {
    channels: Arc<RwLock<HashMap<String, broadcast::Sender<PubSubMessage>>>>,
}

impl PubSubBroker {
    pub fn new() -> Self {
        Self { channels: Arc::new(RwLock::new(HashMap::new())) }
    }

    pub fn subscribe(&self, channel: &str) -> broadcast::Receiver<PubSubMessage> {
        let mut channels = self.channels.write().unwrap();
        channels.entry(channel.to_string())
            .or_insert_with(|| broadcast::channel(1024).0)
            .subscribe()
    }

    pub fn publish(&self, channel: &str, payload: serde_json::Value, sender: Option<String>) -> Result<usize, String> {
        let channels = self.channels.read().unwrap();
        if let Some(tx) = channels.get(channel) {
            let msg = PubSubMessage {
                channel: channel.to_string(),
                payload,
                sender,
                timestamp: chrono::Utc::now(),
            };
            tx.send(msg).map_err(|e| format!("Publish error: {}", e))
        } else {
            Ok(0) // No subscribers
        }
    }

    pub fn channel_count(&self) -> usize {
        self.channels.read().unwrap().len()
    }
}
