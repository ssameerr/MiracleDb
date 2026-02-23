//! WebSocket API - Change Data Capture streams

use std::sync::Arc;
use axum::{extract::{ws::{WebSocket, WebSocketUpgrade, Message}, Extension}, response::IntoResponse};
use tokio::sync::broadcast;
use serde::{Deserialize, Serialize};
use crate::engine::MiracleEngine;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CdcEvent {
    pub table: String,
    pub operation: String, // INSERT, UPDATE, DELETE
    pub timestamp: i64,
    pub data: serde_json::Value,
}


pub async fn handler(ws: WebSocketUpgrade, Extension(engine): Extension<Arc<MiracleEngine>>) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_socket(socket, engine))
}

async fn handle_socket(mut socket: WebSocket, engine: Arc<MiracleEngine>) {
    // Send welcome message
    let _ = socket.send(Message::Text(serde_json::json!({
        "type": "connected",
        "message": "Connected to MiracleDb CDC stream"
    }).to_string())).await;
    
    // Handle incoming messages
    while let Some(msg) = socket.recv().await {
        match msg {
            Ok(Message::Text(text)) => {
                if let Ok(cmd) = serde_json::from_str::<SubscribeCommand>(&text) {
                    let _ = socket.send(Message::Text(serde_json::json!({
                        "type": "subscribed",
                        "table": cmd.table
                    }).to_string())).await;
                }
            }
            Ok(Message::Close(_)) => break,
            _ => {}
        }
    }
}

#[derive(Deserialize)]
struct SubscribeCommand {
    table: String,
    operations: Option<Vec<String>>,
}

/// A message sent over the WebSocket connection.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WsMessage {
    /// Server → client: stream is connected.
    Connected { server_version: String },
    /// Server → client: a CDC event.
    Event(CdcEvent),
    /// Server → client: subscription confirmed.
    Subscribed { table: String },
    /// Client → server: ping.
    Ping,
    /// Server → client: pong.
    Pong,
}

/// A client subscription to CDC events for a specific table.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WsSubscription {
    pub table: String,
    /// Filter to specific operations; empty = all operations.
    pub operations: Vec<String>,
}

impl WsSubscription {
    pub fn new(table: &str) -> Self {
        Self {
            table: table.to_string(),
            operations: Vec::new(),
        }
    }

    pub fn with_operations(mut self, ops: Vec<&str>) -> Self {
        self.operations = ops.into_iter().map(String::from).collect();
        self
    }

    pub fn matches_operation(&self, op: &str) -> bool {
        self.operations.is_empty() || self.operations.iter().any(|o| o == op)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ws_subscription_new() {
        let sub = WsSubscription::new("orders");
        assert_eq!(sub.table, "orders");
        assert!(sub.operations.is_empty());
    }

    #[test]
    fn test_ws_subscription_matches_all_when_empty() {
        let sub = WsSubscription::new("users");
        assert!(sub.matches_operation("INSERT"));
        assert!(sub.matches_operation("DELETE"));
    }

    #[test]
    fn test_ws_subscription_filters_operations() {
        let sub = WsSubscription::new("users")
            .with_operations(vec!["INSERT"]);
        assert!(sub.matches_operation("INSERT"));
        assert!(!sub.matches_operation("DELETE"));
    }

    #[test]
    fn test_cdc_event_serializes() {
        let event = CdcEvent {
            table: "products".to_string(),
            operation: "INSERT".to_string(),
            timestamp: 1700000000,
            data: serde_json::json!({"id": 1, "price": 9.99}),
        };
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("products"));
        assert!(json.contains("INSERT"));
    }

    #[test]
    fn test_ws_message_connected_serializes() {
        let msg = WsMessage::Connected { server_version: "1.0.0".to_string() };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("connected"));
        assert!(json.contains("1.0.0"));
    }
}
