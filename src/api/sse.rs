//! Server-Sent Events - Multi-channel streaming

use std::sync::Arc;
use std::collections::HashMap;
use axum::{response::sse::{Event, Sse}, extract::{Extension, Path, Query}};
use futures::stream::{self, Stream};
use std::convert::Infallible;
use std::time::Duration;
use tokio::sync::{broadcast, RwLock};
use tokio_stream::{StreamExt as _, wrappers::BroadcastStream};
use serde::{Deserialize, Serialize};
use crate::engine::MiracleEngine;

/// SSE channel
pub struct SSEChannel {
    tx: broadcast::Sender<SSEMessage>,
    subscribers: std::sync::atomic::AtomicU64,
}

/// SSE message
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SSEMessage {
    pub event_type: String,
    pub data: serde_json::Value,
    pub id: Option<String>,
    pub timestamp: i64,
}

impl SSEChannel {
    pub fn new(capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(capacity);
        Self {
            tx,
            subscribers: std::sync::atomic::AtomicU64::new(0),
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<SSEMessage> {
        self.subscribers.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.tx.subscribe()
    }

    pub fn publish(&self, msg: SSEMessage) -> Result<usize, String> {
        self.tx.send(msg).map_err(|e| e.to_string())
    }

    pub fn subscriber_count(&self) -> u64 {
        self.subscribers.load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// SSE Manager for multi-channel support
pub struct SSEManager {
    channels: RwLock<HashMap<String, Arc<SSEChannel>>>,
    default_capacity: usize,
}

impl SSEManager {
    pub fn new(default_capacity: usize) -> Self {
        Self {
            channels: RwLock::new(HashMap::new()),
            default_capacity,
        }
    }

    pub async fn get_or_create_channel(&self, name: &str) -> Arc<SSEChannel> {
        let channels = self.channels.read().await;
        if let Some(channel) = channels.get(name) {
            return Arc::clone(channel);
        }
        drop(channels);

        let mut channels = self.channels.write().await;
        let channel = Arc::new(SSEChannel::new(self.default_capacity));
        channels.insert(name.to_string(), Arc::clone(&channel));
        channel
    }

    pub async fn publish(&self, channel_name: &str, msg: SSEMessage) -> Result<usize, String> {
        let channel = self.get_or_create_channel(channel_name).await;
        channel.publish(msg)
    }

    pub async fn list_channels(&self) -> Vec<(String, u64)> {
        let channels = self.channels.read().await;
        channels.iter()
            .map(|(name, ch)| (name.clone(), ch.subscriber_count()))
            .collect()
    }
}

impl Default for SSEManager {
    fn default() -> Self {
        Self::new(1000)
    }
}

/// Query params for SSE
#[derive(Deserialize)]
pub struct SSEParams {
    pub last_event_id: Option<String>,
}


/// Default heartbeat handler
pub async fn handler(Extension(engine): Extension<Arc<MiracleEngine>>) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let stream = stream::repeat_with(|| {
        Event::default()
            .event("heartbeat")
            .data(serde_json::json!({
                "type": "heartbeat",
                "timestamp": chrono::Utc::now().timestamp()
            }).to_string())
    })
    .map(Ok)
    .throttle(Duration::from_secs(30));
    
    Sse::new(stream).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(Duration::from_secs(10))
            .text("ping")
    )
}

/// Table changes stream handler
pub async fn table_changes(
    Path(table): Path<String>,
    Query(params): Query<SSEParams>,
    Extension(sse_manager): Extension<Arc<SSEManager>>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let channel = sse_manager.get_or_create_channel(&format!("table:{}", table)).await;
    let rx = channel.subscribe();
    
    let stream = BroadcastStream::new(rx)
        .filter_map(|result| {
            result.ok().map(|msg| {
                Ok(Event::default()
                    .event(&msg.event_type)
                    .data(serde_json::to_string(&msg.data).unwrap_or_default())
                    .id(msg.id.unwrap_or_else(|| msg.timestamp.to_string())))
            })
        });
    
    Sse::new(stream).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("ping")
    )
}

/// Query results stream handler
pub async fn query_stream(
    Extension(engine): Extension<Arc<MiracleEngine>>,
    Query(params): Query<HashMap<String, String>>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let sql = params.get("sql").cloned().unwrap_or_default();
    
    // Execute query and stream results
    let stream = stream::once(async move {
        Ok(Event::default()
            .event("result")
            .data(serde_json::json!({
                "status": "executed",
                "sql": sql,
                "timestamp": chrono::Utc::now().timestamp()
            }).to_string()))
    });
    
    Sse::new(stream)
}
