//! Event Bus Module - Asynchronous event-driven architecture

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use serde::{Serialize, Deserialize};

/// Event payload
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Event {
    pub id: String,
    pub event_type: String,
    pub source: String,
    pub payload: serde_json::Value,
    pub metadata: HashMap<String, String>,
    pub timestamp: i64,
}

/// Event filter
#[derive(Clone, Debug)]
pub struct EventFilter {
    pub event_types: Option<Vec<String>>,
    pub sources: Option<Vec<String>>,
}

impl EventFilter {
    pub fn matches(&self, event: &Event) -> bool {
        if let Some(types) = &self.event_types {
            if !types.contains(&event.event_type) {
                return false;
            }
        }
        if let Some(sources) = &self.sources {
            if !sources.contains(&event.source) {
                return false;
            }
        }
        true
    }
}

/// Event handler trait
pub trait EventHandler: Send + Sync {
    fn handle(&self, event: &Event);
}

/// Event bus for pub/sub messaging
pub struct EventBus {
    sender: broadcast::Sender<Event>,
    handlers: RwLock<HashMap<String, Vec<Arc<dyn EventHandler>>>>,
    filters: RwLock<HashMap<String, EventFilter>>,
    event_count: std::sync::atomic::AtomicU64,
}

impl EventBus {
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self {
            sender,
            handlers: RwLock::new(HashMap::new()),
            filters: RwLock::new(HashMap::new()),
            event_count: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Publish an event
    pub async fn publish(&self, event: Event) -> Result<usize, String> {
        self.event_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Notify registered handlers
        let handlers = self.handlers.read().await;
        let filters = self.filters.read().await;

        for (handler_id, handler_list) in handlers.iter() {
            let filter = filters.get(handler_id);
            let should_handle = filter.map(|f| f.matches(&event)).unwrap_or(true);

            if should_handle {
                for handler in handler_list {
                    handler.handle(&event);
                }
            }
        }

        // Broadcast to subscribers
        self.sender.send(event)
            .map_err(|e| format!("Failed to publish event: {}", e))
    }

    /// Subscribe to events
    pub fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.sender.subscribe()
    }

    /// Subscribe with filter
    pub async fn subscribe_filtered(&self, filter: EventFilter) -> (String, broadcast::Receiver<Event>) {
        let id = uuid::Uuid::new_v4().to_string();
        let rx = self.sender.subscribe();

        let mut filters = self.filters.write().await;
        filters.insert(id.clone(), filter);

        (id, rx)
    }

    /// Register a handler
    pub async fn register_handler(&self, id: &str, handler: Arc<dyn EventHandler>, filter: Option<EventFilter>) {
        let mut handlers = self.handlers.write().await;
        handlers.entry(id.to_string())
            .or_insert_with(Vec::new)
            .push(handler);

        if let Some(f) = filter {
            let mut filters = self.filters.write().await;
            filters.insert(id.to_string(), f);
        }
    }

    /// Unregister a handler
    pub async fn unregister_handler(&self, id: &str) {
        let mut handlers = self.handlers.write().await;
        handlers.remove(id);

        let mut filters = self.filters.write().await;
        filters.remove(id);
    }

    /// Get event count
    pub fn event_count(&self) -> u64 {
        self.event_count.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Create an event builder
    pub fn event_builder() -> EventBuilder {
        EventBuilder::default()
    }
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new(10000)
    }
}

/// Event builder
#[derive(Default)]
pub struct EventBuilder {
    event_type: String,
    source: String,
    payload: serde_json::Value,
    metadata: HashMap<String, String>,
}

impl EventBuilder {
    pub fn event_type(mut self, t: &str) -> Self {
        self.event_type = t.to_string();
        self
    }

    pub fn source(mut self, s: &str) -> Self {
        self.source = s.to_string();
        self
    }

    pub fn payload(mut self, p: serde_json::Value) -> Self {
        self.payload = p;
        self
    }

    pub fn metadata(mut self, key: &str, value: &str) -> Self {
        self.metadata.insert(key.to_string(), value.to_string());
        self
    }

    pub fn build(self) -> Event {
        Event {
            id: uuid::Uuid::new_v4().to_string(),
            event_type: self.event_type,
            source: self.source,
            payload: self.payload,
            metadata: self.metadata,
            timestamp: chrono::Utc::now().timestamp_millis(),
        }
    }
}

/// Standard database events
pub mod events {
    pub const TABLE_CREATED: &str = "table.created";
    pub const TABLE_DROPPED: &str = "table.dropped";
    pub const ROW_INSERTED: &str = "row.inserted";
    pub const ROW_UPDATED: &str = "row.updated";
    pub const ROW_DELETED: &str = "row.deleted";
    pub const QUERY_EXECUTED: &str = "query.executed";
    pub const USER_CONNECTED: &str = "user.connected";
    pub const USER_DISCONNECTED: &str = "user.disconnected";
    pub const REPLICATION_LAG: &str = "replication.lag";
    pub const NODE_JOINED: &str = "cluster.node_joined";
    pub const NODE_LEFT: &str = "cluster.node_left";
}
