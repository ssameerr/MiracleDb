//! Hook Module - Lifecycle and event hooks

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

/// Hook point
#[derive(Clone, Debug, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub enum HookPoint {
    // Query lifecycle
    BeforeQuery,
    AfterQuery,
    OnQueryError,
    
    // Transaction lifecycle
    BeforeTransaction,
    AfterCommit,
    AfterRollback,
    
    // Connection lifecycle
    OnConnect,
    OnDisconnect,
    
    // Data lifecycle
    BeforeInsert,
    AfterInsert,
    BeforeUpdate,
    AfterUpdate,
    BeforeDelete,
    AfterDelete,
    
    // Schema lifecycle
    BeforeCreateTable,
    AfterCreateTable,
    BeforeDropTable,
    AfterDropTable,
    
    // Server lifecycle
    OnStartup,
    OnShutdown,
    
    // Custom
    Custom(String),
}

/// Hook action
#[derive(Clone, Debug)]
pub enum HookAction {
    Continue,
    Modify(serde_json::Value),
    Abort(String),
    Retry,
}

/// Hook context
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HookContext {
    pub hook_point: HookPoint,
    pub user_id: Option<String>,
    pub database: Option<String>,
    pub table: Option<String>,
    pub query: Option<String>,
    pub data: Option<serde_json::Value>,
    pub metadata: HashMap<String, String>,
    pub timestamp: i64,
}

impl HookContext {
    pub fn new(hook_point: HookPoint) -> Self {
        Self {
            hook_point,
            user_id: None,
            database: None,
            table: None,
            query: None,
            data: None,
            metadata: HashMap::new(),
            timestamp: chrono::Utc::now().timestamp(),
        }
    }

    pub fn with_user(mut self, user_id: &str) -> Self {
        self.user_id = Some(user_id.to_string());
        self
    }

    pub fn with_database(mut self, database: &str) -> Self {
        self.database = Some(database.to_string());
        self
    }

    pub fn with_table(mut self, table: &str) -> Self {
        self.table = Some(table.to_string());
        self
    }

    pub fn with_query(mut self, query: &str) -> Self {
        self.query = Some(query.to_string());
        self
    }

    pub fn with_data(mut self, data: serde_json::Value) -> Self {
        self.data = Some(data);
        self
    }

    pub fn with_metadata(mut self, key: &str, value: &str) -> Self {
        self.metadata.insert(key.to_string(), value.to_string());
        self
    }
}

/// Hook handler trait
pub trait HookHandler: Send + Sync {
    fn handle(&self, context: &HookContext) -> HookAction;
    fn priority(&self) -> i32 { 0 }
    fn name(&self) -> &str;
}

/// Simple function hook
pub struct FnHook {
    name: String,
    priority: i32,
    handler: Box<dyn Fn(&HookContext) -> HookAction + Send + Sync>,
}

impl FnHook {
    pub fn new<F>(name: &str, handler: F) -> Self
    where
        F: Fn(&HookContext) -> HookAction + Send + Sync + 'static,
    {
        Self {
            name: name.to_string(),
            priority: 0,
            handler: Box::new(handler),
        }
    }

    pub fn with_priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }
}

impl HookHandler for FnHook {
    fn handle(&self, context: &HookContext) -> HookAction {
        (self.handler)(context)
    }

    fn priority(&self) -> i32 {
        self.priority
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// Hook registry
pub struct HookRegistry {
    hooks: RwLock<HashMap<HookPoint, Vec<Arc<dyn HookHandler>>>>,
}

impl HookRegistry {
    pub fn new() -> Self {
        Self {
            hooks: RwLock::new(HashMap::new()),
        }
    }

    /// Register a hook handler
    pub async fn register(&self, point: HookPoint, handler: Arc<dyn HookHandler>) {
        let mut hooks = self.hooks.write().await;
        let handlers = hooks.entry(point).or_insert_with(Vec::new);
        handlers.push(handler);
        handlers.sort_by(|a, b| b.priority().cmp(&a.priority()));
    }

    /// Unregister a hook by name
    pub async fn unregister(&self, point: &HookPoint, name: &str) {
        let mut hooks = self.hooks.write().await;
        if let Some(handlers) = hooks.get_mut(point) {
            handlers.retain(|h| h.name() != name);
        }
    }

    /// Execute hooks for a point
    pub async fn execute(&self, context: &HookContext) -> HookAction {
        let hooks = self.hooks.read().await;
        
        if let Some(handlers) = hooks.get(&context.hook_point) {
            for handler in handlers {
                match handler.handle(context) {
                    HookAction::Continue => continue,
                    action => return action,
                }
            }
        }

        HookAction::Continue
    }

    /// Execute hooks and collect all actions
    pub async fn execute_all(&self, context: &HookContext) -> Vec<(String, HookAction)> {
        let hooks = self.hooks.read().await;
        let mut results = Vec::new();
        
        if let Some(handlers) = hooks.get(&context.hook_point) {
            for handler in handlers {
                let action = handler.handle(context);
                results.push((handler.name().to_string(), action));
            }
        }

        results
    }

    /// List registered hooks
    pub async fn list(&self, point: &HookPoint) -> Vec<String> {
        let hooks = self.hooks.read().await;
        hooks.get(point)
            .map(|handlers| handlers.iter().map(|h| h.name().to_string()).collect())
            .unwrap_or_default()
    }

    /// Check if any hooks are registered for a point
    pub async fn has_hooks(&self, point: &HookPoint) -> bool {
        let hooks = self.hooks.read().await;
        hooks.get(point).map(|h| !h.is_empty()).unwrap_or(false)
    }
}

impl Default for HookRegistry {
    fn default() -> Self {
        Self::new()
    }
}
