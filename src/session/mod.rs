//! Session Module - Connection and session management

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

/// Session information
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Session {
    pub id: String,
    pub user_id: String,
    pub tenant_id: Option<String>,
    pub created_at: i64,
    pub last_activity: i64,
    pub metadata: HashMap<String, String>,
    #[serde(skip)]
    pub last_activity_instant: Option<Instant>,
}

/// Connection info
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConnectionInfo {
    pub session_id: String,
    pub client_ip: String,
    pub user_agent: Option<String>,
    pub connected_at: i64,
    pub queries_executed: u64,
    pub bytes_sent: u64,
    pub bytes_received: u64,
}

/// Session limits
#[derive(Clone, Debug)]
pub struct SessionLimits {
    pub max_sessions_per_user: usize,
    pub session_timeout_seconds: u64,
    pub max_idle_seconds: u64,
}

impl Default for SessionLimits {
    fn default() -> Self {
        Self {
            max_sessions_per_user: 10,
            session_timeout_seconds: 3600, // 1 hour
            max_idle_seconds: 900, // 15 minutes
        }
    }
}

/// Session manager
pub struct SessionManager {
    sessions: Arc<RwLock<HashMap<String, Session>>>,
    connections: Arc<RwLock<HashMap<String, ConnectionInfo>>>,
    user_sessions: Arc<RwLock<HashMap<String, Vec<String>>>>,
    limits: SessionLimits,
}

impl SessionManager {
    pub fn new(limits: SessionLimits) -> Self {
        let manager = Self {
            sessions: Arc::new(RwLock::new(HashMap::new())),
            connections: Arc::new(RwLock::new(HashMap::new())),
            user_sessions: Arc::new(RwLock::new(HashMap::new())),
            limits,
        };

        // Start cleanup task
        let sessions = Arc::clone(&manager.sessions);
        let connections = Arc::clone(&manager.connections);
        let user_sessions = Arc::clone(&manager.user_sessions);
        let max_idle = manager.limits.max_idle_seconds;

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                
                let now = Instant::now();
                let mut sessions_guard = sessions.write().await;
                let mut connections_guard = connections.write().await;
                let mut user_sessions_guard = user_sessions.write().await;

                // Find expired sessions
                let expired: Vec<String> = sessions_guard.iter()
                    .filter(|(_, s)| {
                        s.last_activity_instant
                            .map(|i| now.duration_since(i).as_secs() > max_idle)
                            .unwrap_or(false)
                    })
                    .map(|(id, _)| id.clone())
                    .collect();

                // Remove expired
                for session_id in expired {
                    if let Some(session) = sessions_guard.remove(&session_id) {
                        connections_guard.remove(&session_id);
                        if let Some(user_sessions) = user_sessions_guard.get_mut(&session.user_id) {
                            user_sessions.retain(|id| id != &session_id);
                        }
                    }
                }
            }
        });

        manager
    }

    /// Create a new session
    pub async fn create(&self, user_id: &str, tenant_id: Option<&str>, client_ip: &str) -> Result<Session, String> {
        // Check session limit
        let user_sessions = self.user_sessions.read().await;
        if let Some(sessions) = user_sessions.get(user_id) {
            if sessions.len() >= self.limits.max_sessions_per_user {
                return Err("Maximum sessions exceeded".to_string());
            }
        }
        drop(user_sessions);

        let session_id = uuid::Uuid::new_v4().to_string();
        let now = chrono::Utc::now().timestamp();

        let session = Session {
            id: session_id.clone(),
            user_id: user_id.to_string(),
            tenant_id: tenant_id.map(String::from),
            created_at: now,
            last_activity: now,
            metadata: HashMap::new(),
            last_activity_instant: Some(Instant::now()),
        };

        let connection = ConnectionInfo {
            session_id: session_id.clone(),
            client_ip: client_ip.to_string(),
            user_agent: None,
            connected_at: now,
            queries_executed: 0,
            bytes_sent: 0,
            bytes_received: 0,
        };

        let mut sessions = self.sessions.write().await;
        let mut connections = self.connections.write().await;
        let mut user_sessions = self.user_sessions.write().await;

        sessions.insert(session_id.clone(), session.clone());
        connections.insert(session_id.clone(), connection);
        user_sessions.entry(user_id.to_string())
            .or_insert_with(Vec::new)
            .push(session_id);

        Ok(session)
    }

    /// Get session by ID
    pub async fn get(&self, session_id: &str) -> Option<Session> {
        let sessions = self.sessions.read().await;
        sessions.get(session_id).cloned()
    }

    /// Update last activity
    pub async fn touch(&self, session_id: &str) {
        let mut sessions = self.sessions.write().await;
        if let Some(session) = sessions.get_mut(session_id) {
            session.last_activity = chrono::Utc::now().timestamp();
            session.last_activity_instant = Some(Instant::now());
        }
    }

    /// Record query execution
    pub async fn record_query(&self, session_id: &str, bytes_in: u64, bytes_out: u64) {
        self.touch(session_id).await;

        let mut connections = self.connections.write().await;
        if let Some(conn) = connections.get_mut(session_id) {
            conn.queries_executed += 1;
            conn.bytes_received += bytes_in;
            conn.bytes_sent += bytes_out;
        }
    }

    /// Destroy session
    pub async fn destroy(&self, session_id: &str) -> Result<(), String> {
        let mut sessions = self.sessions.write().await;
        let session = sessions.remove(session_id)
            .ok_or("Session not found")?;

        let mut connections = self.connections.write().await;
        connections.remove(session_id);

        let mut user_sessions = self.user_sessions.write().await;
        if let Some(sessions) = user_sessions.get_mut(&session.user_id) {
            sessions.retain(|id| id != session_id);
        }

        Ok(())
    }

    /// List all sessions
    pub async fn list(&self) -> Vec<Session> {
        let sessions = self.sessions.read().await;
        sessions.values().cloned().collect()
    }

    /// Get sessions for a user
    pub async fn get_user_sessions(&self, user_id: &str) -> Vec<Session> {
        let user_sessions = self.user_sessions.read().await;
        let session_ids = user_sessions.get(user_id).cloned().unwrap_or_default();
        drop(user_sessions);

        let sessions = self.sessions.read().await;
        session_ids.iter()
            .filter_map(|id| sessions.get(id).cloned())
            .collect()
    }

    /// Get connection info
    pub async fn get_connection(&self, session_id: &str) -> Option<ConnectionInfo> {
        let connections = self.connections.read().await;
        connections.get(session_id).cloned()
    }

    /// Count active sessions
    pub async fn count(&self) -> usize {
        let sessions = self.sessions.read().await;
        sessions.len()
    }
}

impl Default for SessionManager {
    fn default() -> Self {
        Self::new(SessionLimits::default())
    }
}
