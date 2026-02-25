//! Notification API - REST endpoints for system and user notifications

use std::sync::Arc;
use axum::{
    Router,
    routing::{get, post},
    extract::{Extension, Path, Query},
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Serialize};
use crate::notification::{NotificationManager, Notification, Priority, AlertRule};

/// Notification API state
#[derive(Clone)]
pub struct NotificationApiState {
    pub manager: Arc<NotificationManager>,
}

impl NotificationApiState {
    pub fn new(manager: Arc<NotificationManager>) -> Self {
        Self { manager }
    }
}

/// Create notification routes
pub fn routes(state: NotificationApiState) -> Router {
    Router::new()
        // List all notifications
        .route("/", get(list_notifications).post(create_notification))
        // System notifications
        .route("/system", get(get_system_notifications))
        // User-specific notifications
        .route("/user/:user_id", get(get_user_notifications))
        // Single notification
        .route("/:id", get(get_notification))
        // Mark as read
        .route("/:id/read", post(mark_as_read))
        // Alert rules
        .route("/rules", get(list_rules))
        .layer(Extension(state))
}

// ==================== Request/Response Models ====================

#[derive(Debug, Deserialize)]
pub struct NotificationFilter {
    /// Filter by priority
    pub priority: Option<String>,
    /// Filter by source
    pub source: Option<String>,
    /// Maximum number of notifications to return
    pub limit: Option<usize>,
    /// Offset for pagination
    pub offset: Option<usize>,
}

#[derive(Debug, Serialize)]
pub struct NotificationResponse {
    pub id: String,
    pub title: String,
    pub message: String,
    pub priority: String,
    pub source: String,
    pub timestamp: i64,
    pub metadata: std::collections::HashMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub read: Option<bool>,
}

impl From<&Notification> for NotificationResponse {
    fn from(n: &Notification) -> Self {
        Self {
            id: n.id.clone(),
            title: n.title.clone(),
            message: n.message.clone(),
            priority: format!("{:?}", n.priority),
            source: n.source.clone(),
            timestamp: n.timestamp,
            metadata: n.metadata.clone(),
            read: None,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct NotificationListResponse {
    pub notifications: Vec<NotificationResponse>,
    pub total: usize,
    pub limit: usize,
    pub offset: usize,
}

#[derive(Debug, Deserialize)]
pub struct CreateNotificationRequest {
    pub title: String,
    pub message: String,
    #[serde(default = "default_priority")]
    pub priority: String,
    #[serde(default = "default_source")]
    pub source: String,
    #[serde(default)]
    pub user_id: Option<String>,
    #[serde(default)]
    pub metadata: std::collections::HashMap<String, String>,
}

fn default_priority() -> String {
    "Normal".to_string()
}

fn default_source() -> String {
    "system".to_string()
}

#[derive(Debug, Serialize)]
pub struct AlertRuleResponse {
    pub id: String,
    pub name: String,
    pub condition: String,
    pub channels: Vec<String>,
    pub priority: String,
    pub enabled: bool,
    pub cooldown_seconds: u64,
    pub last_triggered: Option<i64>,
}

impl From<&AlertRule> for AlertRuleResponse {
    fn from(r: &AlertRule) -> Self {
        Self {
            id: r.id.clone(),
            name: r.name.clone(),
            condition: format!("{:?}", r.condition),
            channels: r.channels.clone(),
            priority: format!("{:?}", r.priority),
            enabled: r.enabled,
            cooldown_seconds: r.cooldown_seconds,
            last_triggered: r.last_triggered,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ApiResponse<T> {
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl<T> ApiResponse<T> {
    pub fn success(data: T) -> Self {
        Self {
            status: "success".to_string(),
            data: Some(data),
            error: None,
        }
    }

    pub fn error(msg: &str) -> Self {
        Self {
            status: "error".to_string(),
            data: None,
            error: Some(msg.to_string()),
        }
    }
}

// ==================== Handlers ====================

/// List all notifications with optional filters
async fn list_notifications(
    Extension(state): Extension<NotificationApiState>,
    Query(filter): Query<NotificationFilter>,
) -> Json<ApiResponse<NotificationListResponse>> {
    let limit = filter.limit.unwrap_or(50);
    let offset = filter.offset.unwrap_or(0);
    
    // Get history from manager
    let history = state.manager.get_history(1000).await;
    
    // Apply filters
    let filtered: Vec<&Notification> = history.iter()
        .filter(|n| {
            // Priority filter
            if let Some(ref p) = filter.priority {
                let priority_str = format!("{:?}", n.priority).to_lowercase();
                if priority_str != p.to_lowercase() {
                    return false;
                }
            }
            // Source filter
            if let Some(ref s) = filter.source {
                if !n.source.to_lowercase().contains(&s.to_lowercase()) {
                    return false;
                }
            }
            true
        })
        .collect();
    
    let total = filtered.len();
    
    // Apply pagination
    let paginated: Vec<NotificationResponse> = filtered.into_iter()
        .skip(offset)
        .take(limit)
        .map(|n| n.into())
        .collect();
    
    Json(ApiResponse::success(NotificationListResponse {
        notifications: paginated,
        total,
        limit,
        offset,
    }))
}

/// Get a single notification by ID
async fn get_notification(
    Extension(state): Extension<NotificationApiState>,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<NotificationResponse>>, (StatusCode, Json<ApiResponse<()>>)> {
    let history = state.manager.get_history(1000).await;
    
    match history.iter().find(|n| n.id == id) {
        Some(notification) => Ok(Json(ApiResponse::success(notification.into()))),
        None => Err((
            StatusCode::NOT_FOUND,
            Json(ApiResponse::error("Notification not found")),
        )),
    }
}

/// Get system-wide notifications
async fn get_system_notifications(
    Extension(state): Extension<NotificationApiState>,
    Query(filter): Query<NotificationFilter>,
) -> Json<ApiResponse<NotificationListResponse>> {
    let limit = filter.limit.unwrap_or(50);
    let offset = filter.offset.unwrap_or(0);
    
    let history = state.manager.get_history(1000).await;
    
    // Filter for system notifications
    let system_notifs: Vec<NotificationResponse> = history.iter()
        .filter(|n| n.source == "system" || n.source == "alert_manager")
        .skip(offset)
        .take(limit)
        .map(|n| n.into())
        .collect();
    
    let total = system_notifs.len();
    
    Json(ApiResponse::success(NotificationListResponse {
        notifications: system_notifs,
        total,
        limit,
        offset,
    }))
}

/// Get notifications for a specific user
async fn get_user_notifications(
    Extension(state): Extension<NotificationApiState>,
    Path(user_id): Path<String>,
    Query(filter): Query<NotificationFilter>,
) -> Json<ApiResponse<NotificationListResponse>> {
    let limit = filter.limit.unwrap_or(50);
    let offset = filter.offset.unwrap_or(0);
    
    let history = state.manager.get_history(1000).await;
    
    // Filter for user-specific notifications (check metadata for user_id)
    let user_notifs: Vec<NotificationResponse> = history.iter()
        .filter(|n| {
            n.metadata.get("user_id").map(|u| u == &user_id).unwrap_or(false)
        })
        .skip(offset)
        .take(limit)
        .map(|n| n.into())
        .collect();
    
    let total = user_notifs.len();
    
    Json(ApiResponse::success(NotificationListResponse {
        notifications: user_notifs,
        total,
        limit,
        offset,
    }))
}

/// Create a new notification
async fn create_notification(
    Extension(state): Extension<NotificationApiState>,
    Json(req): Json<CreateNotificationRequest>,
) -> Result<Json<ApiResponse<NotificationResponse>>, (StatusCode, Json<ApiResponse<()>>)> {
    let priority = match req.priority.to_lowercase().as_str() {
        "low" => Priority::Low,
        "normal" => Priority::Normal,
        "high" => Priority::High,
        "critical" => Priority::Critical,
        _ => Priority::Normal,
    };
    
    let mut metadata = req.metadata;
    if let Some(user_id) = req.user_id {
        metadata.insert("user_id".to_string(), user_id);
    }
    
    let notification = Notification {
        id: uuid::Uuid::new_v4().to_string(),
        title: req.title,
        message: req.message,
        priority,
        source: req.source,
        timestamp: chrono::Utc::now().timestamp(),
        metadata,
    };
    
    let response: NotificationResponse = (&notification).into();
    
    match state.manager.send(notification).await {
        Ok(_) => Ok(Json(ApiResponse::success(response))),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse::error(&e)),
        )),
    }
}

/// Mark a notification as read (placeholder - would need persistence layer)
async fn mark_as_read(
    Path(id): Path<String>,
) -> Json<ApiResponse<serde_json::Value>> {
    // In a full implementation, this would update a read status in a persistence layer
    Json(ApiResponse::success(serde_json::json!({
        "notification_id": id,
        "read": true,
        "read_at": chrono::Utc::now().timestamp()
    })))
}

/// List all alert rules
async fn list_rules(
    Extension(state): Extension<NotificationApiState>,
) -> Json<ApiResponse<Vec<AlertRuleResponse>>> {
    let rules = state.manager.list_rules().await;
    let responses: Vec<AlertRuleResponse> = rules.iter().map(|r| r.into()).collect();
    Json(ApiResponse::success(responses))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_notification_response_from() {
        let notification = Notification {
            id: "test-123".to_string(),
            title: "Test Title".to_string(),
            message: "Test Message".to_string(),
            priority: Priority::High,
            source: "system".to_string(),
            timestamp: 1234567890,
            metadata: std::collections::HashMap::new(),
        };
        
        let response: NotificationResponse = (&notification).into();
        
        assert_eq!(response.id, "test-123");
        assert_eq!(response.title, "Test Title");
        assert_eq!(response.priority, "High");
    }
}
