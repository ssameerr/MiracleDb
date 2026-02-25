//! Notification API Tests

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use tower::ServiceExt;
use miracledb::notification::NotificationManager;
use miracledb::api::notifications;
use std::sync::Arc;
use serde_json::Value;

#[tokio::test]
async fn test_list_notifications() {
    // 1. Initialize NotificationManager
    let (manager, _rx) = NotificationManager::new();
    let manager = Arc::new(manager);
    
    // 2. Build Router
    let state = notifications::NotificationApiState::new(manager.clone());
    let app = notifications::routes(state);

    // 3. Test GET /
    let response = app.clone().oneshot(
        Request::builder()
            .method("GET")
            .uri("/")
            .body(Body::empty())
            .unwrap(),
    ).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    
    let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body_json: Value = serde_json::from_slice(&body_bytes).unwrap();
    
    assert_eq!(body_json["status"], "success");
    assert!(body_json["data"]["notifications"].is_array());
}

#[tokio::test]
async fn test_create_notification() {
    // 1. Initialize NotificationManager
    let (manager, _rx) = NotificationManager::new();
    let manager = Arc::new(manager);
    
    // 2. Build Router
    let state = notifications::NotificationApiState::new(manager.clone());
    let app = notifications::routes(state);

    // 3. Create notification
    let payload = serde_json::json!({
        "title": "Test Notification",
        "message": "This is a test message",
        "priority": "high",
        "source": "system"
    });
    
    let response = app.clone().oneshot(
        Request::builder()
            .method("POST")
            .uri("/")
            .header("Content-Type", "application/json")
            .body(Body::from(serde_json::to_string(&payload).unwrap()))
            .unwrap(),
    ).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    
    let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body_json: Value = serde_json::from_slice(&body_bytes).unwrap();
    
    assert_eq!(body_json["status"], "success");
    assert_eq!(body_json["data"]["title"], "Test Notification");
    assert_eq!(body_json["data"]["priority"], "High");
}

#[tokio::test]
async fn test_get_system_notifications() {
    // 1. Initialize NotificationManager
    let (manager, _rx) = NotificationManager::new();
    let manager = Arc::new(manager);
    
    // 2. Build Router
    let state = notifications::NotificationApiState::new(manager.clone());
    let app = notifications::routes(state);

    // 3. Test GET /system
    let response = app.clone().oneshot(
        Request::builder()
            .method("GET")
            .uri("/system")
            .body(Body::empty())
            .unwrap(),
    ).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    
    let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body_json: Value = serde_json::from_slice(&body_bytes).unwrap();
    
    assert_eq!(body_json["status"], "success");
    assert!(body_json["data"]["notifications"].is_array());
}

#[tokio::test]
async fn test_get_user_notifications() {
    // 1. Initialize NotificationManager
    let (manager, _rx) = NotificationManager::new();
    let manager = Arc::new(manager);
    
    // 2. Build Router
    let state = notifications::NotificationApiState::new(manager.clone());
    let app = notifications::routes(state);

    // 3. Test GET /user/:user_id
    let response = app.clone().oneshot(
        Request::builder()
            .method("GET")
            .uri("/user/test-user-123")
            .body(Body::empty())
            .unwrap(),
    ).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    
    let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body_json: Value = serde_json::from_slice(&body_bytes).unwrap();
    
    assert_eq!(body_json["status"], "success");
    // Should return empty array initially
    assert!(body_json["data"]["notifications"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn test_list_alert_rules() {
    // 1. Initialize NotificationManager
    let (manager, _rx) = NotificationManager::new();
    let manager = Arc::new(manager);
    
    // 2. Build Router
    let state = notifications::NotificationApiState::new(manager.clone());
    let app = notifications::routes(state);

    // 3. Test GET /rules
    let response = app.clone().oneshot(
        Request::builder()
            .method("GET")
            .uri("/rules")
            .body(Body::empty())
            .unwrap(),
    ).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    
    let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body_json: Value = serde_json::from_slice(&body_bytes).unwrap();
    
    assert_eq!(body_json["status"], "success");
    assert!(body_json["data"].is_array());
}

#[tokio::test]
async fn test_create_user_notification() {
    // 1. Initialize NotificationManager
    let (manager, _rx) = NotificationManager::new();
    let manager = Arc::new(manager);
    
    // 2. Build Router
    let state = notifications::NotificationApiState::new(manager.clone());
    let app = notifications::routes(state);

    // 3. Create user-specific notification
    let payload = serde_json::json!({
        "title": "User Notification",
        "message": "This is for a specific user",
        "priority": "normal",
        "source": "account",
        "user_id": "user-456"
    });
    
    let response = app.clone().oneshot(
        Request::builder()
            .method("POST")
            .uri("/")
            .header("Content-Type", "application/json")
            .body(Body::from(serde_json::to_string(&payload).unwrap()))
            .unwrap(),
    ).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    
    let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body_json: Value = serde_json::from_slice(&body_bytes).unwrap();
    
    assert_eq!(body_json["status"], "success");
    assert_eq!(body_json["data"]["title"], "User Notification");
    // Check that user_id is in metadata
    assert_eq!(body_json["data"]["metadata"]["user_id"], "user-456");
}

#[tokio::test]
async fn test_mark_as_read() {
    // 1. Initialize NotificationManager
    let (manager, _rx) = NotificationManager::new();
    let manager = Arc::new(manager);
    
    // 2. Build Router
    let state = notifications::NotificationApiState::new(manager.clone());
    let app = notifications::routes(state);

    // 3. Test POST /:id/read
    let response = app.clone().oneshot(
        Request::builder()
            .method("POST")
            .uri("/some-notification-id/read")
            .body(Body::empty())
            .unwrap(),
    ).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    
    let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body_json: Value = serde_json::from_slice(&body_bytes).unwrap();
    
    assert_eq!(body_json["status"], "success");
    assert_eq!(body_json["data"]["read"], true);
}
