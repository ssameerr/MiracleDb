//! API Middleware - Authentication, rate limiting, tracing, etc.

use axum::{middleware::Next, extract::Request, response::Response, http::{StatusCode, header}, Extension};
use jsonwebtoken::{decode, DecodingKey, Validation, Algorithm};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Claims {
    pub sub: String,
    pub exp: usize,
    pub roles: Vec<String>,
}

/// Request context extracted by middleware
#[derive(Clone, Debug)]
pub struct RequestContext {
    pub user_id: Option<String>,
    pub roles: Vec<String>,
    pub request_id: String,
    pub client_ip: Option<String>,
}

/// JWT authentication middleware
pub async fn auth_middleware(mut request: Request, next: Next) -> Result<Response, StatusCode> {
    let path = request.uri().path();
    
    // Skip auth for public endpoints
    let public_paths = [
        "/health",
        "/ready", 
        "/metrics",
        "/api/docs",
        "/api/docs/openapi.json",
        "/api/v1/debug/benchmark",
    ];
    
    // Check exact matches
    if public_paths.contains(&path) {
        return Ok(next.run(request).await);
    }
    
    // Check prefix matches for public paths
    let public_prefixes = [
        "/graphql",      // GraphQL introspection needs to work
        "/ws/",          // WebSocket upgrade happens before auth header is available  
        "/events/",      // SSE connections
        "/admin",        // Admin Dashboard
    ];
    
    for prefix in public_prefixes {
        if path.starts_with(prefix) {
            return Ok(next.run(request).await);
        }
    }
    
    let api_key_header = request.headers()
        .get("X-API-Key")
        .and_then(|v| v.to_str().ok());

    // 1. API Key Auth
    if let Some(key) = api_key_header {
        let sec_mgr = request.extensions().get::<Arc<crate::security::SecurityManager>>();
        if let Some(mgr) = sec_mgr {
            if let Some(api_key) = mgr.api_keys.validate(key).await {
                // Return Claims-like object or populate extensions
                let claims = Claims {
                    sub: api_key.user_id,
                    exp: usize::MAX, // API keys don't expire in this JWT sense
                    roles: api_key.roles,
                };
                request.extensions_mut().insert(claims);
                return Ok(next.run(request).await);
            }
        }
    }

    let auth_header = request.headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok());
    
    match auth_header {
        Some(auth) if auth.starts_with("Bearer ") => {
            let token = &auth[7..];
            let secret = std::env::var("JWT_SECRET")
                .unwrap_or_else(|_| "default_secret_do_not_use_in_prod".to_string());
            let key = DecodingKey::from_secret(secret.as_bytes());
            
            // In a real scenario, use a proper validation config
            let validation = Validation::new(Algorithm::HS256);
            
            match decode::<Claims>(token, &key, &validation) {
                Ok(token_data) => {
                    // Token is valid, insert claims into request extension
                    request.extensions_mut().insert(token_data.claims);
                    Ok(next.run(request).await)
                }
                Err(_) => {
                    // Invalid token
                    Err(StatusCode::UNAUTHORIZED)
                }
            }
        }
        _ => {
            // Missing or invalid header
            Err(StatusCode::UNAUTHORIZED)
        }
    }
}

/// Rate limiter state
pub struct RateLimiter {
    requests: RwLock<HashMap<String, Vec<Instant>>>,
    max_requests: usize,
    window: Duration,
}

impl RateLimiter {
    pub fn new(max_requests: usize, window_seconds: u64) -> Self {
        Self {
            requests: RwLock::new(HashMap::new()),
            max_requests,
            window: Duration::from_secs(window_seconds),
        }
    }

    pub async fn check(&self, key: &str) -> bool {
        let now = Instant::now();
        let mut requests = self.requests.write().await;
        
        let entry = requests.entry(key.to_string()).or_insert_with(Vec::new);
        
        // Remove old requests
        entry.retain(|t| now.duration_since(*t) < self.window);
        
        if entry.len() >= self.max_requests {
            return false;
        }
        
        entry.push(now);
        true
    }
}

/// Rate limiting middleware (with state)
pub async fn rate_limit_middleware(
    // Access state via Extension
    Extension(limiter): Extension<Arc<RateLimiter>>,
    request: Request, 
    next: Next
) -> Result<Response, StatusCode> {
    // Extract client IP (simplified, should use X-Forwarded-For in real reverse proxy setup)
    let client_ip = request
        .extensions()
        .get::<axum::extract::ConnectInfo<std::net::SocketAddr>>()
        .map(|ci| ci.0.ip().to_string())
        .unwrap_or_else(|| "unknown".to_string());

    if !limiter.check(&client_ip).await {
         tracing::warn!("Rate limit exceeded for {}", client_ip);
         return Err(StatusCode::TOO_MANY_REQUESTS);
    }
    
    Ok(next.run(request).await)
}

/// Request logging middleware
pub async fn logging_middleware(request: Request, next: Next) -> Result<Response, StatusCode> {
    let method = request.method().clone();
    let uri = request.uri().clone();
    let start = Instant::now();
    
    let request_id = uuid::Uuid::new_v4().to_string();
    
    tracing::info!(
        request_id = %request_id,
        method = %method,
        uri = %uri,
        "Request started"
    );
    
    let response = next.run(request).await;
    
    let duration = start.elapsed();
    
    tracing::info!(
        request_id = %request_id,
        method = %method,
        uri = %uri,
        status = %response.status().as_u16(),
        duration_ms = %duration.as_millis(),
        "Request completed"
    );
    
    Ok(response)
}

/// CORS middleware
pub async fn cors_middleware(request: Request, next: Next) -> Result<Response, StatusCode> {
    let mut response = next.run(request).await;
    
    let headers = response.headers_mut();
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        "*".parse().unwrap(),
    );
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_METHODS,
        "GET, POST, PUT, DELETE, OPTIONS".parse().unwrap(),
    );
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_HEADERS,
        "Content-Type, Authorization".parse().unwrap(),
    );
    
    Ok(response)
}

/// Compression middleware
pub async fn compression_middleware(request: Request, next: Next) -> Result<Response, StatusCode> {
    let accept_encoding = request.headers()
        .get(header::ACCEPT_ENCODING)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    
    let response = next.run(request).await;
    
    // In production: compress response if accept_encoding contains gzip/deflate
    // and response body is large enough
    
    Ok(response)
}

/// Timeout middleware
pub async fn timeout_middleware(request: Request, next: Next) -> Result<Response, StatusCode> {
    let timeout = Duration::from_secs(30);
    
    match tokio::time::timeout(timeout, next.run(request)).await {
        Ok(response) => Ok(response),
        Err(_) => Err(StatusCode::REQUEST_TIMEOUT),
    }
}

/// Request ID middleware
pub async fn request_id_middleware(mut request: Request, next: Next) -> Result<Response, StatusCode> {
    let request_id = uuid::Uuid::new_v4().to_string();
    
    request.headers_mut().insert(
        "X-Request-ID",
        request_id.parse().unwrap(),
    );
    
    let mut response = next.run(request).await;
    
    response.headers_mut().insert(
        "X-Request-ID",
        request_id.parse().unwrap(),
    );
    
    Ok(response)
}
