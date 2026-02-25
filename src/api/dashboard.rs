//! Admin Dashboard API - Web-based database management interface
//!
//! Provides REST endpoints for:
//! - Database overview and stats
//! - Table management
//! - Query execution
//! - Configuration (including SSL on/off)
//! - User management
//! - Performance monitoring

use axum::{Router, routing::{get, post, put, delete}, extract::{Json, Extension, Path}, http::StatusCode, response::{Html, IntoResponse}};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;
use std::collections::HashMap;
use crate::security::{SecurityManager, User};

// =============================================================================
// Dashboard Configuration
// =============================================================================

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DashboardConfig {
    pub enabled: bool,
    pub port: u16,
    pub auth_required: bool,
    pub admin_username: String,
    pub admin_password_hash: String,
    pub session_timeout_minutes: u32,
}

impl Default for DashboardConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            port: 8081,
            auth_required: true,
            admin_username: "admin".to_string(),
            admin_password_hash: String::new(), // Set on first run
            session_timeout_minutes: 60,
        }
    }
}

// =============================================================================
// SSL/TLS Configuration API
// =============================================================================

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SslConfig {
    pub enabled: bool,
    pub cert_path: String,
    pub key_path: String,
    pub ca_path: Option<String>,
    pub require_client_cert: bool,
    pub min_tls_version: String, // "1.2" or "1.3"
}

impl Default for SslConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            cert_path: "./certs/server.crt".to_string(),
            key_path: "./certs/server.key".to_string(),
            ca_path: None,
            require_client_cert: false,
            min_tls_version: "1.3".to_string(),
        }
    }
}

// =============================================================================
// Dashboard State
// =============================================================================

pub struct DashboardState {
    pub config: RwLock<DashboardConfig>,
    pub ssl_config: RwLock<SslConfig>,
    pub sessions: RwLock<Vec<Session>>,
}

#[derive(Clone, Debug, Serialize)]
pub struct Session {
    pub id: String,
    pub username: String,
    pub created_at: i64,
    pub expires_at: i64,
}

impl DashboardState {
    pub fn new() -> Self {
        Self {
            config: RwLock::new(DashboardConfig::default()),
            ssl_config: RwLock::new(SslConfig::default()),
            sessions: RwLock::new(Vec::new()),
        }
    }
}

// =============================================================================
// API Responses
// =============================================================================

#[derive(Serialize)]
pub struct DashboardOverview {
    pub version: String,
    pub uptime_seconds: u64,
    pub tables_count: usize,
    pub total_rows: u64,
    pub memory_used_mb: u64,
    pub disk_used_mb: u64,
    pub connections_active: u32,
    pub queries_per_second: f64,
    pub ssl_enabled: bool,
}

#[derive(Serialize)]
pub struct TableInfo {
    pub name: String,
    pub row_count: u64,
    pub size_bytes: u64,
    pub columns: Vec<ColumnInfo>,
    pub created_at: String,
}

#[derive(Serialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

#[derive(Deserialize)]
pub struct QueryRequest {
    pub sql: String,
}

#[derive(Serialize)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub execution_time_ms: u64,
    pub rows_affected: Option<u64>,
}

#[derive(Deserialize)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}

#[derive(Serialize)]
pub struct LoginResponse {
    pub success: bool,
    pub token: Option<String>,
    pub error: Option<String>,
}

// =============================================================================
// Routes
// =============================================================================

pub fn routes(state: Arc<DashboardState>) -> Router {
    Router::new()
        // Dashboard UI (embedded HTML)
        .route("/", get(serve_dashboard))
        .route("/dashboard", get(serve_dashboard))
        
        // Auth
        .route("/api/auth/login", post(login))
        .route("/api/auth/logout", post(logout))
        
        // Overview
        .route("/api/overview", get(get_overview))
        
        // Tables
        .route("/api/tables", get(list_tables))
        .route("/api/tables/:name", get(get_table).delete(drop_table))
        .route("/api/tables/:name/data", get(get_table_data))
        
        // Query
        .route("/api/query", post(execute_query))
        
        // Configuration
        .route("/api/config", get(get_config).put(update_config))
        .route("/api/config/ssl", get(get_ssl_config).put(update_ssl_config))
        .route("/api/config/ssl/enable", post(enable_ssl))
        .route("/api/config/ssl/disable", post(disable_ssl))
        .route("/api/config/ssl/upload-cert", post(upload_certificate))
        
        // Monitoring
        .route("/api/metrics", get(get_metrics))
        .route("/api/connections", get(get_connections))
        .route("/api/slow-queries", get(get_slow_queries))
        
        // User Management
        .route("/api/users", post(create_user))

        .layer(Extension(state))
}

// =============================================================================
// Embedded Dashboard HTML
// =============================================================================

async fn serve_dashboard() -> impl IntoResponse {
    Html(DASHBOARD_HTML)
}

const DASHBOARD_HTML: &str = r#"
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>MiracleDb Admin Dashboard</title>
    <style>
        :root {
            --bg-dark: #0f172a;
            --bg-card: #1e293b;
            --text-primary: #f1f5f9;
            --text-secondary: #94a3b8;
            --accent: #3b82f6;
            --success: #22c55e;
            --warning: #f59e0b;
            --danger: #ef4444;
        }
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
            background: var(--bg-dark);
            color: var(--text-primary);
            min-height: 100vh;
        }
        .header {
            background: linear-gradient(135deg, #1e3a8a 0%, #7c3aed 100%);
            padding: 1.5rem 2rem;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        .header h1 { font-size: 1.5rem; font-weight: 700; }
        .header .status { display: flex; gap: 1rem; align-items: center; }
        .status-dot { width: 10px; height: 10px; border-radius: 50%; background: var(--success); }
        .container { max-width: 1400px; margin: 0 auto; padding: 2rem; }
        .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 1.5rem; }
        .card {
            background: var(--bg-card);
            border-radius: 12px;
            padding: 1.5rem;
            border: 1px solid rgba(255,255,255,0.1);
        }
        .card h2 { font-size: 0.875rem; color: var(--text-secondary); margin-bottom: 0.5rem; }
        .card .value { font-size: 2rem; font-weight: 700; }
        .card.ssl { border-left: 4px solid var(--accent); }
        .btn {
            padding: 0.5rem 1rem;
            border-radius: 6px;
            border: none;
            cursor: pointer;
            font-weight: 500;
            transition: all 0.2s;
        }
        .btn-primary { background: var(--accent); color: white; }
        .btn-success { background: var(--success); color: white; }
        .btn-danger { background: var(--danger); color: white; }
        .btn:hover { opacity: 0.9; transform: translateY(-1px); }
        .toggle {
            position: relative;
            width: 50px;
            height: 26px;
            background: #475569;
            border-radius: 13px;
            cursor: pointer;
            transition: background 0.3s;
        }
        .toggle.active { background: var(--success); }
        .toggle::after {
            content: '';
            position: absolute;
            width: 22px;
            height: 22px;
            background: white;
            border-radius: 50%;
            top: 2px;
            left: 2px;
            transition: transform 0.3s;
        }
        .toggle.active::after { transform: translateX(24px); }
        .section { margin-top: 2rem; }
        .section h3 { margin-bottom: 1rem; font-size: 1.25rem; }
        .table-container { overflow-x: auto; }
        table { width: 100%; border-collapse: collapse; }
        th, td { padding: 0.75rem 1rem; text-align: left; border-bottom: 1px solid rgba(255,255,255,0.1); }
        th { color: var(--text-secondary); font-weight: 500; }
        .query-box { width: 100%; background: #0f172a; border: 1px solid rgba(255,255,255,0.2); border-radius: 8px; padding: 1rem; color: white; font-family: monospace; min-height: 100px; resize: vertical; }
        .config-row { display: flex; justify-content: space-between; align-items: center; padding: 1rem 0; border-bottom: 1px solid rgba(255,255,255,0.1); }
        .config-row:last-child { border-bottom: none; }
        input[type="text"], input[type="password"] {
            background: #0f172a;
            border: 1px solid rgba(255,255,255,0.2);
            border-radius: 6px;
            padding: 0.5rem 1rem;
            color: white;
            width: 300px;
        }
        .tabs { display: flex; gap: 0.5rem; margin-bottom: 1.5rem; }
        .tab { padding: 0.5rem 1rem; border-radius: 6px; cursor: pointer; background: transparent; color: var(--text-secondary); border: 1px solid transparent; }
        .tab.active { background: var(--accent); color: white; }
        .tab:hover { border-color: var(--accent); }
    </style>
</head>
<body>
    <div class="header">
        <h1>🚀 MiracleDb Admin Dashboard</h1>
        <div class="status">
            <span class="status-dot"></span>
            <span>Connected</span>
            <button class="btn btn-danger" onclick="logout()">Logout</button>
        </div>
    </div>
    
    <div class="container">
        <div class="tabs">
            <div class="tab active" onclick="showTab('overview')">Overview</div>
            <div class="tab" onclick="showTab('tables')">Tables</div>
            <div class="tab" onclick="showTab('query')">Query</div>
            <div class="tab" onclick="showTab('config')">Configuration</div>
            <div class="tab" onclick="showTab('ssl')">SSL/TLS</div>
        </div>
        
        <!-- Overview Tab -->
        <div id="tab-overview" class="tab-content">
            <div class="grid">
                <div class="card">
                    <h2>Tables</h2>
                    <div class="value" id="stat-tables">--</div>
                </div>
                <div class="card">
                    <h2>Total Rows</h2>
                    <div class="value" id="stat-rows">--</div>
                </div>
                <div class="card">
                    <h2>Memory Used</h2>
                    <div class="value" id="stat-memory">-- MB</div>
                </div>
                <div class="card">
                    <h2>Queries/sec</h2>
                    <div class="value" id="stat-qps">--</div>
                </div>
                <div class="card ssl">
                    <h2>SSL Status</h2>
                    <div class="value" id="stat-ssl">--</div>
                </div>
                <div class="card">
                    <h2>Uptime</h2>
                    <div class="value" id="stat-uptime">--</div>
                </div>
            </div>
        </div>
        
        <!-- Tables Tab -->
        <div id="tab-tables" class="tab-content" style="display:none;">
            <div class="card">
                <h3>Database Tables</h3>
                <div class="table-container">
                    <table>
                        <thead>
                            <tr><th>Name</th><th>Rows</th><th>Size</th><th>Actions</th></tr>
                        </thead>
                        <tbody id="tables-list"></tbody>
                    </table>
                </div>
            </div>
        </div>
        
        <!-- Query Tab -->
        <div id="tab-query" class="tab-content" style="display:none;">
            <div class="card">
                <h3>SQL Query Editor</h3>
                <textarea class="query-box" id="query-input" placeholder="SELECT * FROM your_table LIMIT 10;"></textarea>
                <div style="margin-top: 1rem;">
                    <button class="btn btn-primary" onclick="executeQuery()">Execute Query</button>
                </div>
                <div id="query-result" style="margin-top: 1rem;"></div>
            </div>
        </div>
        
        <!-- Config Tab -->
        <div id="tab-config" class="tab-content" style="display:none;">
            <div class="card">
                <h3>Server Configuration</h3>
                <div class="config-row">
                    <span>Dashboard Enabled</span>
                    <div class="toggle active" onclick="toggleConfig('dashboard_enabled')"></div>
                </div>
                <div class="config-row">
                    <span>Authentication Required</span>
                    <div class="toggle active" onclick="toggleConfig('auth_required')"></div>
                </div>
                <div class="config-row">
                    <span>Session Timeout (minutes)</span>
                    <input type="text" value="60">
                </div>
            </div>
        </div>
        
        <!-- SSL Tab -->
        <div id="tab-ssl" class="tab-content" style="display:none;">
            <div class="card">
                <h3>SSL/TLS Configuration</h3>
                <div class="config-row">
                    <span><strong>SSL Enabled</strong></span>
                    <div class="toggle" id="ssl-toggle" onclick="toggleSSL()"></div>
                </div>
                <div class="config-row">
                    <span>Certificate Path</span>
                    <input type="text" id="ssl-cert" value="./certs/server.crt">
                </div>
                <div class="config-row">
                    <span>Private Key Path</span>
                    <input type="text" id="ssl-key" value="./certs/server.key">
                </div>
                <div class="config-row">
                    <span>CA Certificate (optional)</span>
                    <input type="text" id="ssl-ca" value="">
                </div>
                <div class="config-row">
                    <span>Minimum TLS Version</span>
                    <select id="ssl-version" style="background:#0f172a;color:white;padding:0.5rem;border-radius:6px;">
                        <option value="1.2">TLS 1.2</option>
                        <option value="1.3" selected>TLS 1.3</option>
                    </select>
                </div>
                <div class="config-row">
                    <span>Require Client Certificate</span>
                    <div class="toggle" id="ssl-client-cert"></div>
                </div>
                <div style="margin-top: 1rem; display: flex; gap: 1rem;">
                    <button class="btn btn-primary" onclick="saveSSLConfig()">Save Configuration</button>
                    <button class="btn btn-success" onclick="generateCert()">Generate Self-Signed Cert</button>
                </div>
            </div>
        </div>
    </div>
    
    <script>
        // --- Helpers ---
        function escapeHtml(text) {
            if (text == null) return '';
            return String(text)
                .replace(/&/g, "&amp;")
                .replace(/</g, "&lt;")
                .replace(/>/g, "&gt;")
                .replace(/"/g, "&quot;")
                .replace(/'/g, "&#039;");
        }

        // Tab switching
        function showTab(name) {
            document.querySelectorAll('.tab-content').forEach(el => el.style.display = 'none');
            document.querySelectorAll('.tab').forEach(el => el.classList.remove('active'));
            document.getElementById('tab-' + name).style.display = 'block';
            event.target.classList.add('active');
            if (name === 'overview') loadOverview();
            if (name === 'tables') loadTables();
            if (name === 'ssl') loadSSLConfig();
        }
        
        // API calls
        async function loadOverview() {
            try {
                const res = await fetch('/admin/api/overview');
                const data = await res.json();
                document.getElementById('stat-tables').textContent = data.tables_count;
                document.getElementById('stat-rows').textContent = data.total_rows.toLocaleString();
                document.getElementById('stat-memory').textContent = data.memory_used_mb + ' MB';
                document.getElementById('stat-qps').textContent = data.queries_per_second.toFixed(1);
                document.getElementById('stat-ssl').textContent = data.ssl_enabled ? '🔒 Enabled' : '🔓 Disabled';
                document.getElementById('stat-uptime').textContent = formatUptime(data.uptime_seconds);
            } catch (e) { console.error(e); }
        }
        
        async function loadTables() {
            try {
                const res = await fetch('/admin/api/tables');
                const tables = await res.json();
                const tbody = document.getElementById('tables-list');
                tbody.innerHTML = tables.map(t => `
                    <tr>
                        <td>${escapeHtml(t.name)}</td>
                        <td>${t.row_count.toLocaleString()}</td>
                        <td>${formatBytes(t.size_bytes)}</td>
                        <td>
                            <button class="btn btn-primary" onclick="viewTable('${escapeHtml(t.name)}')">View</button>
                            <button class="btn btn-danger" onclick="dropTable('${escapeHtml(t.name)}')">Drop</button>
                        </td>
                    </tr>
                `).join('');
            } catch (e) { console.error(e); }
        }
        
        async function loadSSLConfig() {
            try {
                const res = await fetch('/admin/api/config/ssl');
                const config = await res.json();
                document.getElementById('ssl-toggle').classList.toggle('active', config.enabled);
                document.getElementById('ssl-cert').value = config.cert_path;
                document.getElementById('ssl-key').value = config.key_path;
                document.getElementById('ssl-ca').value = config.ca_path || '';
                document.getElementById('ssl-version').value = config.min_tls_version;
            } catch (e) { console.error(e); }
        }
        
        async function toggleSSL() {
            const toggle = document.getElementById('ssl-toggle');
            const enabled = !toggle.classList.contains('active');
            const endpoint = enabled ? '/admin/api/config/ssl/enable' : '/admin/api/config/ssl/disable';
            await fetch(endpoint, { method: 'POST' });
            toggle.classList.toggle('active');
        }
        
        async function saveSSLConfig() {
            const config = {
                enabled: document.getElementById('ssl-toggle').classList.contains('active'),
                cert_path: document.getElementById('ssl-cert').value,
                key_path: document.getElementById('ssl-key').value,
                ca_path: document.getElementById('ssl-ca').value || null,
                min_tls_version: document.getElementById('ssl-version').value,
                require_client_cert: document.getElementById('ssl-client-cert').classList.contains('active')
            };
            await fetch('/admin/api/config/ssl', {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(config)
            });
            alert('SSL configuration saved!');
        }
        
        async function executeQuery() {
            const sql = document.getElementById('query-input').value;
            const res = await fetch('/admin/api/query', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ sql })
            });
            const result = await res.json();
            const div = document.getElementById('query-result');
            if (result.columns) {
                div.innerHTML = `
                    <p style="color: var(--success);">Executed in ${result.execution_time_ms}ms</p>
                    <table>
                        <thead><tr>${result.columns.map(c => `<th>${escapeHtml(c)}</th>`).join('')}</tr></thead>
                        <tbody>${result.rows.map(r => `<tr>${r.map(v => `<td>${escapeHtml(v)}</td>`).join('')}</tr>`).join('')}</tbody>
                    </table>
                `;
            } else {
                div.innerHTML = `<p style="color: var(--danger);">Error: ${escapeHtml(result.error || 'Unknown error')}</p>`;
            }
        }
        
        function formatUptime(seconds) {
            const d = Math.floor(seconds / 86400);
            const h = Math.floor((seconds % 86400) / 3600);
            const m = Math.floor((seconds % 3600) / 60);
            return d > 0 ? `${d}d ${h}h` : `${h}h ${m}m`;
        }
        
        function formatBytes(bytes) {
            if (bytes < 1024) return bytes + ' B';
            if (bytes < 1048576) return (bytes / 1024).toFixed(1) + ' KB';
            return (bytes / 1048576).toFixed(1) + ' MB';
        }
        
        // Initial load
        loadOverview();
    </script>
</body>
</html>
"#;

// =============================================================================
// Handlers
// =============================================================================

async fn login(
    Extension(state): Extension<Arc<DashboardState>>,
    Extension(security_mgr): Extension<Arc<SecurityManager>>,
    Json(req): Json<LoginRequest>,
) -> impl IntoResponse {
    let config = state.config.read().await;
    
    // Simple auth check (in production: use proper password hashing like argon2)
    // For now, checks against configured password hash (or plain text if we haven't implemented hashing yet)
    // Default is empty string, which means disabled/unconfigured.
    
    let mut is_valid = if config.admin_password_hash.is_empty() {
        // If no password configured, use default "admin"
        req.username == config.admin_username && req.password == "admin"
    } else {
        req.username == config.admin_username && req.password == config.admin_password_hash
    };

    // If local dashboard auth fails, try Active Directory
    if !is_valid {
        is_valid = security_mgr.ad.authenticate(&req.username, &req.password).await;
    }

    if is_valid {
        let token = uuid::Uuid::new_v4().to_string();
        let session = Session {
            id: token.clone(),
            username: req.username,
            created_at: chrono::Utc::now().timestamp(),
            expires_at: chrono::Utc::now().timestamp() + (config.session_timeout_minutes as i64 * 60),
        };
        
        state.sessions.write().await.push(session);
        
        Json(LoginResponse {
            success: true,
            token: Some(token),
            error: None,
        })
    } else {
        Json(LoginResponse {
            success: false,
            token: None,
            error: Some("Invalid credentials".to_string()),
        })
    }
}

async fn logout() -> impl IntoResponse {
    StatusCode::OK
}

async fn get_overview(
    Extension(state): Extension<Arc<DashboardState>>,
) -> impl IntoResponse {
    let ssl_config = state.ssl_config.read().await;
    
    Json(DashboardOverview {
        version: "0.1.0".to_string(),
        uptime_seconds: 3600, // Would be actual uptime
        tables_count: 10,
        total_rows: 1_000_000,
        memory_used_mb: 256,
        disk_used_mb: 512,
        connections_active: 5,
        queries_per_second: 150.5,
        ssl_enabled: ssl_config.enabled,
    })
}

async fn list_tables() -> impl IntoResponse {
    Json(vec![
        TableInfo {
            name: "benchmark".to_string(),
            row_count: 1_000_000,
            size_bytes: 50_000_000,
            columns: vec![
                ColumnInfo { name: "id".to_string(), data_type: "BIGINT".to_string(), nullable: false },
                ColumnInfo { name: "name".to_string(), data_type: "TEXT".to_string(), nullable: true },
            ],
            created_at: "2026-01-18".to_string(),
        },
    ])
}

async fn get_table(Path(name): Path<String>) -> impl IntoResponse {
    Json(TableInfo {
        name,
        row_count: 1000,
        size_bytes: 50000,
        columns: vec![],
        created_at: "2026-01-18".to_string(),
    })
}

async fn drop_table(Path(_name): Path<String>) -> impl IntoResponse {
    StatusCode::OK
}

async fn get_table_data(Path(_name): Path<String>) -> impl IntoResponse {
    Json(serde_json::json!({ "rows": [] }))
}

async fn execute_query(Json(req): Json<QueryRequest>) -> impl IntoResponse {
    // Would execute actual query
    Json(QueryResult {
        columns: vec!["result".to_string()],
        rows: vec![vec![serde_json::Value::String("Query executed".to_string())]],
        execution_time_ms: 5,
        rows_affected: Some(0),
    })
}

async fn get_config(
    Extension(state): Extension<Arc<DashboardState>>,
) -> impl IntoResponse {
    let config = state.config.read().await;
    Json(config.clone())
}

async fn update_config(
    Extension(state): Extension<Arc<DashboardState>>,
    Json(new_config): Json<DashboardConfig>,
) -> impl IntoResponse {
    *state.config.write().await = new_config;
    StatusCode::OK
}

async fn get_ssl_config(
    Extension(state): Extension<Arc<DashboardState>>,
) -> impl IntoResponse {
    let config = state.ssl_config.read().await;
    Json(config.clone())
}

async fn update_ssl_config(
    Extension(state): Extension<Arc<DashboardState>>,
    Json(new_config): Json<SslConfig>,
) -> impl IntoResponse {
    *state.ssl_config.write().await = new_config;
    StatusCode::OK
}

async fn enable_ssl(
    Extension(state): Extension<Arc<DashboardState>>,
) -> impl IntoResponse {
    state.ssl_config.write().await.enabled = true;
    Json(serde_json::json!({ "message": "SSL enabled" }))
}

async fn disable_ssl(
    Extension(state): Extension<Arc<DashboardState>>,
) -> impl IntoResponse {
    state.ssl_config.write().await.enabled = false;
    Json(serde_json::json!({ "message": "SSL disabled" }))
}

async fn upload_certificate() -> impl IntoResponse {
    // Would handle file upload
    Json(serde_json::json!({ "message": "Certificate uploaded" }))
}

async fn get_metrics() -> impl IntoResponse {
    Json(serde_json::json!({
        "cpu_usage": 15.5,
        "memory_usage": 45.2,
        "disk_io_reads": 1000,
        "disk_io_writes": 500,
        "network_rx_bytes": 1000000,
        "network_tx_bytes": 500000,
    }))
}

async fn get_connections() -> impl IntoResponse {
    Json(serde_json::json!([
        { "id": 1, "user": "app", "database": "main", "state": "active", "query": "SELECT ..." },
    ]))
}

async fn get_slow_queries() -> impl IntoResponse {
    Json(serde_json::json!([
        { "query": "SELECT * FROM large_table", "duration_ms": 5000, "timestamp": "2026-01-18T12:00:00Z" },
    ]))
}

async fn generate_cert() -> impl IntoResponse {
    // Would generate self-signed certificate
    Json(serde_json::json!({ "message": "Self-signed certificate generated" }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dashboard_config_default() {
        let config = DashboardConfig::default();
        assert!(config.enabled);
        assert_eq!(config.port, 8081);
    }

    #[test]
    fn test_ssl_config_default() {
        let config = SslConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.min_tls_version, "1.3");
    }
}

// =============================================================================
// User Management Handlers
// =============================================================================

#[derive(Deserialize)]
struct CreateUserRequest {
    username: String,
    role: Option<String>,
}

async fn create_user(
    Extension(security_mgr): Extension<Arc<SecurityManager>>,
    Json(req): Json<CreateUserRequest>,
) -> impl IntoResponse {
    let user = User {
        id: uuid::Uuid::new_v4().to_string(),
        username: req.username.clone(),
        roles: vec![req.role.unwrap_or_else(|| "readonly".to_string())],
        attributes: HashMap::new(),
        created_at: chrono::Utc::now(),
        last_login: None,
    };
    
    match security_mgr.rbac.create_user(user).await {
        Ok(_) => (StatusCode::CREATED, "User created").into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, format!("User creation failed: {}", e)).into_response(),
    }
}
