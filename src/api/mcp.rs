//! Model Context Protocol - LLM tool-use interface

use axum::{Router, routing::post, Json, http::StatusCode};
use serde::{Deserialize, Serialize};

/// A public MCP tool descriptor returned to LLM clients.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpTool {
    pub name: String,
    pub description: String,
    pub parameters: serde_json::Value,
}

/// An MCP request from an LLM client.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpRequest {
    pub tool: String,
    pub parameters: serde_json::Value,
    /// Optional caller session / request ID.
    pub request_id: Option<String>,
}

/// An MCP response returned to an LLM client.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpResponse {
    pub request_id: Option<String>,
    pub result: Option<serde_json::Value>,
    pub error: Option<String>,
}

impl McpResponse {
    pub fn ok(request_id: Option<String>, result: serde_json::Value) -> Self {
        Self { request_id, result: Some(result), error: None }
    }

    pub fn err(request_id: Option<String>, message: impl Into<String>) -> Self {
        Self { request_id, result: None, error: Some(message.into()) }
    }

    pub fn is_ok(&self) -> bool {
        self.error.is_none()
    }
}

/// The MCP server — manages tool registration and dispatches requests.
pub struct McpServer {
    tools: Vec<McpTool>,
}

impl McpServer {
    pub fn new() -> Self {
        Self { tools: Vec::new() }
    }

    /// Register a tool.
    pub fn register_tool(&mut self, tool: McpTool) {
        self.tools.push(tool);
    }

    /// List registered tools.
    pub fn list_tools(&self) -> &[McpTool] {
        &self.tools
    }

    /// Execute a request by tool name.
    pub fn execute(&self, req: McpRequest) -> McpResponse {
        if self.tools.iter().any(|t| t.name == req.tool) {
            McpResponse::ok(
                req.request_id,
                serde_json::json!({"status": "executed", "tool": req.tool}),
            )
        } else {
            McpResponse::err(req.request_id, format!("Unknown tool: {}", req.tool))
        }
    }
}

impl Default for McpServer {
    fn default() -> Self {
        Self::new()
    }
}

pub fn routes() -> Router {
    Router::new()
        .route("/tools", post(list_tools))
        .route("/execute", post(execute_tool))
}

#[derive(Serialize)]
struct Tool {
    name: String,
    description: String,
    parameters: serde_json::Value,
}

async fn list_tools() -> Json<Vec<Tool>> {
    Json(vec![
        Tool {
            name: "query".to_string(),
            description: "Execute a SQL query".to_string(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "sql": {"type": "string", "description": "SQL query to execute"}
                },
                "required": ["sql"]
            }),
        },
        Tool {
            name: "list_tables".to_string(),
            description: "List all tables".to_string(),
            parameters: serde_json::json!({"type": "object", "properties": {}}),
        },
        Tool {
            name: "describe_table".to_string(),
            description: "Get table schema".to_string(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "table": {"type": "string", "description": "Table name"}
                },
                "required": ["table"]
            }),
        },
    ])
}

#[derive(Deserialize)]
struct ToolExecution {
    tool: String,
    parameters: serde_json::Value,
}

async fn execute_tool(Json(req): Json<ToolExecution>) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    match req.tool.as_str() {
        "query" => {
            let sql = req.parameters.get("sql")
                .and_then(|v| v.as_str())
                .ok_or((StatusCode::BAD_REQUEST, "Missing sql parameter".to_string()))?;
            Ok(Json(serde_json::json!({"result": format!("Executed: {}", sql)})))
        }
        "list_tables" => Ok(Json(serde_json::json!({"tables": ["users", "orders"]}))),
        "describe_table" => Ok(Json(serde_json::json!({"schema": {}}))),
        _ => Err((StatusCode::NOT_FOUND, "Unknown tool".to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mcp_server_register_and_list_tools() {
        let mut server = McpServer::new();
        server.register_tool(McpTool {
            name: "query".to_string(),
            description: "Execute SQL".to_string(),
            parameters: serde_json::json!({}),
        });
        assert_eq!(server.list_tools().len(), 1);
        assert_eq!(server.list_tools()[0].name, "query");
    }

    #[test]
    fn test_mcp_execute_known_tool() {
        let mut server = McpServer::new();
        server.register_tool(McpTool {
            name: "list_tables".to_string(),
            description: "List tables".to_string(),
            parameters: serde_json::json!({}),
        });
        let resp = server.execute(McpRequest {
            tool: "list_tables".to_string(),
            parameters: serde_json::json!({}),
            request_id: Some("req-1".to_string()),
        });
        assert!(resp.is_ok());
        assert_eq!(resp.request_id, Some("req-1".to_string()));
    }

    #[test]
    fn test_mcp_execute_unknown_tool_returns_error() {
        let server = McpServer::new();
        let resp = server.execute(McpRequest {
            tool: "nonexistent".to_string(),
            parameters: serde_json::json!({}),
            request_id: None,
        });
        assert!(!resp.is_ok());
        assert!(resp.error.unwrap().contains("nonexistent"));
    }

    #[test]
    fn test_mcp_response_ok_and_err() {
        let ok = McpResponse::ok(None, serde_json::json!({"rows": []}));
        assert!(ok.is_ok());

        let err = McpResponse::err(Some("id-1".to_string()), "oops");
        assert!(!err.is_ok());
        assert_eq!(err.request_id, Some("id-1".to_string()));
    }
}
