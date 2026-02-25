use axum::{
    response::{Html, IntoResponse},
    Json,
};
use serde_json::Value;
use crate::docs::{
    DocGenerator, ApiInfo, EndpointDoc, ParameterDoc, ParameterLocation, 
    ResponseDoc, BodyDoc, SchemaDoc, PropertyDoc
};

/// Serves the OpenAPI 3.0 JSON spec
pub async fn openapi_json() -> Json<Value> {
    let mut gen = DocGenerator::new(ApiInfo {
        title: "MiracleDb API".to_string(),
        version: "0.1.0".to_string(),
        description: "The Ultimate Database API - SQL, Vector, Graph, Streams. Use 'Authorize' button to add Bearer token.".to_string(),
        contact: Some("support@miracledb.com".to_string()),
        license: Some("Commercial".to_string()),
    });

    // --- Add Endpoints ---

    // 1. Health Check
    gen.add_endpoint(EndpointDoc {
        path: "/health".to_string(),
        method: "GET".to_string(),
        summary: "Health Check".to_string(),
        description: "Returns 200 OK if the server is running accessible.".to_string(),
        parameters: vec![],
        request_body: None,
        responses: vec![
            ResponseDoc {
                status_code: 200,
                description: "Server is healthy".to_string(),
                body: None,
            }
        ],
        tags: vec!["System".to_string()],
        deprecated: false,
    });

    // 2. SQL Query
    gen.add_endpoint(EndpointDoc {
        path: "/api/v1/sql".to_string(),
        method: "POST".to_string(),
        summary: "Execute SQL".to_string(),
        description: "Execute a SQL query against the MiracleEngine.".to_string(),
        parameters: vec![],
        request_body: Some(BodyDoc {
            content_type: "application/json".to_string(),
            schema: SchemaDoc {
                schema_type: "object".to_string(),
                properties: vec![
                    PropertyDoc {
                        name: "query".to_string(),
                        property_type: "string".to_string(),
                        description: "SQL query string".to_string(),
                        nullable: false,
                        example: Some(serde_json::json!("SELECT * FROM users")),
                    }
                ],
                required: vec!["query".to_string()],
            },
            example: None,
        }),
        responses: vec![
            ResponseDoc {
                status_code: 200,
                description: "Query executed successfully".to_string(),
                body: Some(BodyDoc {
                    content_type: "application/json".to_string(),
                    schema: SchemaDoc {
                        schema_type: "object".to_string(),
                        properties: vec![
                            PropertyDoc { name: "status".to_string(), property_type: "string".to_string(), description: "success or error".to_string(), nullable: false, example: None },
                            PropertyDoc { name: "data".to_string(), property_type: "array".to_string(), description: "Result rows".to_string(), nullable: true, example: None },
                            PropertyDoc { name: "error".to_string(), property_type: "string".to_string(), description: "Error message".to_string(), nullable: true, example: None },
                        ],
                        required: vec!["status".to_string()],
                    },
                    example: None,
                }),
            }
        ],
        tags: vec!["Query".to_string()],
        deprecated: false,
    });

    // 3. RLM Agent
    gen.add_endpoint(EndpointDoc {
        path: "/api/v1/agent/run".to_string(),
        method: "POST".to_string(),
        summary: "Run RLM Agent".to_string(),
        description: "Submit a goal to the Recursive Language Model agent.".to_string(),
        parameters: vec![],
        request_body: Some(BodyDoc {
            content_type: "application/json".to_string(),
            schema: SchemaDoc {
                schema_type: "object".to_string(),
                properties: vec![
                    PropertyDoc {
                        name: "goal".to_string(),
                        property_type: "string".to_string(),
                        description: "Natural language goal description".to_string(),
                        nullable: false,
                        example: Some(serde_json::json!("Analyze active users in the last 24h")),
                    }
                ],
                required: vec!["goal".to_string()],
            },
            example: None,
        }),
        responses: vec![
            ResponseDoc {
                status_code: 200,
                description: "Agent finished execution".to_string(),
                body: None,
            }
        ],
        tags: vec!["AI Agent".to_string()],
        deprecated: false,
    });

    // 4. Metrics
    gen.add_endpoint(EndpointDoc {
        path: "/metrics".to_string(),
        method: "GET".to_string(),
        summary: "Prometheus Metrics".to_string(),
        description: "Exposes system metrics for Prometheus scraping.".to_string(),
        parameters: vec![],
        request_body: None,
        responses: vec![
            ResponseDoc {
                status_code: 200,
                description: "Metrics text".to_string(),
                body: None,
            }
        ],
        tags: vec!["System".to_string()],
        deprecated: false,
    });

    Json(gen.to_openapi())
}

/// Serves the Swagger UI HTML
pub async fn swagger_ui() -> impl IntoResponse {
    Html(r#"
<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="UTF-8">
    <title>MiracleDb Swagger UI</title>
    <link rel="stylesheet" type="text/css" href="https://cdnjs.cloudflare.com/ajax/libs/swagger-ui/5.11.0/swagger-ui.css" />
    <style>
      html
      {
        box-sizing: border-box;
        overflow: -moz-scrollbars-vertical;
        overflow-y: scroll;
      }

      *,
      *:before,
      *:after
      {
        box-sizing: inherit;
      }

      body
      {
        margin:0;
        background: #fafafa;
      }
    </style>
  </head>

  <body>
    <div id="swagger-ui"></div>

    <script src="https://cdnjs.cloudflare.com/ajax/libs/swagger-ui/5.11.0/swagger-ui-bundle.js" charset="UTF-8"> </script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/swagger-ui/5.11.0/swagger-ui-standalone-preset.js" charset="UTF-8"> </script>
    <script>
    window.onload = function() {
      // Begin Swagger UI call region
      const ui = SwaggerUIBundle({
        url: "/api/docs/openapi.json",
        dom_id: '#swagger-ui',
        deepLinking: true,
        presets: [
          SwaggerUIBundle.presets.apis,
          SwaggerUIStandalonePreset
        ],
        plugins: [
          SwaggerUIBundle.plugins.DownloadUrl
        ],
        layout: "StandaloneLayout"
      });
      // End Swagger UI call region

      window.ui = ui;
    };
  </script>
  </body>
</html>
"#)
}
