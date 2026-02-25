//! Documentation Module - API documentation generation

use std::collections::HashMap;
use serde::{Serialize, Deserialize};

/// API endpoint documentation
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EndpointDoc {
    pub path: String,
    pub method: String,
    pub summary: String,
    pub description: String,
    pub parameters: Vec<ParameterDoc>,
    pub request_body: Option<BodyDoc>,
    pub responses: Vec<ResponseDoc>,
    pub tags: Vec<String>,
    pub deprecated: bool,
}

/// Parameter documentation
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ParameterDoc {
    pub name: String,
    pub location: ParameterLocation,
    pub description: String,
    pub required: bool,
    pub param_type: String,
    pub default: Option<String>,
    pub example: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ParameterLocation {
    Path,
    Query,
    Header,
    Cookie,
}

/// Request/response body documentation
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BodyDoc {
    pub content_type: String,
    pub schema: SchemaDoc,
    pub example: Option<serde_json::Value>,
}

/// Response documentation
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResponseDoc {
    pub status_code: u16,
    pub description: String,
    pub body: Option<BodyDoc>,
}

/// Schema documentation
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SchemaDoc {
    pub schema_type: String,
    pub properties: Vec<PropertyDoc>,
    pub required: Vec<String>,
}

/// Property documentation
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PropertyDoc {
    pub name: String,
    pub property_type: String,
    pub description: String,
    pub nullable: bool,
    pub example: Option<serde_json::Value>,
}

/// Function documentation
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FunctionDoc {
    pub name: String,
    pub description: String,
    pub category: String,
    pub parameters: Vec<FunctionParamDoc>,
    pub return_type: String,
    pub examples: Vec<ExampleDoc>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FunctionParamDoc {
    pub name: String,
    pub param_type: String,
    pub description: String,
    pub optional: bool,
    pub default: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExampleDoc {
    pub title: String,
    pub sql: String,
    pub result: Option<String>,
}

/// Documentation generator
pub struct DocGenerator {
    endpoints: Vec<EndpointDoc>,
    functions: Vec<FunctionDoc>,
    info: ApiInfo,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ApiInfo {
    pub title: String,
    pub version: String,
    pub description: String,
    pub contact: Option<String>,
    pub license: Option<String>,
}

impl DocGenerator {
    pub fn new(info: ApiInfo) -> Self {
        Self {
            endpoints: vec![],
            functions: vec![],
            info,
        }
    }

    pub fn add_endpoint(&mut self, endpoint: EndpointDoc) {
        self.endpoints.push(endpoint);
    }

    pub fn add_function(&mut self, function: FunctionDoc) {
        self.functions.push(function);
    }

    /// Generate OpenAPI spec
    pub fn to_openapi(&self) -> serde_json::Value {
        let mut paths: HashMap<String, serde_json::Value> = HashMap::new();

        for endpoint in &self.endpoints {
            let operation = serde_json::json!({
                "summary": endpoint.summary,
                "description": endpoint.description,
                "tags": endpoint.tags,
                "deprecated": endpoint.deprecated,
                "parameters": endpoint.parameters.iter().map(|p| {
                    serde_json::json!({
                        "name": p.name,
                        "in": match p.location {
                            ParameterLocation::Path => "path",
                            ParameterLocation::Query => "query",
                            ParameterLocation::Header => "header",
                            ParameterLocation::Cookie => "cookie",
                        },
                        "description": p.description,
                        "required": p.required,
                        "schema": { "type": p.param_type },
                    })
                }).collect::<Vec<_>>(),
                "responses": endpoint.responses.iter().map(|r| {
                    (r.status_code.to_string(), serde_json::json!({
                        "description": r.description,
                    }))
                }).collect::<HashMap<_, _>>(),
            });

            let path_item = paths.entry(endpoint.path.clone())
                .or_insert_with(|| serde_json::json!({}));
            if let Some(obj) = path_item.as_object_mut() {
                obj.insert(endpoint.method.to_lowercase(), operation);
            }
        }

        serde_json::json!({
            "openapi": "3.0.0",
            "info": {
                "title": self.info.title,
                "version": self.info.version,
                "description": self.info.description,
            },
            "paths": paths,
            "components": {
                "securitySchemes": {
                    "bearerAuth": {
                        "type": "http",
                        "scheme": "bearer",
                        "bearerFormat": "JWT",
                        "description": "Enter JWT token. Use /api/v1/token endpoint to generate a token."
                    }
                }
            },
            "security": [
                {
                    "bearerAuth": []
                }
            ]
        })
    }

    /// Generate function reference
    pub fn to_function_reference(&self) -> serde_json::Value {
        let by_category: HashMap<String, Vec<&FunctionDoc>> = self.functions.iter()
            .fold(HashMap::new(), |mut acc, f| {
                acc.entry(f.category.clone()).or_insert_with(Vec::new).push(f);
                acc
            });

        serde_json::json!({
            "title": "Function Reference",
            "categories": by_category.iter().map(|(cat, funcs)| {
                serde_json::json!({
                    "name": cat,
                    "functions": funcs.iter().map(|f| {
                        serde_json::json!({
                            "name": f.name,
                            "description": f.description,
                            "parameters": f.parameters,
                            "return_type": f.return_type,
                            "examples": f.examples,
                        })
                    }).collect::<Vec<_>>(),
                })
            }).collect::<Vec<_>>(),
        })
    }

    /// Generate Markdown documentation
    pub fn to_markdown(&self) -> String {
        let mut md = String::new();

        md.push_str(&format!("# {}\n\n", self.info.title));
        md.push_str(&format!("{}\n\n", self.info.description));
        md.push_str(&format!("**Version:** {}\n\n", self.info.version));

        md.push_str("## Endpoints\n\n");
        for endpoint in &self.endpoints {
            md.push_str(&format!("### {} {}\n\n", endpoint.method, endpoint.path));
            md.push_str(&format!("{}\n\n", endpoint.description));

            if !endpoint.parameters.is_empty() {
                md.push_str("**Parameters:**\n\n");
                md.push_str("| Name | Type | Required | Description |\n");
                md.push_str("|------|------|----------|-------------|\n");
                for param in &endpoint.parameters {
                    md.push_str(&format!("| {} | {} | {} | {} |\n",
                        param.name, param.param_type,
                        if param.required { "Yes" } else { "No" },
                        param.description
                    ));
                }
                md.push('\n');
            }

            md.push_str("**Responses:**\n\n");
            for response in &endpoint.responses {
                md.push_str(&format!("- **{}**: {}\n", response.status_code, response.description));
            }
            md.push_str("\n---\n\n");
        }

        md.push_str("## Functions\n\n");
        for func in &self.functions {
            md.push_str(&format!("### {}\n\n", func.name));
            md.push_str(&format!("{}\n\n", func.description));
            md.push_str(&format!("**Category:** {}\n\n", func.category));
            md.push_str(&format!("**Returns:** {}\n\n", func.return_type));

            if !func.examples.is_empty() {
                md.push_str("**Examples:**\n\n");
                for example in &func.examples {
                    md.push_str(&format!("_{}_\n```sql\n{}\n```\n\n", example.title, example.sql));
                }
            }
        }

        md
    }
}
