//! Template Module - SQL and query templates

use std::collections::HashMap;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};
use regex::Regex;

/// Template parameter
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TemplateParam {
    pub name: String,
    pub param_type: ParamType,
    pub required: bool,
    pub default: Option<String>,
    pub description: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ParamType {
    String,
    Integer,
    Float,
    Boolean,
    Date,
    Array,
    Json,
}

/// SQL template
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Template {
    pub id: String,
    pub name: String,
    pub description: String,
    pub sql: String,
    pub parameters: Vec<TemplateParam>,
    pub created_at: i64,
    pub updated_at: i64,
    pub version: u32,
    pub tags: Vec<String>,
}

impl Template {
    pub fn new(id: &str, name: &str, sql: &str) -> Self {
        let now = chrono::Utc::now().timestamp();
        Self {
            id: id.to_string(),
            name: name.to_string(),
            description: String::new(),
            sql: sql.to_string(),
            parameters: Self::extract_parameters(sql),
            created_at: now,
            updated_at: now,
            version: 1,
            tags: vec![],
        }
    }

    /// Extract parameters from SQL template
    fn extract_parameters(sql: &str) -> Vec<TemplateParam> {
        let re = Regex::new(r"\{\{(\w+)\}\}").unwrap();
        let mut params = Vec::new();
        let mut seen = std::collections::HashSet::new();

        for cap in re.captures_iter(sql) {
            let name = cap[1].to_string();
            if seen.insert(name.clone()) {
                params.push(TemplateParam {
                    name,
                    param_type: ParamType::String,
                    required: true,
                    default: None,
                    description: String::new(),
                });
            }
        }

        params
    }

    /// Render template with parameters
    pub fn render(&self, params: &HashMap<String, serde_json::Value>) -> Result<String, String> {
        let mut sql = self.sql.clone();

        // Check required parameters
        for param in &self.parameters {
            if param.required && !params.contains_key(&param.name) && param.default.is_none() {
                return Err(format!("Missing required parameter: {}", param.name));
            }
        }

        // Replace parameters
        for param in &self.parameters {
            let value = params.get(&param.name)
                .map(|v| self.format_value(v, &param.param_type))
                .or_else(|| param.default.clone())
                .unwrap_or("NULL".to_string());

            sql = sql.replace(&format!("{{{{{}}}}}", param.name), &value);
        }

        Ok(sql)
    }

    fn format_value(&self, value: &serde_json::Value, param_type: &ParamType) -> String {
        match param_type {
            ParamType::String => {
                if let Some(s) = value.as_str() {
                    format!("'{}'", s.replace('\'', "''"))
                } else {
                    value.to_string()
                }
            }
            ParamType::Integer | ParamType::Float => {
                value.to_string()
            }
            ParamType::Boolean => {
                if value.as_bool().unwrap_or(false) {
                    "TRUE".to_string()
                } else {
                    "FALSE".to_string()
                }
            }
            ParamType::Date => {
                if let Some(s) = value.as_str() {
                    format!("DATE '{}'", s)
                } else {
                    value.to_string()
                }
            }
            ParamType::Array => {
                if let Some(arr) = value.as_array() {
                    let vals: Vec<String> = arr.iter()
                        .map(|v| self.format_value(v, &ParamType::String))
                        .collect();
                    format!("({})", vals.join(", "))
                } else {
                    value.to_string()
                }
            }
            ParamType::Json => {
                format!("'{}'", value.to_string().replace('\'', "''"))
            }
        }
    }
}

/// Template manager
pub struct TemplateManager {
    templates: RwLock<HashMap<String, Template>>,
}

impl TemplateManager {
    pub fn new() -> Self {
        Self {
            templates: RwLock::new(HashMap::new()),
        }
    }

    /// Register a template
    pub async fn register(&self, template: Template) {
        let mut templates = self.templates.write().await;
        templates.insert(template.id.clone(), template);
    }

    /// Get template by ID
    pub async fn get(&self, id: &str) -> Option<Template> {
        let templates = self.templates.read().await;
        templates.get(id).cloned()
    }

    /// Execute template with parameters
    pub async fn execute(&self, id: &str, params: &HashMap<String, serde_json::Value>) -> Result<String, String> {
        let template = self.get(id).await
            .ok_or("Template not found")?;
        template.render(params)
    }

    /// List all templates
    pub async fn list(&self) -> Vec<Template> {
        let templates = self.templates.read().await;
        templates.values().cloned().collect()
    }

    /// Search templates by tag
    pub async fn search_by_tag(&self, tag: &str) -> Vec<Template> {
        let templates = self.templates.read().await;
        templates.values()
            .filter(|t| t.tags.contains(&tag.to_string()))
            .cloned()
            .collect()
    }

    /// Delete template
    pub async fn delete(&self, id: &str) -> bool {
        let mut templates = self.templates.write().await;
        templates.remove(id).is_some()
    }

    /// Update template
    pub async fn update(&self, id: &str, sql: &str) -> Result<(), String> {
        let mut templates = self.templates.write().await;
        let template = templates.get_mut(id)
            .ok_or("Template not found")?;

        template.sql = sql.to_string();
        template.parameters = Template::extract_parameters(sql);
        template.updated_at = chrono::Utc::now().timestamp();
        template.version += 1;

        Ok(())
    }

    /// Create built-in templates
    pub async fn load_builtins(&self) {
        let builtins = vec![
            Template::new(
                "select_all",
                "Select All",
                "SELECT * FROM {{table}} LIMIT {{limit}}"
            ),
            Template::new(
                "count_rows",
                "Count Rows",
                "SELECT COUNT(*) as count FROM {{table}}"
            ),
            Template::new(
                "find_by_id",
                "Find By ID",
                "SELECT * FROM {{table}} WHERE id = {{id}}"
            ),
            Template::new(
                "delete_by_id",
                "Delete By ID",
                "DELETE FROM {{table}} WHERE id = {{id}}"
            ),
            Template::new(
                "recent_rows",
                "Recent Rows",
                "SELECT * FROM {{table}} ORDER BY {{order_column}} DESC LIMIT {{limit}}"
            ),
        ];

        for template in builtins {
            self.register(template).await;
        }
    }
}

impl Default for TemplateManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_template_render() {
        let template = Template::new("test", "Test", "SELECT * FROM {{table}} WHERE id = {{id}}");
        
        let mut params = HashMap::new();
        params.insert("table".to_string(), serde_json::json!("users"));
        params.insert("id".to_string(), serde_json::json!(123));

        let result = template.render(&params).unwrap();
        assert!(result.contains("users"));
        assert!(result.contains("123"));
    }
}
