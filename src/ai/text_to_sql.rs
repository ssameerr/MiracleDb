//! Text-to-SQL engine — converts natural language to SQL via any LlmProvider.

use crate::ai::provider::{AiError, LlmProvider};
use std::sync::Arc;

/// Minimal schema info passed to the prompt so the LLM knows available tables.
#[derive(Debug, Clone)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<String>,
}

pub struct TextToSqlEngine {
    provider: Arc<dyn LlmProvider>,
}

impl TextToSqlEngine {
    pub fn new(provider: Arc<dyn LlmProvider>) -> Self {
        Self { provider }
    }

    /// Build the full prompt: system instruction + schema + user query.
    pub fn build_prompt(&self, query: &str, schema: &[TableSchema]) -> String {
        let schema_str = if schema.is_empty() {
            String::new()
        } else {
            let tables: Vec<String> = schema.iter()
                .map(|t| format!("  {}({})", t.name, t.columns.join(", ")))
                .collect();
            format!("Available tables:\n{}\n\n", tables.join("\n"))
        };

        format!(
            "You are a SQL expert. Convert natural language to SQL.\n\
             Return ONLY the SQL statement, no explanation, no markdown.\n\n\
             {}Query: {}\n\nSQL:",
            schema_str, query
        )
    }

    /// Translate a natural language query to SQL.
    pub async fn translate(&self, query: &str, schema: &[TableSchema]) -> Result<String, AiError> {
        let prompt = self.build_prompt(query, schema);
        let raw = self.provider.complete(&prompt).await?;
        // Strip any accidental markdown code fences (use strip_prefix/strip_suffix to remove exactly once)
        let trimmed = raw.trim();
        let inner = if let Some(s) = trimmed.strip_prefix("```sql") {
            s
        } else if let Some(s) = trimmed.strip_prefix("```") {
            s
        } else {
            trimmed
        };
        let sql = inner.strip_suffix("```").unwrap_or(inner).trim().to_string();
        Ok(sql)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ai::provider::{AiError, LlmProvider};
    use async_trait::async_trait;

    struct MockLlm(String);

    #[async_trait]
    impl LlmProvider for MockLlm {
        async fn complete(&self, _p: &str) -> Result<String, AiError> { Ok(self.0.clone()) }
        fn name(&self) -> &str { "mock" }
    }

    #[tokio::test]
    async fn test_text_to_sql_basic() {
        let engine = TextToSqlEngine::new(Arc::new(MockLlm("SELECT * FROM users".to_string())));
        let sql = engine.translate("show all users", &[]).await.unwrap();
        assert_eq!(sql, "SELECT * FROM users");
    }

    #[tokio::test]
    async fn test_text_to_sql_injects_schema() {
        let engine = TextToSqlEngine::new(Arc::new(MockLlm("SELECT 1".to_string())));
        let schema = vec![TableSchema { name: "orders".to_string(), columns: vec!["id".to_string(), "amount".to_string()] }];
        let sql = engine.translate("get all orders", &schema).await.unwrap();
        assert_eq!(sql, "SELECT 1");
    }

    #[test]
    fn test_build_prompt_contains_query() {
        let engine = TextToSqlEngine::new(Arc::new(MockLlm("".to_string())));
        let prompt = engine.build_prompt("find users named Alice", &[]);
        assert!(prompt.contains("find users named Alice"));
        assert!(prompt.contains("SQL"));
        assert!(!prompt.contains("Available tables:"));
    }

    #[test]
    fn test_build_prompt_includes_schema() {
        let engine = TextToSqlEngine::new(Arc::new(MockLlm("".to_string())));
        let schema = vec![TableSchema { name: "products".to_string(), columns: vec!["id".to_string(), "price".to_string()] }];
        let prompt = engine.build_prompt("cheapest product", &schema);
        assert!(prompt.contains("products"));
        assert!(prompt.contains("price"));
    }
}
