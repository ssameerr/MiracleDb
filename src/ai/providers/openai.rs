//! OpenAI and Azure OpenAI provider (same API, configurable base URL).

use crate::ai::provider::{AiError, EmbeddingProvider, LlmProvider};
use async_trait::async_trait;
use serde_json::Value;

pub struct OpenAiProvider {
    pub api_key: String,
    pub model: String,
    pub base_url: String,
    pub embedding_dims: usize,
    client: reqwest::Client,
}

impl OpenAiProvider {
    pub fn new(api_key: &str, model: &str, base_url: &str) -> Self {
        Self {
            api_key: api_key.to_string(),
            model: model.to_string(),
            base_url: base_url.trim_end_matches('/').to_string(),
            embedding_dims: 1536, // text-embedding-3-small default
            client: reqwest::Client::new(),
        }
    }

    pub fn parse_response(&self, json: &Value) -> Result<String, AiError> {
        json.pointer("/choices/0/message/content")
            .and_then(|v| v.as_str())
            .map(|s| s.trim().to_string())
            .ok_or_else(|| AiError::Parse("Missing choices[0].message.content".to_string()))
    }
}

#[async_trait]
impl LlmProvider for OpenAiProvider {
    async fn complete(&self, prompt: &str) -> Result<String, AiError> {
        let body = serde_json::json!({
            "model": self.model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.0
        });

        let resp = self.client
            .post(format!("{}/chat/completions", self.base_url))
            .bearer_auth(&self.api_key)
            .json(&body)
            .send()
            .await
            .map_err(|e| AiError::ProviderUnavailable(format!("openai: {}", e)))?
            .error_for_status()?;

        let json: Value = resp.json().await?;
        self.parse_response(&json)
    }

    fn name(&self) -> &str { "openai" }
}

#[async_trait]
impl EmbeddingProvider for OpenAiProvider {
    async fn embed(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, AiError> {
        let body = serde_json::json!({
            "model": self.model,
            "input": texts
        });

        let resp = self.client
            .post(format!("{}/embeddings", self.base_url))
            .bearer_auth(&self.api_key)
            .json(&body)
            .send()
            .await
            .map_err(|e| AiError::ProviderUnavailable(format!("openai embeddings: {}", e)))?
            .error_for_status()?;

        let json: Value = resp.json().await?;
        let data = json.get("data")
            .and_then(|v| v.as_array())
            .ok_or_else(|| AiError::Parse("Missing data array".to_string()))?;

        data.iter().map(|item| {
            item.get("embedding")
                .and_then(|v| v.as_array())
                .ok_or_else(|| AiError::Parse("Missing embedding".to_string()))
                .map(|arr| arr.iter().filter_map(|v| v.as_f64().map(|f| f as f32)).collect())
        }).collect()
    }

    fn dimensions(&self) -> usize { self.embedding_dims }

    fn name(&self) -> &str { "openai" }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ai::provider::LlmProvider;

    #[test]
    fn test_openai_provider_new() {
        let p = OpenAiProvider::new("sk-test", "gpt-4o-mini", "https://api.openai.com/v1");
        assert_eq!(LlmProvider::name(&p), "openai");
        assert_eq!(p.model, "gpt-4o-mini");
        assert_eq!(p.base_url, "https://api.openai.com/v1");
    }

    #[test]
    fn test_azure_openai_provider_new() {
        let p = OpenAiProvider::new(
            "azure-key",
            "gpt-4o",
            "https://myorg.openai.azure.com/openai/deployments/gpt-4o",
        );
        assert!(p.base_url.contains("azure"));
    }

    #[test]
    fn test_openai_parse_response() {
        let p = OpenAiProvider::new("sk-test", "gpt-4o-mini", "https://api.openai.com/v1");
        let raw = serde_json::json!({
            "choices": [{"message": {"content": "SELECT COUNT(*) FROM products"}}]
        });
        assert_eq!(p.parse_response(&raw).unwrap(), "SELECT COUNT(*) FROM products");
    }
}
