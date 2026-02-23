//! vLLM provider — uses OpenAI-compatible API. base_url should include /v1 (e.g. http://host:8000/v1).

use crate::ai::provider::{AiError, LlmProvider};
use async_trait::async_trait;
use serde_json::Value;

pub struct VllmProvider {
    pub base_url: String,
    pub model: String,
    client: reqwest::Client,
}

impl VllmProvider {
    pub fn new(base_url: &str, model: &str) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            model: model.to_string(),
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
impl LlmProvider for VllmProvider {
    async fn complete(&self, prompt: &str) -> Result<String, AiError> {
        let body = serde_json::json!({
            "model": self.model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.0
        });

        let resp = self.client
            .post(format!("{}/chat/completions", self.base_url))
            .json(&body)
            .send()
            .await
            .map_err(|e| AiError::ProviderUnavailable(format!("vllm: {}", e)))?
            .error_for_status()?;

        let json: Value = resp.json().await?;
        self.parse_response(&json)
    }

    fn name(&self) -> &str { "vllm" }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vllm_provider_new() {
        let p = VllmProvider::new("http://localhost:8000/v1", "defog/sqlcoder-7b-2");
        assert_eq!(p.name(), "vllm");
        assert_eq!(p.model, "defog/sqlcoder-7b-2");
        // Test slash trimming
        let p_slash = VllmProvider::new("http://localhost:8000/v1/", "model");
        assert_eq!(p_slash.base_url, "http://localhost:8000/v1");
    }

    #[test]
    fn test_vllm_parse_response() {
        let p = VllmProvider::new("http://localhost:8000", "sqlcoder");
        let raw = serde_json::json!({
            "choices": [{"message": {"content": "SELECT 1"}}]
        });
        assert_eq!(p.parse_response(&raw).unwrap(), "SELECT 1");
    }
}
