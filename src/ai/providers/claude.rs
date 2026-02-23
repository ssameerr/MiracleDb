//! Anthropic Claude API provider.

use crate::ai::provider::{AiError, LlmProvider};
use async_trait::async_trait;
use serde_json::Value;

pub struct ClaudeProvider {
    pub api_key: String,
    pub model: String,
    client: reqwest::Client,
}

impl ClaudeProvider {
    pub fn new(api_key: &str, model: &str) -> Self {
        Self {
            api_key: api_key.to_string(),
            model: model.to_string(),
            client: reqwest::Client::new(),
        }
    }

    pub fn parse_response(&self, json: &Value) -> Result<String, AiError> {
        json.pointer("/content/0/text")
            .and_then(|v| v.as_str())
            .map(|s| s.trim().to_string())
            .ok_or_else(|| AiError::Parse("Missing content[0].text".to_string()))
    }
}

#[async_trait]
impl LlmProvider for ClaudeProvider {
    async fn complete(&self, prompt: &str) -> Result<String, AiError> {
        let body = serde_json::json!({
            "model": self.model,
            "max_tokens": 1024,
            "messages": [{"role": "user", "content": prompt}]
        });

        let resp = self.client
            .post("https://api.anthropic.com/v1/messages")
            .header("x-api-key", &self.api_key)
            .header("anthropic-version", "2023-06-01")
            .header("content-type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| AiError::ProviderUnavailable(format!("claude: {}", e)))?
            .error_for_status()?;

        let json: Value = resp.json().await?;
        self.parse_response(&json)
    }

    fn name(&self) -> &str { "claude" }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_claude_provider_new() {
        let p = ClaudeProvider::new("sk-test", "claude-haiku-4-5-20251001");
        assert_eq!(p.name(), "claude");
        assert_eq!(p.model, "claude-haiku-4-5-20251001");
        assert_eq!(p.api_key, "sk-test");
    }

    #[test]
    fn test_claude_parse_response() {
        let p = ClaudeProvider::new("sk-test", "claude-haiku-4-5-20251001");
        let raw = serde_json::json!({
            "content": [{"type": "text", "text": "SELECT * FROM orders"}]
        });
        assert_eq!(p.parse_response(&raw).unwrap(), "SELECT * FROM orders");
    }
}
