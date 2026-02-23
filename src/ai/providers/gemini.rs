//! Google Gemini API provider.

use crate::ai::provider::{AiError, LlmProvider};
use async_trait::async_trait;
use serde_json::Value;

pub struct GeminiProvider {
    pub api_key: String,
    pub model: String,
    client: reqwest::Client,
}

impl GeminiProvider {
    pub fn new(api_key: &str, model: &str) -> Self {
        Self {
            api_key: api_key.to_string(),
            model: model.to_string(),
            client: reqwest::Client::new(),
        }
    }

    pub fn parse_response(&self, json: &Value) -> Result<String, AiError> {
        json.pointer("/candidates/0/content/parts/0/text")
            .and_then(|v| v.as_str())
            .map(|s| s.trim().to_string())
            .ok_or_else(|| AiError::Parse("Missing candidates[0].content.parts[0].text".to_string()))
    }
}

#[async_trait]
impl LlmProvider for GeminiProvider {
    async fn complete(&self, prompt: &str) -> Result<String, AiError> {
        let url = format!(
            "https://generativelanguage.googleapis.com/v1beta/models/{}:generateContent?key={}",
            self.model, self.api_key
        );

        let body = serde_json::json!({
            "contents": [{"parts": [{"text": prompt}]}]
        });

        let resp = self.client
            .post(&url)
            .json(&body)
            .send()
            .await
            .map_err(|e| AiError::ProviderUnavailable(format!("gemini: {}", e)))?
            .error_for_status()?;

        let json: Value = resp.json().await?;
        self.parse_response(&json)
    }

    fn name(&self) -> &str { "gemini" }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gemini_provider_new() {
        let p = GeminiProvider::new("key-123", "gemini-1.5-flash");
        assert_eq!(p.name(), "gemini");
        assert_eq!(p.model, "gemini-1.5-flash");
        assert_eq!(p.api_key, "key-123");
    }

    #[test]
    fn test_gemini_parse_response() {
        let p = GeminiProvider::new("key-123", "gemini-1.5-flash");
        let raw = serde_json::json!({
            "candidates": [{
                "content": {"parts": [{"text": "SELECT 42"}]}
            }]
        });
        assert_eq!(p.parse_response(&raw).unwrap(), "SELECT 42");
    }
}
