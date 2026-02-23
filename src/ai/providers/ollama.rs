//! Ollama provider — local LLM via Ollama HTTP API.

use crate::ai::provider::{AiError, EmbeddingProvider, LlmProvider};
use async_trait::async_trait;
use serde_json::Value;

pub struct OllamaProvider {
    pub base_url: String,
    pub model: String,
    client: reqwest::Client,
}

impl OllamaProvider {
    pub fn new(base_url: &str, model: &str) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            model: model.to_string(),
            client: reqwest::Client::new(),
        }
    }

    pub fn build_prompt(&self, user_query: &str) -> String {
        format!(
            "You are a SQL expert. Convert the following natural language query to SQL.\n\
             Return ONLY the SQL statement, no explanation.\n\n\
             Query: {}\n\nSQL:",
            user_query
        )
    }

    pub fn parse_completion_response(&self, json: &Value) -> Result<String, AiError> {
        json.get("response")
            .and_then(|v| v.as_str())
            .map(|s| s.trim().to_string())
            .ok_or_else(|| AiError::Parse("Missing 'response' field".to_string()))
    }
}

#[async_trait]
impl LlmProvider for OllamaProvider {
    async fn complete(&self, prompt: &str) -> Result<String, AiError> {
        let body = serde_json::json!({
            "model": self.model,
            "prompt": prompt,
            "stream": false
        });

        let resp = self.client
            .post(format!("{}/api/generate", self.base_url))
            .json(&body)
            .send()
            .await
            .map_err(|e| AiError::ProviderUnavailable(format!("ollama: {}", e)))?;

        let json: Value = resp.json().await?;
        self.parse_completion_response(&json)
    }

    fn name(&self) -> &str { "ollama" }
}

#[async_trait]
impl EmbeddingProvider for OllamaProvider {
    async fn embed(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, AiError> {
        let mut results = Vec::new();
        for text in texts {
            let body = serde_json::json!({
                "model": self.model,
                "prompt": text
            });
            let resp = self.client
                .post(format!("{}/api/embeddings", self.base_url))
                .json(&body)
                .send()
                .await
                .map_err(|e| AiError::ProviderUnavailable(format!("ollama embeddings: {}", e)))?;

            let json: Value = resp.json().await?;
            let vec = json.get("embedding")
                .and_then(|v| v.as_array())
                .ok_or_else(|| AiError::Parse("Missing 'embedding' field".to_string()))?
                .iter()
                .filter_map(|v| v.as_f64().map(|f| f as f32))
                .collect();
            results.push(vec);
        }
        Ok(results)
    }

    fn dimensions(&self) -> usize { 768 } // nomic-embed-text default

    fn name(&self) -> &str { "ollama" }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ollama_provider_new() {
        let p = OllamaProvider::new("http://localhost:11434", "llama3.2:3b");
        assert_eq!(LlmProvider::name(&p), "ollama");
        assert_eq!(p.model, "llama3.2:3b");
    }

    #[test]
    fn test_ollama_build_prompt() {
        let p = OllamaProvider::new("http://localhost:11434", "llama3.2:3b");
        let prompt = p.build_prompt("show all users");
        assert!(prompt.contains("show all users"));
    }

    #[test]
    fn test_ollama_parse_response() {
        let p = OllamaProvider::new("http://localhost:11434", "llama3.2:3b");
        let raw = serde_json::json!({"response": "SELECT * FROM users"});
        let sql = p.parse_completion_response(&raw).unwrap();
        assert_eq!(sql, "SELECT * FROM users");
    }
}
