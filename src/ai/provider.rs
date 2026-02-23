//! Core AI provider traits and error type.

use async_trait::async_trait;

/// Errors from AI provider calls.
#[derive(Debug, thiserror::Error)]
pub enum AiError {
    #[error("Provider '{0}' unavailable")]
    ProviderUnavailable(String),
    #[error("HTTP error: {0}")]
    Http(String),
    #[error("Parse error: {0}")]
    Parse(String),
    #[error("Configuration error: {0}")]
    Config(String),
}

impl From<reqwest::Error> for AiError {
    fn from(e: reqwest::Error) -> Self {
        AiError::Http(e.to_string())
    }
}

/// A provider that generates text completions (used for Text-to-SQL).
#[async_trait]
pub trait LlmProvider: Send + Sync {
    /// Generate a completion for the given prompt.
    async fn complete(&self, prompt: &str) -> Result<String, AiError>;
    /// Human-readable provider name (e.g. "ollama", "claude").
    fn name(&self) -> &str;
}

/// A provider that converts text into float vectors (embeddings).
#[async_trait]
pub trait EmbeddingProvider: Send + Sync {
    /// Embed one or more texts. Returns one vector per input text.
    async fn embed(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, AiError>;
    /// Dimensionality of output vectors (e.g. 384 for all-MiniLM-L6-v2).
    fn dimensions(&self) -> usize;
    /// Human-readable provider name.
    fn name(&self) -> &str;
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockLlm { response: String }
    struct MockEmbedder { dims: usize }

    #[async_trait::async_trait]
    impl LlmProvider for MockLlm {
        async fn complete(&self, _prompt: &str) -> Result<String, AiError> {
            Ok(self.response.clone())
        }
        fn name(&self) -> &str { "mock" }
    }

    #[async_trait::async_trait]
    impl EmbeddingProvider for MockEmbedder {
        async fn embed(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, AiError> {
            Ok(texts.iter().map(|_| vec![0.1f32; self.dims]).collect())
        }
        fn dimensions(&self) -> usize { self.dims }
        fn name(&self) -> &str { "mock_embed" }
    }

    #[tokio::test]
    async fn test_llm_provider_trait() {
        let llm = MockLlm { response: "SELECT * FROM users".to_string() };
        let result = llm.complete("show all users").await.unwrap();
        assert_eq!(result, "SELECT * FROM users");
        assert_eq!(llm.name(), "mock");
    }

    #[tokio::test]
    async fn test_embedding_provider_trait() {
        let embedder = MockEmbedder { dims: 384 };
        let vecs = embedder.embed(&["hello", "world"]).await.unwrap();
        assert_eq!(vecs.len(), 2);
        assert_eq!(vecs[0].len(), 384);
        assert_eq!(embedder.dimensions(), 384);
    }

    #[test]
    fn test_ai_error_display() {
        let e = AiError::ProviderUnavailable("ollama".to_string());
        assert!(e.to_string().contains("ollama"));
    }
}
