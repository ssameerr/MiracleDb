//! AI layer — LLM providers, embeddings, Text-to-SQL, hybrid search.

pub mod provider;
pub mod providers;
pub mod text_to_sql;
pub mod embeddings;
pub mod hybrid_search;

pub use provider::{AiError, EmbeddingProvider, LlmProvider};

use std::sync::Arc;
use providers::candle::CandleEmbeddingProvider;

/// Top-level AI configuration (maps to [llm] section in miracledb.toml).
#[derive(Debug, Clone)]
pub struct AiConfig {
    pub default_llm_provider: String,
    pub fallback_llm_provider: Option<String>,
    pub default_embedding_provider: String,
    pub ollama_base_url: String,
    pub ollama_model: String,
    pub claude_api_key: Option<String>,
    pub openai_api_key: Option<String>,
    pub openai_base_url: String,
    pub gemini_api_key: Option<String>,
    pub gemini_model: String,
    pub vllm_base_url: Option<String>,
    pub vllm_model: Option<String>,
}

impl Default for AiConfig {
    fn default() -> Self {
        Self {
            default_llm_provider: "ollama".to_string(),
            fallback_llm_provider: Some("claude".to_string()),
            default_embedding_provider: "candle".to_string(),
            ollama_base_url: "http://localhost:11434".to_string(),
            ollama_model: "llama3.2:3b".to_string(),
            claude_api_key: std::env::var("ANTHROPIC_API_KEY").ok(),
            openai_api_key: std::env::var("OPENAI_API_KEY").ok(),
            openai_base_url: "https://api.openai.com/v1".to_string(),
            gemini_api_key: std::env::var("GEMINI_API_KEY").ok(),
            gemini_model: "gemini-1.5-flash".to_string(),
            vllm_base_url: std::env::var("VLLM_BASE_URL").ok(),
            vllm_model: std::env::var("VLLM_MODEL").ok(),
        }
    }
}

impl AiConfig {
    /// Resolve `${ENV_VAR}` placeholders in config strings.
    pub fn resolve_env(&self, value: &str) -> String {
        if value.starts_with("${") && value.ends_with('}') {
            let var_name = &value[2..value.len() - 1];
            std::env::var(var_name).unwrap_or_default()
        } else {
            value.to_string()
        }
    }
}

/// Selects the active LLM and embedding provider based on config and session overrides.
pub struct ProviderRegistry {
    pub config: AiConfig,
    pub session_llm_override: Option<String>,
    pub session_embedding_override: Option<String>,
}

impl ProviderRegistry {
    pub fn new(config: AiConfig) -> Self {
        Self { config, session_llm_override: None, session_embedding_override: None }
    }

    /// Override the LLM provider for the current session (SET llm.provider = '...').
    pub fn set_session_llm(&mut self, provider: &str) {
        self.session_llm_override = Some(provider.to_string());
    }

    /// Override the embedding provider for the current session.
    pub fn set_session_embedder(&mut self, provider: &str) {
        self.session_embedding_override = Some(provider.to_string());
    }

    /// Return the active embedding provider.
    pub fn embedder(&self) -> Arc<dyn EmbeddingProvider> {
        let name = self.session_embedding_override
            .as_deref()
            .unwrap_or(&self.config.default_embedding_provider);

        match name {
            "candle" | _ => Arc::new(CandleEmbeddingProvider::new()),
        }
    }

    /// Active LLM provider name (resolved).
    pub fn active_llm_name(&self) -> &str {
        self.session_llm_override
            .as_deref()
            .unwrap_or(&self.config.default_llm_provider)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ai_config_defaults() {
        let config = AiConfig::default();
        assert_eq!(config.default_llm_provider, "ollama");
        assert_eq!(config.default_embedding_provider, "candle");
    }

    #[test]
    fn test_provider_registry_candle_default() {
        let config = AiConfig::default();
        let registry = ProviderRegistry::new(config);
        let embedder = registry.embedder();
        assert_eq!(embedder.name(), "candle");
        assert_eq!(embedder.dimensions(), 384);
    }

    #[test]
    fn test_provider_registry_session_override() {
        let config = AiConfig::default();
        let mut registry = ProviderRegistry::new(config);
        registry.set_session_llm("ollama");
        assert_eq!(registry.session_llm_override, Some("ollama".to_string()));
    }

    #[test]
    fn test_ai_config_from_env() {
        std::env::set_var("ANTHROPIC_API_KEY", "test-key");
        let config = AiConfig::default();
        assert_eq!(config.resolve_env("${ANTHROPIC_API_KEY}"), "test-key");
        std::env::remove_var("ANTHROPIC_API_KEY");
    }
}
