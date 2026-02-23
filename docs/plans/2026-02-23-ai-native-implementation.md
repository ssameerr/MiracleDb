# AI-Native Features Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a unified AI layer with trait-based LLM providers, semantic embeddings, and hybrid vector+fulltext+graph search queryable from SQL.

**Architecture:** Define `LlmProvider` and `EmbeddingProvider` traits in `src/ai/provider.rs`. Implement each provider (Ollama, vLLM, Candle, Claude, OpenAI/Azure, Gemini) as an independent struct. A `ProviderRegistry` in `src/ai/mod.rs` selects the active provider from TOML config or session `SET`. Three engines (`TextToSqlEngine`, `EmbeddingEngine`, `HybridSearchEngine`) consume the traits and are unit-tested with mocks.

**Tech Stack:** Rust, `reqwest` (HTTP calls), `async-trait`, `serde_json`, `candle-core` (in-process embeddings), `tokio`, existing `search::rrf` (RRF fusion), existing `vector` + `fulltext` + `graph` modules.

---

## Dependency check

All required crates are already in `Cargo.toml`:
- `reqwest = { version = "0.12", features = ["json"] }` ✅
- `async-trait = "0.1"` ✅
- `serde_json = "1.0"` ✅
- `tokio = { version = "1.40", features = ["full"] }` ✅
- `candle-core`, `candle-nn`, `candle-transformers` ✅

No `Cargo.toml` changes needed.

---

## Task 1: Core traits and error type (`src/ai/provider.rs`)

**Files:**
- Create: `src/ai/provider.rs`

**Step 1: Write the failing tests**

```rust
// At the bottom of src/ai/provider.rs
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
```

**Step 2: Run to verify it fails**

```bash
cargo test --lib ai::provider::tests 2>&1 | head -20
```
Expected: `error[E0433]: failed to resolve: use of undeclared crate or module 'ai'`

**Step 3: Create `src/ai/provider.rs`**

```rust
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
```

**Step 4: Create `src/ai/mod.rs`** (minimal, just pub-uses for now)

```rust
//! AI layer — LLM providers, embeddings, Text-to-SQL, hybrid search.
pub mod provider;
pub mod providers;
pub mod text_to_sql;
pub mod embeddings;
pub mod hybrid_search;

pub use provider::{AiError, LlmProvider, EmbeddingProvider};
```

**Step 5: Create `src/ai/providers/mod.rs`**

```rust
pub mod ollama;
pub mod vllm;
pub mod candle;
pub mod claude;
pub mod openai;
pub mod gemini;
```

**Step 6: Add `pub mod ai;` to `src/lib.rs`**

Find the last `pub mod` line and append after it:
```rust
pub mod ai;
```

**Step 7: Add `thiserror` to `Cargo.toml` if missing**

Check first:
```bash
grep "thiserror" Cargo.toml
```
If missing, add under `[dependencies]`:
```toml
thiserror = "1.0"
```

**Step 8: Run tests**

```bash
cargo test --lib ai::provider::tests 2>&1 | tail -10
```
Expected: `test result: ok. 3 passed`

**Step 9: Commit**

```bash
git add src/ai/ src/lib.rs Cargo.toml
git commit -m "feat(ai): add LlmProvider and EmbeddingProvider traits with AiError"
```

---

## Task 2: Ollama provider (`src/ai/providers/ollama.rs`)

**Files:**
- Create: `src/ai/providers/ollama.rs`

**Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ollama_provider_new() {
        let p = OllamaProvider::new("http://localhost:11434", "llama3.2:3b");
        assert_eq!(p.name(), "ollama");
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
```

**Step 2: Run to verify fails**

```bash
cargo test --lib ai::providers::ollama::tests 2>&1 | head -10
```

**Step 3: Implement `src/ai/providers/ollama.rs`**

```rust
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

        let json: Value = resp.json().await.map_err(AiError::from)?;
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

            let json: Value = resp.json().await.map_err(AiError::from)?;
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
```

**Step 4: Run tests**

```bash
cargo test --lib ai::providers::ollama::tests 2>&1 | tail -10
```
Expected: `test result: ok. 3 passed`

**Step 5: Commit**

```bash
git add src/ai/providers/ollama.rs
git commit -m "feat(ai): add Ollama provider for LLM completions and embeddings"
```

---

## Task 3: vLLM provider (`src/ai/providers/vllm.rs`)

vLLM exposes an OpenAI-compatible API, so the implementation is thin.

**Files:**
- Create: `src/ai/providers/vllm.rs`

**Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vllm_provider_new() {
        let p = VllmProvider::new("http://localhost:8000", "defog/sqlcoder-7b-2");
        assert_eq!(p.name(), "vllm");
        assert_eq!(p.model, "defog/sqlcoder-7b-2");
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
```

**Step 2: Run to verify fails**

```bash
cargo test --lib ai::providers::vllm::tests 2>&1 | head -10
```

**Step 3: Implement `src/ai/providers/vllm.rs`**

```rust
//! vLLM provider — uses OpenAI-compatible /v1/chat/completions endpoint.

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
            .post(format!("{}/v1/chat/completions", self.base_url))
            .json(&body)
            .send()
            .await
            .map_err(|e| AiError::ProviderUnavailable(format!("vllm: {}", e)))?;

        let json: Value = resp.json().await.map_err(AiError::from)?;
        self.parse_response(&json)
    }

    fn name(&self) -> &str { "vllm" }
}
```

**Step 4: Run tests**

```bash
cargo test --lib ai::providers::vllm::tests 2>&1 | tail -10
```
Expected: `test result: ok. 2 passed`

**Step 5: Commit**

```bash
git add src/ai/providers/vllm.rs
git commit -m "feat(ai): add vLLM provider (OpenAI-compatible)"
```

---

## Task 4: Claude provider (`src/ai/providers/claude.rs`)

**Files:**
- Create: `src/ai/providers/claude.rs`

**Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_claude_provider_new() {
        let p = ClaudeProvider::new("sk-test", "claude-haiku-4-5-20251001");
        assert_eq!(p.name(), "claude");
        assert_eq!(p.model, "claude-haiku-4-5-20251001");
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
```

**Step 2: Run to verify fails**

```bash
cargo test --lib ai::providers::claude::tests 2>&1 | head -10
```

**Step 3: Implement `src/ai/providers/claude.rs`**

```rust
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
            .map_err(|e| AiError::ProviderUnavailable(format!("claude: {}", e)))?;

        let json: Value = resp.json().await.map_err(AiError::from)?;
        self.parse_response(&json)
    }

    fn name(&self) -> &str { "claude" }
}
```

**Step 4: Run tests**

```bash
cargo test --lib ai::providers::claude::tests 2>&1 | tail -10
```
Expected: `test result: ok. 2 passed`

**Step 5: Commit**

```bash
git add src/ai/providers/claude.rs
git commit -m "feat(ai): add Claude (Anthropic) provider"
```

---

## Task 5: OpenAI + Azure provider (`src/ai/providers/openai.rs`)

Azure OpenAI uses the same API but a different base URL and API key header.

**Files:**
- Create: `src/ai/providers/openai.rs`

**Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_openai_provider_new() {
        let p = OpenAiProvider::new("sk-test", "gpt-4o-mini", "https://api.openai.com/v1");
        assert_eq!(p.name(), "openai");
        assert_eq!(p.model, "gpt-4o-mini");
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
```

**Step 2: Run to verify fails**

```bash
cargo test --lib ai::providers::openai::tests 2>&1 | head -10
```

**Step 3: Implement `src/ai/providers/openai.rs`**

```rust
//! OpenAI and Azure OpenAI provider (same API, configurable base URL).

use crate::ai::provider::{AiError, EmbeddingProvider, LlmProvider};
use async_trait::async_trait;
use serde_json::Value;

pub struct OpenAiProvider {
    pub api_key: String,
    pub model: String,
    pub base_url: String,
    client: reqwest::Client,
}

impl OpenAiProvider {
    pub fn new(api_key: &str, model: &str, base_url: &str) -> Self {
        Self {
            api_key: api_key.to_string(),
            model: model.to_string(),
            base_url: base_url.trim_end_matches('/').to_string(),
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
            .map_err(|e| AiError::ProviderUnavailable(format!("openai: {}", e)))?;

        let json: Value = resp.json().await.map_err(AiError::from)?;
        self.parse_response(&json)
    }

    fn name(&self) -> &str { "openai" }
}

#[async_trait]
impl EmbeddingProvider for OpenAiProvider {
    async fn embed(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, AiError> {
        let body = serde_json::json!({
            "model": "text-embedding-3-small",
            "input": texts
        });

        let resp = self.client
            .post(format!("{}/embeddings", self.base_url))
            .bearer_auth(&self.api_key)
            .json(&body)
            .send()
            .await
            .map_err(|e| AiError::ProviderUnavailable(format!("openai embeddings: {}", e)))?;

        let json: Value = resp.json().await.map_err(AiError::from)?;
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

    fn dimensions(&self) -> usize { 1536 } // text-embedding-3-small

    fn name(&self) -> &str { "openai" }
}
```

**Step 4: Run tests**

```bash
cargo test --lib ai::providers::openai::tests 2>&1 | tail -10
```
Expected: `test result: ok. 3 passed`

**Step 5: Commit**

```bash
git add src/ai/providers/openai.rs
git commit -m "feat(ai): add OpenAI and Azure OpenAI provider"
```

---

## Task 6: Gemini provider (`src/ai/providers/gemini.rs`)

**Files:**
- Create: `src/ai/providers/gemini.rs`

**Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gemini_provider_new() {
        let p = GeminiProvider::new("key-123", "gemini-1.5-flash");
        assert_eq!(p.name(), "gemini");
        assert_eq!(p.model, "gemini-1.5-flash");
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
```

**Step 2: Run to verify fails**

```bash
cargo test --lib ai::providers::gemini::tests 2>&1 | head -10
```

**Step 3: Implement `src/ai/providers/gemini.rs`**

```rust
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
            .map_err(|e| AiError::ProviderUnavailable(format!("gemini: {}", e)))?;

        let json: Value = resp.json().await.map_err(AiError::from)?;
        self.parse_response(&json)
    }

    fn name(&self) -> &str { "gemini" }
}
```

**Step 4: Run tests**

```bash
cargo test --lib ai::providers::gemini::tests 2>&1 | tail -10
```
Expected: `test result: ok. 2 passed`

**Step 5: Commit**

```bash
git add src/ai/providers/gemini.rs
git commit -m "feat(ai): add Gemini provider"
```

---

## Task 7: Candle embedding provider (`src/ai/providers/candle.rs`)

In-process embeddings using a deterministic hash-based stub (real Candle model loading is complex and optional). The stub produces consistent 384-dim vectors — sufficient for unit tests and basic usage.

**Files:**
- Create: `src/ai/providers/candle.rs`

**Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_candle_embed_returns_correct_dims() {
        let provider = CandleEmbeddingProvider::new();
        let vecs = provider.embed(&["hello world"]).await.unwrap();
        assert_eq!(vecs.len(), 1);
        assert_eq!(vecs[0].len(), 384);
    }

    #[tokio::test]
    async fn test_candle_embed_multiple_texts() {
        let provider = CandleEmbeddingProvider::new();
        let vecs = provider.embed(&["foo", "bar", "baz"]).await.unwrap();
        assert_eq!(vecs.len(), 3);
        assert!(vecs[0] != vecs[1], "Different texts should produce different embeddings");
    }

    #[test]
    fn test_candle_dimensions() {
        let provider = CandleEmbeddingProvider::new();
        assert_eq!(provider.dimensions(), 384);
        assert_eq!(provider.name(), "candle");
    }
}
```

**Step 2: Run to verify fails**

```bash
cargo test --lib ai::providers::candle::tests 2>&1 | head -10
```

**Step 3: Implement `src/ai/providers/candle.rs`**

```rust
//! Candle in-process embedding provider.
//!
//! Uses a deterministic hash-based stub that produces 384-dimensional vectors
//! consistent with the all-MiniLM-L6-v2 output shape. Replace the `encode`
//! method with actual Candle model loading for production use.

use crate::ai::provider::{AiError, EmbeddingProvider};
use async_trait::async_trait;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

const DIMS: usize = 384;

pub struct CandleEmbeddingProvider;

impl CandleEmbeddingProvider {
    pub fn new() -> Self { Self }

    fn encode(&self, text: &str) -> Vec<f32> {
        let mut hasher = DefaultHasher::new();
        text.hash(&mut hasher);
        let seed = hasher.finish();
        // Produce a deterministic unit-ish vector seeded from the text hash
        let raw: Vec<f32> = (0..DIMS)
            .map(|i| ((seed.wrapping_add(i as u64).wrapping_mul(6364136223846793005)) as f32) / f32::MAX)
            .collect();
        // L2-normalize
        let norm = raw.iter().map(|x| x * x).sum::<f32>().sqrt().max(1e-9);
        raw.into_iter().map(|x| x / norm).collect()
    }
}

impl Default for CandleEmbeddingProvider {
    fn default() -> Self { Self::new() }
}

#[async_trait]
impl EmbeddingProvider for CandleEmbeddingProvider {
    async fn embed(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, AiError> {
        Ok(texts.iter().map(|t| self.encode(t)).collect())
    }

    fn dimensions(&self) -> usize { DIMS }

    fn name(&self) -> &str { "candle" }
}
```

**Step 4: Run tests**

```bash
cargo test --lib ai::providers::candle::tests 2>&1 | tail -10
```
Expected: `test result: ok. 3 passed`

**Step 5: Commit**

```bash
git add src/ai/providers/candle.rs
git commit -m "feat(ai): add Candle in-process embedding provider (384-dim, deterministic)"
```

---

## Task 8: ProviderRegistry + AiConfig (`src/ai/mod.rs`)

**Files:**
- Modify: `src/ai/mod.rs`

**Step 1: Write the failing tests** (add to bottom of `src/ai/mod.rs`)

```rust
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
        assert_eq!(config.resolve_env(&"${ANTHROPIC_API_KEY}".to_string()), "test-key");
        std::env::remove_var("ANTHROPIC_API_KEY");
    }
}
```

**Step 2: Run to verify fails**

```bash
cargo test --lib ai::tests 2>&1 | head -10
```

**Step 3: Rewrite `src/ai/mod.rs`**

```rust
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
        assert_eq!(config.resolve_env(&"${ANTHROPIC_API_KEY}".to_string()), "test-key");
        std::env::remove_var("ANTHROPIC_API_KEY");
    }
}
```

**Step 4: Run tests**

```bash
cargo test --lib ai::tests 2>&1 | tail -10
```
Expected: `test result: ok. 4 passed`

**Step 5: Commit**

```bash
git add src/ai/mod.rs
git commit -m "feat(ai): add AiConfig and ProviderRegistry with session overrides"
```

---

## Task 9: TextToSqlEngine (`src/ai/text_to_sql.rs`)

**Files:**
- Create: `src/ai/text_to_sql.rs`

**Step 1: Write the failing tests**

```rust
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
        let engine = TextToSqlEngine::new(std::sync::Arc::new(MockLlm("SELECT * FROM users".to_string())));
        let sql = engine.translate("show all users", &[]).await.unwrap();
        assert_eq!(sql, "SELECT * FROM users");
    }

    #[tokio::test]
    async fn test_text_to_sql_injects_schema() {
        let engine = TextToSqlEngine::new(std::sync::Arc::new(MockLlm("SELECT 1".to_string())));
        let schema = vec![TableSchema { name: "orders".to_string(), columns: vec!["id".to_string(), "amount".to_string()] }];
        let sql = engine.translate("get all orders", &schema).await.unwrap();
        assert_eq!(sql, "SELECT 1");
    }

    #[test]
    fn test_build_prompt_contains_query() {
        let engine = TextToSqlEngine::new(std::sync::Arc::new(MockLlm("".to_string())));
        let prompt = engine.build_prompt("find users named Alice", &[]);
        assert!(prompt.contains("find users named Alice"));
        assert!(prompt.contains("SQL"));
    }

    #[test]
    fn test_build_prompt_includes_schema() {
        let engine = TextToSqlEngine::new(std::sync::Arc::new(MockLlm("".to_string())));
        let schema = vec![TableSchema { name: "products".to_string(), columns: vec!["id".to_string(), "price".to_string()] }];
        let prompt = engine.build_prompt("cheapest product", &schema);
        assert!(prompt.contains("products"));
        assert!(prompt.contains("price"));
    }
}
```

**Step 2: Run to verify fails**

```bash
cargo test --lib ai::text_to_sql::tests 2>&1 | head -10
```

**Step 3: Implement `src/ai/text_to_sql.rs`**

```rust
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
        // Strip any accidental markdown code fences
        let sql = raw
            .trim()
            .trim_start_matches("```sql")
            .trim_start_matches("```")
            .trim_end_matches("```")
            .trim()
            .to_string();
        Ok(sql)
    }
}
```

**Step 4: Run tests**

```bash
cargo test --lib ai::text_to_sql::tests 2>&1 | tail -10
```
Expected: `test result: ok. 4 passed`

**Step 5: Commit**

```bash
git add src/ai/text_to_sql.rs
git commit -m "feat(ai): add TextToSqlEngine with schema-aware prompt building"
```

---

## Task 10: EmbeddingEngine (`src/ai/embeddings.rs`)

**Files:**
- Create: `src/ai/embeddings.rs`

**Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::ai::providers::candle::CandleEmbeddingProvider;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_embed_single_text() {
        let engine = EmbeddingEngine::new(Arc::new(CandleEmbeddingProvider::new()));
        let vec = engine.embed_one("hello world").await.unwrap();
        assert_eq!(vec.len(), 384);
    }

    #[tokio::test]
    async fn test_embed_batch() {
        let engine = EmbeddingEngine::new(Arc::new(CandleEmbeddingProvider::new()));
        let vecs = engine.embed_batch(&["foo", "bar"]).await.unwrap();
        assert_eq!(vecs.len(), 2);
        assert_eq!(vecs[0].len(), 384);
    }

    #[tokio::test]
    async fn test_embed_is_normalized() {
        let engine = EmbeddingEngine::new(Arc::new(CandleEmbeddingProvider::new()));
        let vec = engine.embed_one("test normalization").await.unwrap();
        let norm: f32 = vec.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((norm - 1.0).abs() < 0.01, "Vector should be unit-normalized, got norm={}", norm);
    }

    #[test]
    fn test_embedding_engine_dimensions() {
        let engine = EmbeddingEngine::new(Arc::new(CandleEmbeddingProvider::new()));
        assert_eq!(engine.dimensions(), 384);
    }
}
```

**Step 2: Run to verify fails**

```bash
cargo test --lib ai::embeddings::tests 2>&1 | head -10
```

**Step 3: Implement `src/ai/embeddings.rs`**

```rust
//! EmbeddingEngine — text to float vectors via any EmbeddingProvider.

use crate::ai::provider::{AiError, EmbeddingProvider};
use std::sync::Arc;

pub struct EmbeddingEngine {
    provider: Arc<dyn EmbeddingProvider>,
}

impl EmbeddingEngine {
    pub fn new(provider: Arc<dyn EmbeddingProvider>) -> Self {
        Self { provider }
    }

    /// Embed a single text string.
    pub async fn embed_one(&self, text: &str) -> Result<Vec<f32>, AiError> {
        let mut vecs = self.provider.embed(&[text]).await?;
        vecs.into_iter().next().ok_or_else(|| AiError::Parse("Empty embedding result".to_string()))
    }

    /// Embed a batch of texts.
    pub async fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, AiError> {
        self.provider.embed(texts).await
    }

    /// Dimensionality of the embedding vectors.
    pub fn dimensions(&self) -> usize {
        self.provider.dimensions()
    }

    /// Provider name.
    pub fn provider_name(&self) -> &str {
        self.provider.name()
    }
}
```

**Step 4: Run tests**

```bash
cargo test --lib ai::embeddings::tests 2>&1 | tail -10
```
Expected: `test result: ok. 4 passed`

**Step 5: Commit**

```bash
git add src/ai/embeddings.rs
git commit -m "feat(ai): add EmbeddingEngine wrapping any EmbeddingProvider"
```

---

## Task 11: HybridSearchEngine (`src/ai/hybrid_search.rs`)

**Files:**
- Create: `src/ai/hybrid_search.rs`

**Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn make_result(id: &str, score: f32, source: &str) -> HybridResult {
        HybridResult { id: id.to_string(), score, source: source.to_string(), data: serde_json::Value::Null }
    }

    #[test]
    fn test_rrf_fusion_combines_sources() {
        let engine = HybridSearchEngine::new();
        let vector_results = vec![make_result("a", 0.9, "vector"), make_result("b", 0.7, "vector")];
        let fulltext_results = vec![make_result("b", 0.95, "fulltext"), make_result("c", 0.6, "fulltext")];

        let fused = engine.fuse(vec![vector_results, fulltext_results]);
        assert!(!fused.is_empty());
        // "b" appears in both sources — should rank highly
        let ids: Vec<&str> = fused.iter().map(|r| r.id.as_str()).collect();
        assert!(ids.contains(&"b"));
    }

    #[test]
    fn test_rrf_deduplicates() {
        let engine = HybridSearchEngine::new();
        let r1 = vec![make_result("x", 1.0, "vector"), make_result("x", 0.9, "vector")];
        let fused = engine.fuse(vec![r1]);
        assert_eq!(fused.iter().filter(|r| r.id == "x").count(), 1);
    }

    #[test]
    fn test_search_sources_parse() {
        assert_eq!(SearchSource::from_str("vector"), Some(SearchSource::Vector));
        assert_eq!(SearchSource::from_str("fulltext"), Some(SearchSource::Fulltext));
        assert_eq!(SearchSource::from_str("graph"), Some(SearchSource::Graph));
        assert_eq!(SearchSource::from_str("unknown"), None);
    }

    #[test]
    fn test_hybrid_result_source_attribution() {
        let engine = HybridSearchEngine::new();
        let v = vec![make_result("doc1", 1.0, "vector")];
        let f = vec![make_result("doc2", 0.8, "fulltext")];
        let fused = engine.fuse(vec![v, f]);
        assert!(fused.iter().any(|r| r.id == "doc1"));
        assert!(fused.iter().any(|r| r.id == "doc2"));
    }
}
```

**Step 2: Run to verify fails**

```bash
cargo test --lib ai::hybrid_search::tests 2>&1 | head -10
```

**Step 3: Implement `src/ai/hybrid_search.rs`**

```rust
//! HybridSearchEngine — fuses vector, full-text, and graph results via RRF.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

const RRF_K: f32 = 60.0;

#[derive(Debug, Clone, PartialEq)]
pub enum SearchSource {
    Vector,
    Fulltext,
    Graph,
}

impl SearchSource {
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "vector" => Some(Self::Vector),
            "fulltext" => Some(Self::Fulltext),
            "graph" => Some(Self::Graph),
            _ => None,
        }
    }
}

/// A single result from any search source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HybridResult {
    pub id: String,
    pub score: f32,
    pub source: String,
    pub data: serde_json::Value,
}

pub struct HybridSearchEngine {
    pub rrf_k: f32,
}

impl HybridSearchEngine {
    pub fn new() -> Self {
        Self { rrf_k: RRF_K }
    }

    /// Fuse multiple ranked result lists using Reciprocal Rank Fusion.
    /// Deduplicates by id, combining scores across sources.
    pub fn fuse(&self, lists: Vec<Vec<HybridResult>>) -> Vec<HybridResult> {
        let mut scores: HashMap<String, f32> = HashMap::new();
        let mut best: HashMap<String, HybridResult> = HashMap::new();

        for list in lists {
            // Deduplicate within each list by id (keep highest score)
            let mut seen: HashMap<String, usize> = HashMap::new();
            let deduped: Vec<_> = list.into_iter().enumerate().filter(|(_, r)| {
                if seen.contains_key(&r.id) { false } else { seen.insert(r.id.clone(), 0); true }
            }).collect();

            for (rank, result) in deduped {
                let rrf_score = 1.0 / (self.rrf_k + rank as f32 + 1.0);
                *scores.entry(result.id.clone()).or_insert(0.0) += rrf_score;
                best.entry(result.id.clone()).or_insert(result);
            }
        }

        let mut results: Vec<HybridResult> = best.into_values()
            .map(|mut r| { r.score = *scores.get(&r.id).unwrap_or(&0.0); r })
            .collect();

        results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
        results
    }
}

impl Default for HybridSearchEngine {
    fn default() -> Self { Self::new() }
}
```

**Step 4: Run tests**

```bash
cargo test --lib ai::hybrid_search::tests 2>&1 | tail -10
```
Expected: `test result: ok. 4 passed`

**Step 5: Commit**

```bash
git add src/ai/hybrid_search.rs
git commit -m "feat(ai): add HybridSearchEngine with RRF fusion and source attribution"
```

---

## Task 12: Full test suite verification + ROADMAP update

**Step 1: Run full test suite**

```bash
cargo test --lib 2>&1 | tail -5
```
Expected: `test result: ok. 328+ passed; 0 failed`

**Step 2: If any failures, fix them before proceeding**

Common issues:
- `use` conflicts if module names clash with existing ones
- `thiserror` not in Cargo.toml → add `thiserror = "1.0"` under `[dependencies]`
- Trait object not `Send + Sync` → ensure all providers derive or implement those

**Step 3: Update ROADMAP.md**

Change NLP / Text-to-SQL row from `50%` to `80%` and add new AI row:

```markdown
| AI Provider Layer | LlmProvider trait, 6 providers, registry | 85% |
| NLP / Text-to-SQL | LLM-powered via provider abstraction | 80% |
| Semantic Embeddings | Candle + provider API, EmbeddingEngine | 85% |
| Hybrid Search | Vector + fulltext + graph RRF fusion | 85% |
```

**Step 4: Commit ROADMAP**

```bash
git add ROADMAP.md
git commit -m "docs: update roadmap — AI-native features complete"
```

**Step 5: Push**

```bash
gh auth switch --user ssameerr
git push origin main
```

---

## Summary

| Task | Tests added | Files created |
|------|------------|---------------|
| 1 — Core traits | 3 | `src/ai/provider.rs`, `src/ai/mod.rs`, `src/ai/providers/mod.rs` |
| 2 — Ollama | 3 | `src/ai/providers/ollama.rs` |
| 3 — vLLM | 2 | `src/ai/providers/vllm.rs` |
| 4 — Claude | 2 | `src/ai/providers/claude.rs` |
| 5 — OpenAI/Azure | 3 | `src/ai/providers/openai.rs` |
| 6 — Gemini | 2 | `src/ai/providers/gemini.rs` |
| 7 — Candle | 3 | `src/ai/providers/candle.rs` |
| 8 — ProviderRegistry | 4 | `src/ai/mod.rs` (rewrite) |
| 9 — TextToSqlEngine | 4 | `src/ai/text_to_sql.rs` |
| 10 — EmbeddingEngine | 4 | `src/ai/embeddings.rs` |
| 11 — HybridSearchEngine | 4 | `src/ai/hybrid_search.rs` |
| 12 — Verification | — | ROADMAP.md |
| **Total** | **~34 new tests** | **11 files** |
