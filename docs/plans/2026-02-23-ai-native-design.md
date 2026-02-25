# AI-Native Features Design

**Date:** 2026-02-23
**Status:** Implemented — v0.6 shipped, 344 tests passing
**Scope:** v0.6 — AI-Native Features (Phase C of the MiracleDb roadmap)

---

## Overview

Add a unified AI layer to MiracleDb with three capabilities:

1. **LLM-powered Text-to-SQL** — natural language → valid SQL via local or cloud LLMs
2. **Semantic Embeddings** — text → float vectors via local (Candle) or provider APIs
3. **Hybrid Search** — vector + full-text + graph results fused via RRF, queryable from SQL

---

## Architecture

### Approach: Trait-based Provider System

Define `LlmProvider` and `EmbeddingProvider` traits. Each provider (Ollama, vLLM, Candle, Claude, OpenAI, Azure, Gemini) is an independent struct implementing the relevant trait. A `ProviderRegistry` selects the active provider based on config and session state.

### Module Layout

```
src/ai/
  mod.rs              # re-exports, AiConfig, ProviderRegistry
  provider.rs         # LlmProvider + EmbeddingProvider traits
  providers/
    ollama.rs         # Ollama (local HTTP API, default)
    vllm.rs           # vLLM (OpenAI-compatible local API)
    candle.rs         # Candle (in-process, no HTTP, embeddings)
    claude.rs         # Anthropic Claude API
    openai.rs         # OpenAI + Azure OpenAI (same client, configurable base URL)
    gemini.rs         # Google Gemini API
  text_to_sql.rs      # TextToSqlEngine using LlmProvider
  embeddings.rs       # EmbeddingEngine using EmbeddingProvider
  hybrid_search.rs    # HybridSearchEngine: vector + fulltext + graph via RRF
```

### Core Traits

```rust
#[async_trait]
pub trait LlmProvider: Send + Sync {
    async fn complete(&self, prompt: &str) -> Result<String, AiError>;
    fn name(&self) -> &str;
}

#[async_trait]
pub trait EmbeddingProvider: Send + Sync {
    async fn embed(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, AiError>;
    fn dimensions(&self) -> usize;
    fn name(&self) -> &str;
}
```

---

## Configuration

**Format: TOML** (`miracledb.toml`) — Rust-native, already established in the codebase.

```toml
[llm]
default_provider = "ollama"
fallback_provider = "claude"

[llm.ollama]
base_url = "http://localhost:11434"
model = "llama3.2:3b"

[llm.vllm]
base_url = "http://localhost:8000"
model = "defog/sqlcoder-7b-2"

[llm.claude]
api_key = "${ANTHROPIC_API_KEY}"
model = "claude-haiku-4-5-20251001"

[llm.openai]
api_key = "${OPENAI_API_KEY}"
model = "gpt-4o-mini"
base_url = "https://api.openai.com/v1"   # override for Azure

[llm.gemini]
api_key = "${GEMINI_API_KEY}"
model = "gemini-1.5-flash"

[llm.embeddings]
default_provider = "candle"
model = "all-MiniLM-L6-v2"
fallback_provider = "openai"
```

**Runtime override via SQL (PostgreSQL GUC-style):**
```sql
SET llm.provider = 'ollama';
SET llm.embeddings_provider = 'openai';
```

**Provider priority chain:**
1. Session-level `SET` override
2. `default_provider` from config
3. `fallback_provider` from config
4. Error if all unavailable

---

## Data Flow

### Text-to-SQL

```
User: SELECT ai_query('show me orders over $500')
  │
  ▼
TextToSqlEngine
  │  builds prompt: system instructions + table schemas + few-shot examples + user query
  ▼
ProviderRegistry.complete(prompt)
  ├── Ollama (default, local, ~200ms–2s)
  ├── vLLM  (if configured)
  └── Claude/OpenAI/Gemini (cloud fallback, ~300ms–1.5s)
  │
  ▼
Generated SQL → validated (basic syntax check) → DataFusion engine → results
```

### Embeddings

```
Text input
  │
  ▼
EmbeddingEngine
  ├── Candle all-MiniLM-L6-v2 (default, in-process, ~50ms, 384-dim)
  └── Provider API: OpenAI text-embedding-3-small / Ollama nomic-embed-text / Gemini
  │
  ▼
Vec<f32> → stored in Lance / used for ANN search
```

### Hybrid Search

```
SELECT * FROM hybrid_search('angry customers', sources => ['vector','fulltext','graph'], depth => 1)
  │
  ▼
HybridSearchEngine
  ├── VectorSearch  — Lance ANN on stored embeddings
  ├── FullTextSearch — Tantivy BM25
  └── GraphSearch   — node traversal (depth=1 by default), fetch embeddings of related nodes
  │
  ▼
RRF fusion (k=60, equal weights by default, configurable)
  │
  ▼
Ranked results with source attribution
```

**`sources` parameter:**
- `['vector']` — ANN only
- `['fulltext']` — BM25 only
- `['vector', 'fulltext']` — existing RRF behavior
- `['vector', 'fulltext', 'graph']` — full hybrid (new)

---

## Provider Cost Reference

| Provider | ~Cost per Text-to-SQL query | Latency |
|----------|----------------------------|---------|
| Ollama llama3.2:3b (local) | Free, needs 4GB RAM | 200ms–2s |
| Ollama SQLCoder-7B (local) | Free, needs 8GB RAM | 500ms–3s |
| vLLM SQLCoder-34B | Free, needs 20GB GPU | 200ms–1s |
| Claude Haiku 4.5 | ~$0.0003 | 300ms–800ms |
| GPT-4o mini | ~$0.0001 | 300ms–600ms |
| Gemini 1.5 Flash | ~$0.00005 | 200ms–500ms |

---

## Testing Strategy

### Tier 1 — Unit tests (always run, `cargo test --lib`)

- Mock `LlmProvider` returning fixed SQL strings
- Mock `EmbeddingProvider` returning fixed float vectors
- Test `TextToSqlEngine` prompt construction (schema injection, few-shot format)
- Test `ProviderRegistry` config loading, fallback chain, session SET override
- Test `HybridSearchEngine` RRF fusion (weights, ranking, deduplication, source attribution)
- Test `EmbeddingEngine` output normalization and dimension validation
- ~20 new deterministic tests

### Tier 2 — Integration tests (opt-in, require live service)

```bash
MIRACLEDB_TEST_OLLAMA=http://localhost:11434 cargo test --features integration
ANTHROPIC_API_KEY=sk-... cargo test --features integration
```

- Verify actual SQL generation quality
- Verify embedding dimensions match model spec
- CI stays fast — no external deps required

### Mock pattern

```rust
struct MockLlmProvider { response: String }

impl LlmProvider for MockLlmProvider {
    async fn complete(&self, _prompt: &str) -> Result<String, AiError> {
        Ok(self.response.clone())
    }
    fn name(&self) -> &str { "mock" }
}
```

---

## Files to Create / Modify

| File | Action |
|------|--------|
| `src/ai/mod.rs` | Create — AiConfig, ProviderRegistry |
| `src/ai/provider.rs` | Create — LlmProvider, EmbeddingProvider traits, AiError |
| `src/ai/providers/ollama.rs` | Create — OllamaProvider |
| `src/ai/providers/vllm.rs` | Create — VllmProvider |
| `src/ai/providers/candle.rs` | Create — CandleEmbeddingProvider |
| `src/ai/providers/claude.rs` | Create — ClaudeProvider |
| `src/ai/providers/openai.rs` | Create — OpenAiProvider (+ Azure via base_url) |
| `src/ai/providers/gemini.rs` | Create — GeminiProvider |
| `src/ai/text_to_sql.rs` | Create — TextToSqlEngine |
| `src/ai/embeddings.rs` | Create — EmbeddingEngine |
| `src/ai/hybrid_search.rs` | Create — HybridSearchEngine |
| `src/lib.rs` | Modify — add `pub mod ai` |
| `miracledb.toml` | Modify — add `[llm]` section |

---

## Success Criteria

- `cargo test --lib` stays green (308+ tests, 0 failures)
- `SELECT ai_query('show all users')` returns `SELECT * FROM users` via mock provider
- `HybridSearchEngine` fuses 3 sources and returns ranked results
- `ProviderRegistry` correctly falls back from Ollama → Claude when primary unavailable
- All 6 providers compile and have unit tests (mock-based)
