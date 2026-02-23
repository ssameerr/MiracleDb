//! AI layer — LLM providers, embeddings, Text-to-SQL, hybrid search.
pub mod provider;
pub mod providers;
pub mod text_to_sql;
pub mod embeddings;
pub mod hybrid_search;

pub use provider::{AiError, LlmProvider, EmbeddingProvider};
