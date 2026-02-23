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
