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
