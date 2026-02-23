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
            // Deduplicate within each list by id (keep first occurrence, highest rank)
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
