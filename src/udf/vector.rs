//! Vector search utilities

/// Cosine similarity between two vectors
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() || a.is_empty() { return 0.0; }
    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm_a == 0.0 || norm_b == 0.0 { 0.0 } else { dot / (norm_a * norm_b) }
}

/// L2 (Euclidean) distance
pub fn l2_distance(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() { return f32::MAX; }
    a.iter().zip(b.iter()).map(|(x, y)| (x - y).powi(2)).sum::<f32>().sqrt()
}

/// Reciprocal Rank Fusion for hybrid search
pub fn rrf(ranks: &[usize], k: f32) -> f32 {
    ranks.iter().map(|&r| 1.0 / (k + r as f32)).sum()
}

/// Normalize vector to unit length
pub fn normalize(v: &mut [f32]) {
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 { for x in v.iter_mut() { *x /= norm; } }
}
