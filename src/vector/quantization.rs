use serde::{Serialize, Deserialize};

/// Product Quantizer for Approximate Nearest Neighbor Search
#[derive(Clone, Serialize, Deserialize)]
pub struct ProductQuantizer {
    pub dimension: usize,
    pub sub_vectors: usize,
    // Codebooks [sub_vector_idx][centroid_idx][value]
    pub codebooks: Vec<Vec<Vec<f32>>>, 
}

impl ProductQuantizer {
    pub fn new(dimension: usize, sub_vectors: usize) -> Self {
        assert_eq!(dimension % sub_vectors, 0, "Dimension must be divisible by sub_vectors");
        Self {
            dimension,
            sub_vectors,
            codebooks: Vec::new(),
        }
    }

    /// Mock training: Just initializes random centroids
    pub fn train(&mut self, _data: &[Vec<f32>], clusters: usize) {
        let dim_per_sub = self.dimension / self.sub_vectors;
        self.codebooks.clear();
        
        for _ in 0..self.sub_vectors {
            let mut centroids = Vec::new();
            for _ in 0..clusters {
                // Initialize with 0.0 ideally used k-means here
                centroids.push(vec![0.5; dim_per_sub]); 
            }
            self.codebooks.push(centroids);
        }
    }

    /// Encode a vector into discrete codes
    pub fn encode(&self, vector: &[f32]) -> Vec<u8> {
        let dim_per_sub = self.dimension / self.sub_vectors;
        let mut codes = Vec::with_capacity(self.sub_vectors);
        
        for i in 0..self.sub_vectors {
            let start = i * dim_per_sub;
            let end = start + dim_per_sub;
            let sub_vec = &vector[start..end];
            
            // Find nearest centroid
            let mut min_dist = f32::MAX;
            let mut nearest_idx = 0;
            
            if let Some(book) = self.codebooks.get(i) {
                for (idx, centroid) in book.iter().enumerate() {
                    let d = euclidean_distance(sub_vec, centroid);
                    if d < min_dist {
                        min_dist = d;
                        nearest_idx = idx;
                    }
                }
            }
            codes.push(nearest_idx as u8);
        }
        codes
    }
}

fn euclidean_distance(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b.iter())
        .map(|(x, y)| (x - y).powi(2))
        .sum::<f32>()
        .sqrt()
}
