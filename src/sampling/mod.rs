//! Sampling Module - Statistical sampling utilities

use std::collections::HashSet;
use rand::Rng;
use serde::{Serialize, Deserialize};

/// Sampling method
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum SamplingMethod {
    Random,
    Systematic,
    Stratified,
    Reservoir,
    Bernoulli,
}

/// Sample configuration
#[derive(Clone, Debug)]
pub struct SampleConfig {
    pub method: SamplingMethod,
    pub size: usize,
    pub seed: Option<u64>,
}

/// Sampler
pub struct Sampler;

impl Sampler {
    /// Random sampling
    pub fn random<T: Clone>(data: &[T], n: usize) -> Vec<T> {
        if n >= data.len() {
            return data.to_vec();
        }

        let mut rng = rand::thread_rng();
        let mut indices: HashSet<usize> = HashSet::new();

        while indices.len() < n {
            indices.insert(rng.gen_range(0..data.len()));
        }

        indices.into_iter()
            .map(|i| data[i].clone())
            .collect()
    }

    /// Systematic sampling (every k-th element)
    pub fn systematic<T: Clone>(data: &[T], n: usize) -> Vec<T> {
        if n >= data.len() {
            return data.to_vec();
        }

        let k = data.len() / n;
        let mut rng = rand::thread_rng();
        let start: usize = rng.gen_range(0..k);

        (0..n)
            .map(|i| data[(start + i * k) % data.len()].clone())
            .collect()
    }

    /// Stratified sampling
    pub fn stratified<T: Clone, F>(data: &[T], n: usize, strata_fn: F) -> Vec<T>
    where
        F: Fn(&T) -> String,
    {
        use std::collections::HashMap;

        // Group by strata
        let mut strata: HashMap<String, Vec<T>> = HashMap::new();
        for item in data {
            let key = strata_fn(item);
            strata.entry(key).or_insert_with(Vec::new).push(item.clone());
        }

        // Sample proportionally from each stratum
        let mut result = Vec::new();
        let total = data.len();

        for (_, items) in strata {
            let stratum_size = items.len();
            let sample_size = (n * stratum_size) / total;
            let sample = Self::random(&items, sample_size.max(1));
            result.extend(sample);
        }

        // Trim to exact size
        result.truncate(n);
        result
    }

    /// Reservoir sampling (for streaming data)
    pub fn reservoir<T: Clone>(stream: impl Iterator<Item = T>, k: usize) -> Vec<T> {
        let mut reservoir = Vec::with_capacity(k);
        let mut rng = rand::thread_rng();

        for (i, item) in stream.enumerate() {
            if i < k {
                reservoir.push(item);
            } else {
                let j = rng.gen_range(0..=i);
                if j < k {
                    reservoir[j] = item;
                }
            }
        }

        reservoir
    }

    /// Bernoulli sampling (each element with probability p)
    pub fn bernoulli<T: Clone>(data: &[T], p: f64) -> Vec<T> {
        let mut rng = rand::thread_rng();
        data.iter()
            .filter(|_| rng.gen::<f64>() < p)
            .cloned()
            .collect()
    }

    /// Weighted random sampling
    pub fn weighted<T: Clone>(data: &[(T, f64)], n: usize) -> Vec<T> {
        let total_weight: f64 = data.iter().map(|(_, w)| w).sum();
        let mut rng = rand::thread_rng();
        let mut result = Vec::with_capacity(n);

        for _ in 0..n {
            let target = rng.gen::<f64>() * total_weight;
            let mut cumulative = 0.0;

            for (item, weight) in data {
                cumulative += weight;
                if cumulative >= target {
                    result.push(item.clone());
                    break;
                }
            }
        }

        result
    }

    /// Bootstrap resampling
    pub fn bootstrap<T: Clone>(data: &[T], n: usize) -> Vec<T> {
        let mut rng = rand::thread_rng();
        (0..n)
            .map(|_| data[rng.gen_range(0..data.len())].clone())
            .collect()
    }

    /// Jackknife resampling (leave-one-out)
    pub fn jackknife<T: Clone>(data: &[T]) -> Vec<Vec<T>> {
        (0..data.len())
            .map(|i| {
                data.iter()
                    .enumerate()
                    .filter(|(j, _)| *j != i)
                    .map(|(_, v)| v.clone())
                    .collect()
            })
            .collect()
    }
}

/// Statistical summary
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Stats {
    pub count: usize,
    pub min: f64,
    pub max: f64,
    pub sum: f64,
    pub mean: f64,
    pub variance: f64,
    pub std_dev: f64,
    pub median: f64,
    pub percentiles: Vec<(u8, f64)>,
}

impl Stats {
    pub fn from_data(data: &[f64]) -> Self {
        if data.is_empty() {
            return Self {
                count: 0,
                min: 0.0,
                max: 0.0,
                sum: 0.0,
                mean: 0.0,
                variance: 0.0,
                std_dev: 0.0,
                median: 0.0,
                percentiles: vec![],
            };
        }

        let mut sorted = data.to_vec();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let count = data.len();
        let sum: f64 = data.iter().sum();
        let mean = sum / count as f64;
        let variance: f64 = data.iter()
            .map(|x| (x - mean).powi(2))
            .sum::<f64>() / count as f64;

        let median = if count % 2 == 0 {
            (sorted[count / 2 - 1] + sorted[count / 2]) / 2.0
        } else {
            sorted[count / 2]
        };

        let percentiles = vec![
            (25, Self::percentile(&sorted, 25)),
            (50, Self::percentile(&sorted, 50)),
            (75, Self::percentile(&sorted, 75)),
            (90, Self::percentile(&sorted, 90)),
            (95, Self::percentile(&sorted, 95)),
            (99, Self::percentile(&sorted, 99)),
        ];

        Self {
            count,
            min: sorted[0],
            max: sorted[count - 1],
            sum,
            mean,
            variance,
            std_dev: variance.sqrt(),
            median,
            percentiles,
        }
    }

    fn percentile(sorted: &[f64], p: u8) -> f64 {
        let idx = (p as f64 / 100.0 * sorted.len() as f64) as usize;
        sorted[idx.min(sorted.len() - 1)]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_random_sampling() {
        let data: Vec<i32> = (0..100).collect();
        let sample = Sampler::random(&data, 10);
        assert_eq!(sample.len(), 10);
    }

    #[test]
    fn test_reservoir_sampling() {
        let data: Vec<i32> = (0..1000).collect();
        let sample = Sampler::reservoir(data.into_iter(), 10);
        assert_eq!(sample.len(), 10);
    }

    #[test]
    fn test_stats() {
        let data = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let stats = Stats::from_data(&data);
        assert_eq!(stats.count, 5);
        assert_eq!(stats.mean, 3.0);
        assert_eq!(stats.min, 1.0);
        assert_eq!(stats.max, 5.0);
    }
}
