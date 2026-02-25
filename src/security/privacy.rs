//! Differential Privacy Engine
//!
//! Provides privacy-preserving query responses using Laplacian noise

use std::collections::HashMap;
use tokio::sync::RwLock;
use rand::Rng;
use super::SecurityError;

/// Differential Privacy engine
pub struct DifferentialPrivacyEngine {
    /// Global epsilon (privacy budget)
    epsilon: f64,
    /// Per-user budget tracking
    user_budgets: RwLock<HashMap<String, f64>>,
    /// Maximum queries per user
    max_queries_per_user: usize,
}

impl DifferentialPrivacyEngine {
    pub fn new(epsilon: f64) -> Self {
        Self {
            epsilon,
            user_budgets: RwLock::new(HashMap::new()),
            max_queries_per_user: 1000,
        }
    }

    /// Add Laplacian noise to a value
    pub fn add_laplace_noise(&self, value: f64, sensitivity: f64) -> f64 {
        let scale = sensitivity / self.epsilon;
        let noise = self.sample_laplace(scale);
        value + noise
    }

    /// Sample from Laplace distribution
    fn sample_laplace(&self, scale: f64) -> f64 {
        let mut rng = rand::thread_rng();
        let u: f64 = rng.gen_range(-0.5..0.5);
        -scale * u.signum() * (1.0 - 2.0 * u.abs()).ln()
    }

    /// Check and consume privacy budget
    pub async fn consume_budget(&self, user_id: &str, cost: f64) -> Result<(), SecurityError> {
        let mut budgets = self.user_budgets.write().await;
        let remaining = budgets.entry(user_id.to_string()).or_insert(self.epsilon);
        
        if *remaining < cost {
            return Err(SecurityError::PrivacyBudgetExceeded);
        }
        
        *remaining -= cost;
        Ok(())
    }

    /// Get remaining budget for a user
    pub async fn get_remaining_budget(&self, user_id: &str) -> f64 {
        let budgets = self.user_budgets.read().await;
        *budgets.get(user_id).unwrap_or(&self.epsilon)
    }

    /// Reset budget for a user
    pub async fn reset_budget(&self, user_id: &str) {
        let mut budgets = self.user_budgets.write().await;
        budgets.insert(user_id.to_string(), self.epsilon);
    }

    /// Add noise to a count query
    pub fn noisy_count(&self, count: i64, sensitivity: f64) -> i64 {
        let noisy = self.add_laplace_noise(count as f64, sensitivity);
        noisy.round().max(0.0) as i64
    }

    /// Add noise to a sum query
    pub fn noisy_sum(&self, sum: f64, sensitivity: f64) -> f64 {
        self.add_laplace_noise(sum, sensitivity)
    }

    /// Add noise to an average query
    pub fn noisy_avg(&self, sum: f64, count: i64, sensitivity: f64) -> f64 {
        if count == 0 { return 0.0; }
        let noisy_sum = self.add_laplace_noise(sum, sensitivity);
        noisy_sum / count as f64
    }
}

impl Default for DifferentialPrivacyEngine {
    fn default() -> Self { Self::new(1.0) }
}
