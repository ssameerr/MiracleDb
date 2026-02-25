//! Sequence Module - Sequence and auto-increment management

use std::collections::HashMap;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

/// Sequence
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Sequence {
    pub name: String,
    pub current_value: i64,
    pub start_value: i64,
    pub increment: i64,
    pub min_value: i64,
    pub max_value: i64,
    pub cycle: bool,
    pub cache_size: usize,
}

impl Sequence {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            current_value: 0,
            start_value: 1,
            increment: 1,
            min_value: 1,
            max_value: i64::MAX,
            cycle: false,
            cache_size: 1,
        }
    }

    pub fn with_start(mut self, start: i64) -> Self {
        self.start_value = start;
        self.current_value = start - self.increment;
        self
    }

    pub fn with_increment(mut self, increment: i64) -> Self {
        self.increment = increment;
        self
    }

    pub fn with_min(mut self, min: i64) -> Self {
        self.min_value = min;
        self
    }

    pub fn with_max(mut self, max: i64) -> Self {
        self.max_value = max;
        self
    }

    pub fn with_cycle(mut self, cycle: bool) -> Self {
        self.cycle = cycle;
        self
    }

    pub fn with_cache(mut self, cache_size: usize) -> Self {
        self.cache_size = cache_size;
        self
    }
}

/// Sequence cache
struct SequenceCache {
    values: Vec<i64>,
    current_index: usize,
}

impl SequenceCache {
    fn new() -> Self {
        Self {
            values: vec![],
            current_index: 0,
        }
    }

    fn is_empty(&self) -> bool {
        self.current_index >= self.values.len()
    }

    fn next(&mut self) -> Option<i64> {
        if self.current_index < self.values.len() {
            let val = self.values[self.current_index];
            self.current_index += 1;
            Some(val)
        } else {
            None
        }
    }

    fn fill(&mut self, start: i64, count: usize, increment: i64) {
        self.values = (0..count)
            .map(|i| start + (i as i64 * increment))
            .collect();
        self.current_index = 0;
    }
}

/// Sequence manager
pub struct SequenceManager {
    sequences: RwLock<HashMap<String, Sequence>>,
    caches: RwLock<HashMap<String, SequenceCache>>,
}

impl SequenceManager {
    pub fn new() -> Self {
        Self {
            sequences: RwLock::new(HashMap::new()),
            caches: RwLock::new(HashMap::new()),
        }
    }

    /// Create a sequence
    pub async fn create(&self, sequence: Sequence) -> Result<(), String> {
        let mut sequences = self.sequences.write().await;
        
        if sequences.contains_key(&sequence.name) {
            return Err(format!("Sequence {} already exists", sequence.name));
        }

        sequences.insert(sequence.name.clone(), sequence);
        Ok(())
    }

    /// Drop a sequence
    pub async fn drop(&self, name: &str) -> Result<(), String> {
        let mut sequences = self.sequences.write().await;
        let mut caches = self.caches.write().await;
        
        sequences.remove(name)
            .ok_or_else(|| format!("Sequence {} not found", name))?;
        caches.remove(name);
        
        Ok(())
    }

    /// Get next value
    pub async fn next_val(&self, name: &str) -> Result<i64, String> {
        // Try to get from cache first
        {
            let mut caches = self.caches.write().await;
            if let Some(cache) = caches.get_mut(name) {
                if let Some(val) = cache.next() {
                    return Ok(val);
                }
            }
        }

        // Refill cache
        let mut sequences = self.sequences.write().await;
        let mut caches = self.caches.write().await;

        let seq = sequences.get_mut(name)
            .ok_or_else(|| format!("Sequence {} not found", name))?;

        let next_value = seq.current_value + seq.increment;

        if next_value > seq.max_value {
            if seq.cycle {
                seq.current_value = seq.min_value;
            } else {
                return Err("Sequence reached maximum value".to_string());
            }
        } else if next_value < seq.min_value {
            if seq.cycle {
                seq.current_value = seq.max_value;
            } else {
                return Err("Sequence reached minimum value".to_string());
            }
        } else {
            seq.current_value = next_value;
        }

        // Fill cache
        let cache = caches.entry(name.to_string()).or_insert_with(SequenceCache::new);
        cache.fill(seq.current_value, seq.cache_size, seq.increment);

        // Update sequence current value to account for cached values
        seq.current_value += (seq.cache_size as i64 - 1) * seq.increment;

        cache.next().ok_or_else(|| "Failed to get value from cache".to_string())
    }

    /// Get current value (without incrementing)
    pub async fn curr_val(&self, name: &str) -> Result<i64, String> {
        let sequences = self.sequences.read().await;
        let seq = sequences.get(name)
            .ok_or_else(|| format!("Sequence {} not found", name))?;
        Ok(seq.current_value)
    }

    /// Set sequence value
    pub async fn set_val(&self, name: &str, value: i64) -> Result<(), String> {
        let mut sequences = self.sequences.write().await;
        let mut caches = self.caches.write().await;

        let seq = sequences.get_mut(name)
            .ok_or_else(|| format!("Sequence {} not found", name))?;

        if value < seq.min_value || value > seq.max_value {
            return Err("Value out of range".to_string());
        }

        seq.current_value = value;
        caches.remove(name); // Invalidate cache

        Ok(())
    }

    /// Reset sequence
    pub async fn reset(&self, name: &str) -> Result<(), String> {
        let mut sequences = self.sequences.write().await;
        let mut caches = self.caches.write().await;

        let seq = sequences.get_mut(name)
            .ok_or_else(|| format!("Sequence {} not found", name))?;

        seq.current_value = seq.start_value - seq.increment;
        caches.remove(name);

        Ok(())
    }

    /// List sequences
    pub async fn list(&self) -> Vec<Sequence> {
        let sequences = self.sequences.read().await;
        sequences.values().cloned().collect()
    }

    /// Get sequence info
    pub async fn get(&self, name: &str) -> Option<Sequence> {
        let sequences = self.sequences.read().await;
        sequences.get(name).cloned()
    }
}

impl Default for SequenceManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_sequence() {
        let manager = SequenceManager::new();
        
        manager.create(Sequence::new("test_seq").with_start(1)).await.unwrap();
        
        assert_eq!(manager.next_val("test_seq").await.unwrap(), 1);
        assert_eq!(manager.next_val("test_seq").await.unwrap(), 2);
        assert_eq!(manager.next_val("test_seq").await.unwrap(), 3);
    }

    #[tokio::test]
    async fn test_cycle() {
        let manager = SequenceManager::new();
        
        manager.create(
            Sequence::new("cycle_seq")
                .with_start(1)
                .with_max(3)
                .with_cycle(true)
        ).await.unwrap();
        
        assert_eq!(manager.next_val("cycle_seq").await.unwrap(), 1);
        assert_eq!(manager.next_val("cycle_seq").await.unwrap(), 2);
        assert_eq!(manager.next_val("cycle_seq").await.unwrap(), 3);
        assert_eq!(manager.next_val("cycle_seq").await.unwrap(), 1); // Cycles
    }
}
