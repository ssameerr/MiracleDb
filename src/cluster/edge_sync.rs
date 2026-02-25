//! Edge Sync - CRDT-based multi-master replication
use std::collections::{HashMap, HashSet};
use serde::{Serialize, Deserialize};

/// Last-Write-Wins Register CRDT
#[derive(Clone, Serialize, Deserialize)]
pub struct LwwRegister<T> {
    value: T,
    timestamp: i64,
    node_id: String,
}

impl<T: Clone> LwwRegister<T> {
    pub fn new(value: T, node_id: &str) -> Self {
        Self {
            value,
            timestamp: chrono::Utc::now().timestamp_millis(),
            node_id: node_id.to_string(),
        }
    }

    pub fn update(&mut self, value: T) {
        self.value = value;
        self.timestamp = chrono::Utc::now().timestamp_millis();
    }

    pub fn merge(&mut self, other: &LwwRegister<T>) {
        if other.timestamp > self.timestamp ||
           (other.timestamp == self.timestamp && other.node_id > self.node_id) {
            self.value = other.value.clone();
            self.timestamp = other.timestamp;
            self.node_id = other.node_id.clone();
        }
    }

    pub fn value(&self) -> &T { &self.value }
    pub fn timestamp(&self) -> i64 { self.timestamp }
}

/// G-Counter CRDT (grow-only counter)
#[derive(Clone, Serialize, Deserialize)]
pub struct GCounter {
    counts: HashMap<String, u64>,
}

impl GCounter {
    pub fn new() -> Self { Self { counts: HashMap::new() } }

    pub fn increment(&mut self, node_id: &str) {
        *self.counts.entry(node_id.to_string()).or_insert(0) += 1;
    }

    pub fn increment_by(&mut self, node_id: &str, amount: u64) {
        *self.counts.entry(node_id.to_string()).or_insert(0) += amount;
    }

    pub fn value(&self) -> u64 { self.counts.values().sum() }

    pub fn merge(&mut self, other: &GCounter) {
        for (node, count) in &other.counts {
            let entry = self.counts.entry(node.clone()).or_insert(0);
            *entry = (*entry).max(*count);
        }
    }
}

impl Default for GCounter {
    fn default() -> Self { Self::new() }
}

/// PN-Counter CRDT (positive-negative counter)
#[derive(Clone, Serialize, Deserialize)]
pub struct PnCounter {
    positive: GCounter,
    negative: GCounter,
}

impl PnCounter {
    pub fn new() -> Self {
        Self {
            positive: GCounter::new(),
            negative: GCounter::new(),
        }
    }

    pub fn increment(&mut self, node_id: &str) {
        self.positive.increment(node_id);
    }

    pub fn decrement(&mut self, node_id: &str) {
        self.negative.increment(node_id);
    }

    pub fn value(&self) -> i64 {
        self.positive.value() as i64 - self.negative.value() as i64
    }

    pub fn merge(&mut self, other: &PnCounter) {
        self.positive.merge(&other.positive);
        self.negative.merge(&other.negative);
    }
}

impl Default for PnCounter {
    fn default() -> Self { Self::new() }
}

/// G-Set CRDT (grow-only set)
#[derive(Clone, Serialize, Deserialize)]
pub struct GSet<T: std::hash::Hash + Eq + Clone> {
    elements: HashSet<T>,
}

impl<T: std::hash::Hash + Eq + Clone> GSet<T> {
    pub fn new() -> Self { Self { elements: HashSet::new() } }

    pub fn add(&mut self, element: T) {
        self.elements.insert(element);
    }

    pub fn contains(&self, element: &T) -> bool {
        self.elements.contains(element)
    }

    pub fn merge(&mut self, other: &GSet<T>) {
        for element in &other.elements {
            self.elements.insert(element.clone());
        }
    }

    pub fn elements(&self) -> &HashSet<T> { &self.elements }
    pub fn len(&self) -> usize { self.elements.len() }
    pub fn is_empty(&self) -> bool { self.elements.is_empty() }
}

impl<T: std::hash::Hash + Eq + Clone> Default for GSet<T> {
    fn default() -> Self { Self::new() }
}

/// 2P-Set CRDT (two-phase set - add and remove)
#[derive(Clone, Serialize, Deserialize)]
pub struct TwoPSet<T: std::hash::Hash + Eq + Clone> {
    added: GSet<T>,
    removed: GSet<T>,
}

impl<T: std::hash::Hash + Eq + Clone> TwoPSet<T> {
    pub fn new() -> Self {
        Self {
            added: GSet::new(),
            removed: GSet::new(),
        }
    }

    pub fn add(&mut self, element: T) {
        self.added.add(element);
    }

    pub fn remove(&mut self, element: T) {
        if self.added.contains(&element) {
            self.removed.add(element);
        }
    }

    pub fn contains(&self, element: &T) -> bool {
        self.added.contains(element) && !self.removed.contains(element)
    }

    pub fn merge(&mut self, other: &TwoPSet<T>) {
        self.added.merge(&other.added);
        self.removed.merge(&other.removed);
    }

    pub fn elements(&self) -> Vec<T> {
        self.added.elements()
            .iter()
            .filter(|e| !self.removed.contains(e))
            .cloned()
            .collect()
    }
}

impl<T: std::hash::Hash + Eq + Clone> Default for TwoPSet<T> {
    fn default() -> Self { Self::new() }
}

/// LWW-Element-Set CRDT
#[derive(Clone, Serialize, Deserialize)]
pub struct LwwSet<T: std::hash::Hash + Eq + Clone> {
    add_set: HashMap<T, i64>,
    remove_set: HashMap<T, i64>,
}

impl<T: std::hash::Hash + Eq + Clone> LwwSet<T> {
    pub fn new() -> Self {
        Self {
            add_set: HashMap::new(),
            remove_set: HashMap::new(),
        }
    }

    pub fn add(&mut self, element: T) {
        let now = chrono::Utc::now().timestamp_millis();
        self.add_set.insert(element, now);
    }

    pub fn remove(&mut self, element: T) {
        let now = chrono::Utc::now().timestamp_millis();
        self.remove_set.insert(element, now);
    }

    pub fn contains(&self, element: &T) -> bool {
        let add_time = self.add_set.get(element).copied().unwrap_or(0);
        let remove_time = self.remove_set.get(element).copied().unwrap_or(0);
        add_time > remove_time
    }

    pub fn merge(&mut self, other: &LwwSet<T>) {
        for (elem, &time) in &other.add_set {
            let entry = self.add_set.entry(elem.clone()).or_insert(0);
            *entry = (*entry).max(time);
        }
        for (elem, &time) in &other.remove_set {
            let entry = self.remove_set.entry(elem.clone()).or_insert(0);
            *entry = (*entry).max(time);
        }
    }

    pub fn elements(&self) -> Vec<T> {
        self.add_set.keys()
            .filter(|e| self.contains(e))
            .cloned()
            .collect()
    }
}

impl<T: std::hash::Hash + Eq + Clone> Default for LwwSet<T> {
    fn default() -> Self { Self::new() }
}

/// Vector clock for causal ordering
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VectorClock {
    clocks: HashMap<String, u64>,
}

impl VectorClock {
    pub fn new() -> Self {
        Self { clocks: HashMap::new() }
    }

    pub fn increment(&mut self, node_id: &str) {
        *self.clocks.entry(node_id.to_string()).or_insert(0) += 1;
    }

    pub fn get(&self, node_id: &str) -> u64 {
        self.clocks.get(node_id).copied().unwrap_or(0)
    }

    pub fn merge(&mut self, other: &VectorClock) {
        for (node, &clock) in &other.clocks {
            let entry = self.clocks.entry(node.clone()).or_insert(0);
            *entry = (*entry).max(clock);
        }
    }

    pub fn happens_before(&self, other: &VectorClock) -> bool {
        let mut dominated = false;
        for (node, &clock) in &self.clocks {
            let other_clock = other.get(node);
            if clock > other_clock {
                return false;
            }
            if clock < other_clock {
                dominated = true;
            }
        }
        for (node, &_clock) in &other.clocks {
            if !self.clocks.contains_key(node) {
                dominated = true;
            }
        }
        dominated
    }

    pub fn concurrent(&self, other: &VectorClock) -> bool {
        !self.happens_before(other) && !other.happens_before(self)
    }
}

impl Default for VectorClock {
    fn default() -> Self { Self::new() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pn_counter() {
        let mut counter = PnCounter::new();
        counter.increment("node1");
        counter.increment("node1");
        counter.decrement("node1");
        assert_eq!(counter.value(), 1);
    }

    #[test]
    fn test_lww_set() {
        let mut set: LwwSet<String> = LwwSet::new();
        set.add("item1".to_string());
        set.add("item2".to_string());
        assert!(set.contains(&"item1".to_string()));
        
        set.remove("item1".to_string());
        assert!(!set.contains(&"item1".to_string()));
    }

    #[test]
    fn test_vector_clock() {
        let mut vc1 = VectorClock::new();
        let mut vc2 = VectorClock::new();

        vc1.increment("node1");
        vc2.increment("node2");

        assert!(vc1.concurrent(&vc2));
    }
}
