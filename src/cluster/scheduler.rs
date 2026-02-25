//! Scheduler - Query distribution across executors
use super::ClusterNode;

pub struct Scheduler {
    strategy: SchedulingStrategy,
}

#[derive(Clone, Copy)]
pub enum SchedulingStrategy { RoundRobin, LeastLoaded, HashBased }

impl Scheduler {
    pub fn new(strategy: SchedulingStrategy) -> Self {
        Self { strategy }
    }

    pub fn select_executor<'a>(&self, executors: &'a [ClusterNode], _query_hash: u64) -> Option<&'a ClusterNode> {
        if executors.is_empty() { return None; }
        Some(&executors[0]) // Simplified
    }
}
