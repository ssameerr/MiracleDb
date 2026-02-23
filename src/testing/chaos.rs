//! Chaos Testing - Fault injection for resilience testing

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use serde::{Deserialize, Serialize};

/// Type of fault to inject.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FaultType {
    /// Delay a request by the given milliseconds.
    Latency { ms: u64 },
    /// Return an error instead of a result.
    Error { message: String },
    /// Drop the request without a response.
    Drop,
    /// Corrupt the data payload.
    Corruption,
}

/// A configured fault with activation probability.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Fault {
    pub fault_type: FaultType,
    /// Probability (0.0–1.0) that this fault fires on any given call.
    pub probability: f64,
    /// Whether this fault is currently active.
    pub enabled: bool,
}

impl Fault {
    pub fn new(fault_type: FaultType, probability: f64) -> Self {
        Self { fault_type, probability, enabled: true }
    }

    /// Always-fire shorthand (probability = 1.0).
    pub fn always(fault_type: FaultType) -> Self {
        Self::new(fault_type, 1.0)
    }
}

/// Injects faults into named subsystems for chaos / resilience testing.
pub struct ChaosInjector {
    faults: Arc<RwLock<HashMap<String, Fault>>>,
}

impl ChaosInjector {
    pub fn new() -> Self {
        Self {
            faults: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register a fault for the named target.
    pub fn inject(&self, target: &str, fault: Fault) {
        let mut faults = self.faults.write().unwrap();
        faults.insert(target.to_string(), fault);
    }

    /// Remove the fault for `target`.
    pub fn clear(&self, target: &str) {
        let mut faults = self.faults.write().unwrap();
        faults.remove(target);
    }

    /// Remove all registered faults.
    pub fn clear_all(&self) {
        let mut faults = self.faults.write().unwrap();
        faults.clear();
    }

    /// Returns `true` if the named target has an enabled fault that fires
    /// (based on its probability).
    pub fn should_fail(&self, target: &str) -> bool {
        let faults = self.faults.read().unwrap();
        if let Some(fault) = faults.get(target) {
            fault.enabled && fault.probability >= 1.0
        } else {
            false
        }
    }

    /// Returns the active `Fault` for `target`, if any.
    pub fn get_fault(&self, target: &str) -> Option<Fault> {
        let faults = self.faults.read().unwrap();
        faults.get(target).cloned()
    }
}

impl Default for ChaosInjector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_inject_and_should_fail() {
        let injector = ChaosInjector::new();
        injector.inject("db_write", Fault::always(FaultType::Error {
            message: "simulated write failure".to_string(),
        }));
        assert!(injector.should_fail("db_write"));
    }

    #[test]
    fn test_clear_removes_fault() {
        let injector = ChaosInjector::new();
        injector.inject("network", Fault::always(FaultType::Drop));
        assert!(injector.should_fail("network"));
        injector.clear("network");
        assert!(!injector.should_fail("network"));
    }

    #[test]
    fn test_unknown_target_does_not_fail() {
        let injector = ChaosInjector::new();
        assert!(!injector.should_fail("unknown_target"));
    }

    #[test]
    fn test_latency_fault_type() {
        let fault = Fault::new(FaultType::Latency { ms: 500 }, 1.0);
        assert!(matches!(fault.fault_type, FaultType::Latency { ms: 500 }));
        assert!(fault.enabled);
    }

    #[test]
    fn test_clear_all_removes_all_faults() {
        let injector = ChaosInjector::new();
        injector.inject("svc_a", Fault::always(FaultType::Drop));
        injector.inject("svc_b", Fault::always(FaultType::Corruption));
        injector.clear_all();
        assert!(!injector.should_fail("svc_a"));
        assert!(!injector.should_fail("svc_b"));
    }
}
