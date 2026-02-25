//! Nucleus Module - Quantum-Inspired Reactive Data System
//!
//! Implements reactive data atoms with subscriptions, computed values, and triggers

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{RwLock, broadcast};
use slotmap::{SlotMap, new_key_type};
use serde::{Serialize, Deserialize};

new_key_type! { pub struct AtomKey; }

/// Atom: Core reactive data unit (Nucleus + Electrons + Spin)
#[derive(Clone, Serialize, Deserialize)]
pub struct Atom {
    pub id: String,
    pub nucleus: serde_json::Value,     // Core data
    pub electrons: Vec<String>,          // Related atom IDs
    pub spin: f64,                        // Real-time state (sentiment, price, etc.)
    pub version: u64,
    pub last_updated: i64,
    pub metadata: HashMap<String, String>,
}

/// Change event for reactive subscriptions
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AtomChange {
    pub atom_id: String,
    pub change_type: ChangeType,
    pub old_value: Option<serde_json::Value>,
    pub new_value: serde_json::Value,
    pub timestamp: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ChangeType {
    Created,
    Updated,
    SpinChanged,
    ElectronAdded,
    ElectronRemoved,
    Deleted,
}

/// Computed atom definition
#[derive(Clone)]
pub struct ComputedAtom {
    pub id: String,
    pub dependencies: Vec<String>,
    pub compute_fn: Arc<dyn Fn(&[Atom]) -> serde_json::Value + Send + Sync>,
}

/// Trigger definition
#[derive(Clone)]
pub struct Trigger {
    pub id: String,
    pub condition: TriggerCondition,
    pub action: Arc<dyn Fn(&Atom, &AtomChange) + Send + Sync>,
}

#[derive(Clone, Debug)]
pub enum TriggerCondition {
    SpinAbove(f64),
    SpinBelow(f64),
    AnyChange,
    FieldChanged(String),
}

/// Nucleus system for reactive data
pub struct NucleusSystem {
    atoms: RwLock<SlotMap<AtomKey, Atom>>,
    id_map: RwLock<HashMap<String, AtomKey>>,
    change_tx: broadcast::Sender<AtomChange>,
    subscriptions: RwLock<HashMap<String, HashSet<String>>>, // atom_id -> subscriber_ids
    computed: RwLock<HashMap<String, ComputedAtom>>,
    triggers: RwLock<Vec<Trigger>>,
}

impl NucleusSystem {
    pub fn new() -> Self {
        let (change_tx, _) = broadcast::channel(10000);
        Self {
            atoms: RwLock::new(SlotMap::with_key()),
            id_map: RwLock::new(HashMap::new()),
            change_tx,
            subscriptions: RwLock::new(HashMap::new()),
            computed: RwLock::new(HashMap::new()),
            triggers: RwLock::new(Vec::new()),
        }
    }

    /// Create a new atom
    pub async fn create_atom(&self, id: &str, nucleus: serde_json::Value) -> AtomKey {
        let atom = Atom {
            id: id.to_string(),
            nucleus: nucleus.clone(),
            electrons: vec![],
            spin: 0.0,
            version: 1,
            last_updated: chrono::Utc::now().timestamp(),
            metadata: HashMap::new(),
        };

        let mut atoms = self.atoms.write().await;
        let mut id_map = self.id_map.write().await;
        let key = atoms.insert(atom);
        id_map.insert(id.to_string(), key);

        // Emit change event
        let _ = self.change_tx.send(AtomChange {
            atom_id: id.to_string(),
            change_type: ChangeType::Created,
            old_value: None,
            new_value: nucleus,
            timestamp: chrono::Utc::now().timestamp(),
        });

        key
    }

    /// Update atom nucleus
    pub async fn update_nucleus(&self, id: &str, nucleus: serde_json::Value) -> bool {
        let id_map = self.id_map.read().await;
        if let Some(&key) = id_map.get(id) {
            drop(id_map);
            let mut atoms = self.atoms.write().await;
            if let Some(atom) = atoms.get_mut(key) {
                let old_value = atom.nucleus.clone();
                atom.nucleus = nucleus.clone();
                atom.version += 1;
                atom.last_updated = chrono::Utc::now().timestamp();

                let change = AtomChange {
                    atom_id: id.to_string(),
                    change_type: ChangeType::Updated,
                    old_value: Some(old_value),
                    new_value: nucleus,
                    timestamp: atom.last_updated,
                };
                
                drop(atoms);
                let _ = self.change_tx.send(change.clone());
                self.check_triggers(id, &change).await;
                self.update_computed_atoms(id).await;
                
                return true;
            }
        }
        false
    }

    /// Update spin value
    pub async fn update_spin(&self, id: &str, spin: f64) -> bool {
        let id_map = self.id_map.read().await;
        if let Some(&key) = id_map.get(id) {
            drop(id_map);
            let mut atoms = self.atoms.write().await;
            if let Some(atom) = atoms.get_mut(key) {
                let old_spin = atom.spin;
                atom.spin = spin;
                atom.last_updated = chrono::Utc::now().timestamp();

                let change = AtomChange {
                    atom_id: id.to_string(),
                    change_type: ChangeType::SpinChanged,
                    old_value: Some(serde_json::json!(old_spin)),
                    new_value: serde_json::json!(spin),
                    timestamp: atom.last_updated,
                };

                drop(atoms);
                let _ = self.change_tx.send(change.clone());
                self.check_triggers(id, &change).await;
                
                return true;
            }
        }
        false
    }

    /// Add electron (relationship)
    pub async fn add_electron(&self, nucleus_id: &str, electron_id: &str) -> bool {
        let id_map = self.id_map.read().await;
        if let Some(&key) = id_map.get(nucleus_id) {
            drop(id_map);
            let mut atoms = self.atoms.write().await;
            if let Some(atom) = atoms.get_mut(key) {
                if !atom.electrons.contains(&electron_id.to_string()) {
                    atom.electrons.push(electron_id.to_string());
                    
                    let _ = self.change_tx.send(AtomChange {
                        atom_id: nucleus_id.to_string(),
                        change_type: ChangeType::ElectronAdded,
                        old_value: None,
                        new_value: serde_json::json!(electron_id),
                        timestamp: chrono::Utc::now().timestamp(),
                    });
                }
                return true;
            }
        }
        false
    }

    /// Subscribe to changes
    pub fn subscribe(&self) -> broadcast::Receiver<AtomChange> {
        self.change_tx.subscribe()
    }

    /// Register computed atom
    pub async fn register_computed<F>(&self, id: &str, dependencies: Vec<String>, compute_fn: F)
    where
        F: Fn(&[Atom]) -> serde_json::Value + Send + Sync + 'static,
    {
        let computed = ComputedAtom {
            id: id.to_string(),
            dependencies,
            compute_fn: Arc::new(compute_fn),
        };

        let mut computeds = self.computed.write().await;
        computeds.insert(id.to_string(), computed);
    }

    /// Get computed value
    pub async fn get_computed(&self, id: &str) -> Option<serde_json::Value> {
        let computeds = self.computed.read().await;
        let computed = computeds.get(id)?;
        
        let atoms = self.atoms.read().await;
        let id_map = self.id_map.read().await;
        
        let dep_atoms: Vec<Atom> = computed.dependencies.iter()
            .filter_map(|dep_id| {
                id_map.get(dep_id).and_then(|&key| atoms.get(key).cloned())
            })
            .collect();

        Some((computed.compute_fn)(&dep_atoms))
    }

    /// Update computed atoms when dependencies change
    async fn update_computed_atoms(&self, changed_id: &str) {
        let computeds = self.computed.read().await;
        for (id, computed) in computeds.iter() {
            if computed.dependencies.contains(&changed_id.to_string()) {
                // Recompute and notify
                if let Some(value) = self.get_computed(id).await {
                    let _ = self.change_tx.send(AtomChange {
                        atom_id: id.clone(),
                        change_type: ChangeType::Updated,
                        old_value: None,
                        new_value: value,
                        timestamp: chrono::Utc::now().timestamp(),
                    });
                }
            }
        }
    }

    /// Register trigger
    pub async fn register_trigger<F>(&self, id: &str, condition: TriggerCondition, action: F)
    where
        F: Fn(&Atom, &AtomChange) + Send + Sync + 'static,
    {
        let trigger = Trigger {
            id: id.to_string(),
            condition,
            action: Arc::new(action),
        };

        let mut triggers = self.triggers.write().await;
        triggers.push(trigger);
    }

    /// Check and fire triggers
    async fn check_triggers(&self, atom_id: &str, change: &AtomChange) {
        let atom = self.get_atom(atom_id).await;
        let atom = match atom {
            Some(a) => a,
            None => return,
        };

        let triggers = self.triggers.read().await;
        for trigger in triggers.iter() {
            let should_fire = match &trigger.condition {
                TriggerCondition::SpinAbove(threshold) => atom.spin > *threshold,
                TriggerCondition::SpinBelow(threshold) => atom.spin < *threshold,
                TriggerCondition::AnyChange => true,
                TriggerCondition::FieldChanged(_) => matches!(change.change_type, ChangeType::Updated),
            };

            if should_fire {
                (trigger.action)(&atom, change);
            }
        }
    }

    /// Get atom by ID
    pub async fn get_atom(&self, id: &str) -> Option<Atom> {
        let id_map = self.id_map.read().await;
        if let Some(&key) = id_map.get(id) {
            let atoms = self.atoms.read().await;
            return atoms.get(key).cloned();
        }
        None
    }

    /// Get atoms with high spin
    pub async fn get_high_spin(&self, threshold: f64) -> Vec<Atom> {
        let atoms = self.atoms.read().await;
        atoms.values().filter(|a| a.spin.abs() >= threshold).cloned().collect()
    }

    /// Get topology as graph
    pub async fn get_topology(&self) -> serde_json::Value {
        let atoms = self.atoms.read().await;
        let nodes: Vec<_> = atoms.values().map(|a| serde_json::json!({
            "id": a.id, "spin": a.spin, "version": a.version
        })).collect();
        let edges: Vec<_> = atoms.values().flat_map(|a| {
            a.electrons.iter().map(|e| serde_json::json!({"from": a.id, "to": e})).collect::<Vec<_>>()
        }).collect();
        serde_json::json!({"nodes": nodes, "edges": edges})
    }

    /// Delete atom
    pub async fn delete_atom(&self, id: &str) -> bool {
        let mut id_map = self.id_map.write().await;
        if let Some(key) = id_map.remove(id) {
            let mut atoms = self.atoms.write().await;
            if atoms.remove(key).is_some() {
                let _ = self.change_tx.send(AtomChange {
                    atom_id: id.to_string(),
                    change_type: ChangeType::Deleted,
                    old_value: None,
                    new_value: serde_json::Value::Null,
                    timestamp: chrono::Utc::now().timestamp(),
                });
                return true;
            }
        }
        false
    }

    /// Count atoms
    pub async fn count(&self) -> usize {
        let atoms = self.atoms.read().await;
        atoms.len()
    }

    /// List all atoms
    pub async fn list_atoms(&self) -> Vec<Atom> {
        let atoms = self.atoms.read().await;
        atoms.values().cloned().collect()
    }

    /// Add a trigger
    pub async fn add_trigger<F>(&self, id: &str, condition: TriggerCondition, action: F)
    where
        F: Fn(&Atom, &AtomChange) + Send + Sync + 'static,
    {
        let trigger = Trigger {
            id: id.to_string(),
            condition,
            action: Arc::new(action),
        };
        self.triggers.write().await.push(trigger);
    }

    /// Remove a trigger
    pub async fn remove_trigger(&self, id: &str) {
        let mut triggers = self.triggers.write().await;
        triggers.retain(|t| t.id != id);
    }

    /// List triggers
    pub async fn list_triggers(&self) -> Vec<(String, TriggerCondition)> {
        let triggers = self.triggers.read().await;
        triggers.iter().map(|t| (t.id.clone(), t.condition.clone())).collect()
    }

    /// Remove electron
    pub async fn remove_electron(&self, nucleus_id: &str, electron_id: &str) -> bool {
        let id_map = self.id_map.read().await;
        if let Some(&key) = id_map.get(nucleus_id) {
            drop(id_map);
            let mut atoms = self.atoms.write().await;
            if let Some(atom) = atoms.get_mut(key) {
                atom.electrons.retain(|e| e != electron_id);
                let _ = self.change_tx.send(AtomChange {
                    atom_id: nucleus_id.to_string(),
                    change_type: ChangeType::ElectronRemoved,
                    old_value: Some(serde_json::json!(electron_id)),
                    new_value: serde_json::Value::Null,
                    timestamp: chrono::Utc::now().timestamp(),
                });
                return true;
            }
        }
        false
    }

    /// List computed atoms
    pub async fn list_computed(&self) -> Vec<String> {
        let computed = self.computed.read().await;
        computed.keys().cloned().collect()
    }
}

impl Default for NucleusSystem {
    fn default() -> Self { Self::new() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_nucleus_system() {
        let system = NucleusSystem::new();
        
        system.create_atom("atom1", serde_json::json!({"value": 100})).await;
        system.update_spin("atom1", 0.75).await;
        
        let atom = system.get_atom("atom1").await.unwrap();
        assert_eq!(atom.spin, 0.75);
    }

    #[tokio::test]
    async fn test_subscriptions() {
        let system = NucleusSystem::new();
        let mut rx = system.subscribe();
        
        system.create_atom("atom1", serde_json::json!({})).await;
        
        let change = rx.recv().await.unwrap();
        assert!(matches!(change.change_type, ChangeType::Created));
    }
}
