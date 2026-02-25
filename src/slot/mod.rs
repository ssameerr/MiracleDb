//! Slot Module - Replication slots

use std::collections::HashMap;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

/// Replication slot type
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum SlotType {
    Physical,
    Logical,
}

/// Replication slot
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplicationSlot {
    pub name: String,
    pub slot_type: SlotType,
    pub plugin: Option<String>,
    pub database: Option<String>,
    pub temporary: bool,
    pub active: bool,
    pub restart_lsn: Option<String>,
    pub confirmed_flush_lsn: Option<String>,
    pub wal_status: WalStatus,
    pub safe_wal_size: Option<u64>,
    pub two_phase: bool,
    pub created_at: i64,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub enum WalStatus {
    #[default]
    Reserved,
    Extended,
    Unreserved,
    Lost,
}

impl ReplicationSlot {
    pub fn physical(name: &str) -> Self {
        Self {
            name: name.to_string(),
            slot_type: SlotType::Physical,
            plugin: None,
            database: None,
            temporary: false,
            active: false,
            restart_lsn: None,
            confirmed_flush_lsn: None,
            wal_status: WalStatus::default(),
            safe_wal_size: None,
            two_phase: false,
            created_at: chrono::Utc::now().timestamp(),
        }
    }

    pub fn logical(name: &str, plugin: &str, database: &str) -> Self {
        Self {
            name: name.to_string(),
            slot_type: SlotType::Logical,
            plugin: Some(plugin.to_string()),
            database: Some(database.to_string()),
            temporary: false,
            active: false,
            restart_lsn: None,
            confirmed_flush_lsn: None,
            wal_status: WalStatus::default(),
            safe_wal_size: None,
            two_phase: false,
            created_at: chrono::Utc::now().timestamp(),
        }
    }

    pub fn temporary(mut self) -> Self {
        self.temporary = true;
        self
    }

    pub fn with_two_phase(mut self) -> Self {
        self.two_phase = true;
        self
    }
}

/// Slot statistics
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SlotStats {
    pub spill_txns: u64,
    pub spill_count: u64,
    pub spill_bytes: u64,
    pub stream_txns: u64,
    pub stream_count: u64,
    pub stream_bytes: u64,
    pub total_txns: u64,
    pub total_bytes: u64,
}

/// Replication slot manager
pub struct SlotManager {
    slots: RwLock<HashMap<String, ReplicationSlot>>,
    stats: RwLock<HashMap<String, SlotStats>>,
}

impl SlotManager {
    pub fn new() -> Self {
        Self {
            slots: RwLock::new(HashMap::new()),
            stats: RwLock::new(HashMap::new()),
        }
    }

    /// Create a replication slot
    pub async fn create(&self, slot: ReplicationSlot) -> Result<(), String> {
        let mut slots = self.slots.write().await;

        if slots.contains_key(&slot.name) {
            return Err(format!("Slot {} already exists", slot.name));
        }

        slots.insert(slot.name.clone(), slot);
        Ok(())
    }

    /// Drop a replication slot
    pub async fn drop(&self, name: &str) -> Result<(), String> {
        let mut slots = self.slots.write().await;
        let mut stats = self.stats.write().await;

        let slot = slots.get(name)
            .ok_or_else(|| format!("Slot {} not found", name))?;

        if slot.active {
            return Err(format!("Slot {} is active", name));
        }

        slots.remove(name);
        stats.remove(name);
        Ok(())
    }

    /// Get a slot
    pub async fn get(&self, name: &str) -> Option<ReplicationSlot> {
        let slots = self.slots.read().await;
        slots.get(name).cloned()
    }

    /// List slots
    pub async fn list(&self, slot_type: Option<SlotType>) -> Vec<ReplicationSlot> {
        let slots = self.slots.read().await;
        slots.values()
            .filter(|s| slot_type.map(|t| s.slot_type == t).unwrap_or(true))
            .cloned()
            .collect()
    }

    /// Activate slot
    pub async fn activate(&self, name: &str) -> Result<(), String> {
        let mut slots = self.slots.write().await;

        let slot = slots.get_mut(name)
            .ok_or_else(|| format!("Slot {} not found", name))?;

        if slot.active {
            return Err(format!("Slot {} is already active", name));
        }

        slot.active = true;
        Ok(())
    }

    /// Deactivate slot
    pub async fn deactivate(&self, name: &str) {
        let mut slots = self.slots.write().await;
        if let Some(slot) = slots.get_mut(name) {
            slot.active = false;
        }
    }

    /// Advance slot LSN
    pub async fn advance(&self, name: &str, lsn: &str) -> Result<(), String> {
        let mut slots = self.slots.write().await;

        let slot = slots.get_mut(name)
            .ok_or_else(|| format!("Slot {} not found", name))?;

        slot.confirmed_flush_lsn = Some(lsn.to_string());
        Ok(())
    }

    /// Get statistics
    pub async fn get_stats(&self, name: &str) -> Option<SlotStats> {
        let stats = self.stats.read().await;
        stats.get(name).cloned()
    }

    /// Update statistics
    pub async fn update_stats(&self, name: &str, delta: SlotStats) {
        let mut stats = self.stats.write().await;
        let entry = stats.entry(name.to_string()).or_insert_with(SlotStats::default);
        entry.spill_txns += delta.spill_txns;
        entry.spill_bytes += delta.spill_bytes;
        entry.total_txns += delta.total_txns;
        entry.total_bytes += delta.total_bytes;
    }
}

impl Default for SlotManager {
    fn default() -> Self {
        Self::new()
    }
}
