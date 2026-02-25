//! Multixact Module - Multi-transaction ID management

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

/// Multi-transaction member
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MultiXactMember {
    pub xid: u64,
    pub status: MemberStatus,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum MemberStatus {
    ForKeyShare,
    ForShare,
    ForNoKeyUpdate,
    ForUpdate,
    Update,
    NoKeyUpdate,
    Delete,
}

/// Multi-transaction entry
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MultiXact {
    pub multi_xact_id: u64,
    pub members: Vec<MultiXactMember>,
    pub created_at: i64,
}

impl MultiXact {
    pub fn new(multi_xact_id: u64, members: Vec<MultiXactMember>) -> Self {
        Self {
            multi_xact_id,
            members,
            created_at: chrono::Utc::now().timestamp(),
        }
    }

    /// Check if XID is a member
    pub fn is_member(&self, xid: u64) -> bool {
        self.members.iter().any(|m| m.xid == xid)
    }

    /// Get member status
    pub fn get_status(&self, xid: u64) -> Option<MemberStatus> {
        self.members.iter()
            .find(|m| m.xid == xid)
            .map(|m| m.status)
    }

    /// Add member
    pub fn add_member(&mut self, member: MultiXactMember) {
        if !self.is_member(member.xid) {
            self.members.push(member);
        }
    }
}

/// MultiXact manager
pub struct MultiXactManager {
    next_multi_xact_id: AtomicU64,
    oldest_multi_xact_id: AtomicU64,
    multixacts: RwLock<HashMap<u64, MultiXact>>,
    xid_to_multi: RwLock<HashMap<u64, Vec<u64>>>,
}

impl MultiXactManager {
    pub fn new() -> Self {
        Self {
            next_multi_xact_id: AtomicU64::new(1),
            oldest_multi_xact_id: AtomicU64::new(1),
            multixacts: RwLock::new(HashMap::new()),
            xid_to_multi: RwLock::new(HashMap::new()),
        }
    }

    /// Create a new MultiXact
    pub async fn create(&self, members: Vec<MultiXactMember>) -> u64 {
        let multi_id = self.next_multi_xact_id.fetch_add(1, Ordering::SeqCst);
        let multi = MultiXact::new(multi_id, members.clone());

        let mut multixacts = self.multixacts.write().await;
        let mut xid_to_multi = self.xid_to_multi.write().await;

        // Index by member XIDs
        for member in &members {
            xid_to_multi.entry(member.xid)
                .or_insert_with(Vec::new)
                .push(multi_id);
        }

        multixacts.insert(multi_id, multi);
        multi_id
    }

    /// Expand a MultiXact to include another XID
    pub async fn expand(&self, multi_id: u64, new_member: MultiXactMember) -> Result<u64, String> {
        let multixacts = self.multixacts.read().await;
        let existing = multixacts.get(&multi_id)
            .ok_or_else(|| format!("MultiXact {} not found", multi_id))?;

        let mut new_members = existing.members.clone();
        if !existing.is_member(new_member.xid) {
            new_members.push(new_member);
        }

        drop(multixacts);

        // Create new MultiXact with expanded members
        Ok(self.create(new_members).await)
    }

    /// Get a MultiXact
    pub async fn get(&self, multi_id: u64) -> Option<MultiXact> {
        let multixacts = self.multixacts.read().await;
        multixacts.get(&multi_id).cloned()
    }

    /// Get members
    pub async fn get_members(&self, multi_id: u64) -> Vec<MultiXactMember> {
        let multixacts = self.multixacts.read().await;
        multixacts.get(&multi_id)
            .map(|m| m.members.clone())
            .unwrap_or_default()
    }

    /// Check if member
    pub async fn is_member(&self, multi_id: u64, xid: u64) -> bool {
        let multixacts = self.multixacts.read().await;
        multixacts.get(&multi_id)
            .map(|m| m.is_member(xid))
            .unwrap_or(false)
    }

    /// Get MultiXacts containing an XID
    pub async fn get_for_xid(&self, xid: u64) -> Vec<u64> {
        let xid_to_multi = self.xid_to_multi.read().await;
        xid_to_multi.get(&xid).cloned().unwrap_or_default()
    }

    /// Truncate old MultiXacts
    pub async fn truncate(&self, oldest_multi_id: u64) {
        let mut multixacts = self.multixacts.write().await;
        let mut xid_to_multi = self.xid_to_multi.write().await;

        // Remove old multixacts
        let to_remove: Vec<u64> = multixacts.keys()
            .filter(|&&id| id < oldest_multi_id)
            .copied()
            .collect();

        for multi_id in to_remove {
            if let Some(multi) = multixacts.remove(&multi_id) {
                for member in &multi.members {
                    if let Some(list) = xid_to_multi.get_mut(&member.xid) {
                        list.retain(|&id| id != multi_id);
                    }
                }
            }
        }

        self.oldest_multi_xact_id.store(oldest_multi_id, Ordering::SeqCst);
    }

    /// Get next MultiXact ID
    pub fn get_next_id(&self) -> u64 {
        self.next_multi_xact_id.load(Ordering::SeqCst)
    }

    /// Get oldest MultiXact ID
    pub fn get_oldest_id(&self) -> u64 {
        self.oldest_multi_xact_id.load(Ordering::SeqCst)
    }
}

impl Default for MultiXactManager {
    fn default() -> Self {
        Self::new()
    }
}
