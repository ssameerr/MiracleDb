//! Merkle Tree Audit Trail with Dilithium Signatures
//!
//! Provides tamper-evident audit logging with:
//! - Merkle tree structure for efficient proofs
//! - Dilithium-3 signed roots for non-repudiation
//! - O(log n) membership proofs

use std::collections::BTreeMap;
use serde::{Serialize, Deserialize};
use chrono::{DateTime, Utc};
use uuid::Uuid;
use std::net::IpAddr;

use crate::security::pqc::PqcProvider;

/// Tree position (level, index)
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TreePosition {
    pub level: u32,
    pub index: u64,
}

impl TreePosition {
    pub fn new(level: u32, index: u64) -> Self {
        Self { level, index }
    }

    pub fn leaf(index: u64) -> Self {
        Self { level: 0, index }
    }

    pub fn parent(&self) -> Self {
        Self {
            level: self.level + 1,
            index: self.index / 2,
        }
    }

    pub fn sibling(&self) -> Self {
        Self {
            level: self.level,
            index: self.index ^ 1,
        }
    }
}

/// Actor who performed an action
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AuditActor {
    pub user_id: Option<String>,
    pub service_id: Option<String>,
    pub device_hash: [u8; 32],
    pub ip_address: Option<IpAddr>,
    pub geo_location: Option<String>,
}

/// Audit action types
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum AuditAction {
    // Authentication
    Login { method: String },
    Logout,
    TokenRefresh,
    TokenRevoke { token_id: Uuid },

    // Data operations
    Read { table: String, row_count: u64 },
    Write { table: String, row_count: u64 },
    Delete { table: String, row_count: u64 },

    // Key operations
    KeyGenerate { key_type: String },
    KeyRotate { key_id: Uuid },
    KeyRevoke { key_id: Uuid },

    // Admin operations
    PermissionGrant { role: String, target: String },
    PermissionRevoke { role: String, target: String },
    ConfigChange { setting: String, old_value: String, new_value: String },

    // Security events
    SuspiciousActivity { reason: String },
    RateLimitExceeded { endpoint: String },
    AuthenticationFailed { attempts: u32 },

    // Custom
    Custom { action_type: String, details: String },
}

/// Audit entry for the Merkle tree
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MerkleAuditEntry {
    /// Unique entry ID
    pub id: Uuid,
    /// Timestamp
    pub timestamp: DateTime<Utc>,
    /// Who performed the action
    pub actor: AuditActor,
    /// What action was performed
    pub action: AuditAction,
    /// Resource affected
    pub resource: String,
    /// Action result (success/failure)
    pub success: bool,
    /// Error message if failed
    pub error: Option<String>,
    /// Hash of previous entry (chain linkage)
    pub prev_hash: [u8; 32],
    /// This entry's hash
    pub entry_hash: [u8; 32],
}

impl MerkleAuditEntry {
    pub fn new(actor: AuditActor, action: AuditAction, resource: &str, success: bool) -> Self {
        Self {
            id: Uuid::new_v4(),
            timestamp: Utc::now(),
            actor,
            action,
            resource: resource.to_string(),
            success,
            error: None,
            prev_hash: [0u8; 32],
            entry_hash: [0u8; 32],
        }
    }

    pub fn with_error(mut self, error: &str) -> Self {
        self.error = Some(error.to_string());
        self
    }

    pub fn with_prev_hash(mut self, prev_hash: [u8; 32]) -> Self {
        self.prev_hash = prev_hash;
        self
    }

    /// Compute hash of this entry
    pub fn compute_hash(&mut self) {
        let data = bincode::serialize(self).unwrap_or_default();
        self.entry_hash = blake3::hash(&data).into();
    }
}

/// Proof element for Merkle proof
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProofElement {
    /// Sibling hash
    pub hash: [u8; 32],
    /// Position (left or right)
    pub position: ProofPosition,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum ProofPosition {
    Left,
    Right,
}

/// Merkle proof of inclusion
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MerkleProof {
    /// Entry index in the tree
    pub entry_index: u64,
    /// Path from leaf to root
    pub proof_hashes: Vec<ProofElement>,
    /// Root hash at time of proof
    pub root_hash: [u8; 32],
    /// Tree size at time of proof
    pub tree_size: u64,
}

/// Signed Merkle root
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SignedRoot {
    /// Root hash
    pub root_hash: [u8; 32],
    /// Tree size at signing
    pub tree_size: u64,
    /// Signing timestamp
    pub timestamp: DateTime<Utc>,
    /// Dilithium signature
    pub signature: Vec<u8>,
    /// Signer's public key fingerprint
    pub signer_fingerprint: [u8; 32],
}

/// Merkle audit error types
#[derive(Debug, Clone)]
pub enum MerkleAuditError {
    EmptyTree,
    InvalidProof,
    SignatureVerificationFailed,
    TreeCorrupted,
    EntryNotFound,
}

impl std::fmt::Display for MerkleAuditError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MerkleAuditError::EmptyTree => write!(f, "Tree is empty"),
            MerkleAuditError::InvalidProof => write!(f, "Invalid proof"),
            MerkleAuditError::SignatureVerificationFailed => write!(f, "Signature verification failed"),
            MerkleAuditError::TreeCorrupted => write!(f, "Tree data corrupted"),
            MerkleAuditError::EntryNotFound => write!(f, "Entry not found"),
        }
    }
}

impl std::error::Error for MerkleAuditError {}

/// Merkle Tree Audit Trail
pub struct MerkleAuditTree {
    /// Tree nodes (position -> hash)
    nodes: BTreeMap<TreePosition, [u8; 32]>,
    /// Stored entries
    entries: Vec<MerkleAuditEntry>,
    /// Current tree height
    height: u32,
    /// Signed roots (one per signing epoch)
    signed_roots: Vec<SignedRoot>,
    /// PQC provider for signing
    pqc: PqcProvider,
    /// Signing key (Dilithium secret key)
    signing_key: Option<Vec<u8>>,
    /// Verification key (Dilithium public key)
    verification_key: Option<Vec<u8>>,
}

impl MerkleAuditTree {
    pub fn new() -> Self {
        Self {
            nodes: BTreeMap::new(),
            entries: Vec::new(),
            height: 0,
            signed_roots: Vec::new(),
            pqc: PqcProvider::new(),
            signing_key: None,
            verification_key: None,
        }
    }

    /// Set signing keys
    pub fn with_signing_key(mut self, secret_key: Vec<u8>, public_key: Vec<u8>) -> Self {
        self.signing_key = Some(secret_key);
        self.verification_key = Some(public_key);
        self
    }

    /// Append a new audit entry
    pub fn append(&mut self, mut entry: MerkleAuditEntry) -> Result<MerkleProof, MerkleAuditError> {
        // Set previous hash
        if let Some(last) = self.entries.last() {
            entry.prev_hash = last.entry_hash;
        }

        // Compute entry hash
        entry.compute_hash();

        // Add to entries
        let leaf_index = self.entries.len() as u64;
        self.entries.push(entry.clone());

        // Add leaf node
        let leaf_pos = TreePosition::leaf(leaf_index);
        self.nodes.insert(leaf_pos, entry.entry_hash);

        // Update path to root
        self.update_path(leaf_index);

        // Update height if needed
        let new_height = (leaf_index + 1).next_power_of_two().trailing_zeros();
        if new_height > self.height {
            self.height = new_height;
        }

        // Generate proof
        self.generate_proof(leaf_index)
    }

    /// Update hashes from leaf to root
    fn update_path(&mut self, leaf_index: u64) {
        let mut current_pos = TreePosition::leaf(leaf_index);

        while current_pos.level < 32 {
            let parent_pos = current_pos.parent();
            let sibling_pos = current_pos.sibling();

            let current_hash = self.nodes.get(&current_pos).copied().unwrap_or([0u8; 32]);
            let sibling_hash = self.nodes.get(&sibling_pos).copied().unwrap_or([0u8; 32]);

            // Compute parent hash
            let parent_hash = if current_pos.index % 2 == 0 {
                self.hash_pair(&current_hash, &sibling_hash)
            } else {
                self.hash_pair(&sibling_hash, &current_hash)
            };

            self.nodes.insert(parent_pos, parent_hash);
            current_pos = parent_pos;
        }
    }

    /// Hash two nodes together
    fn hash_pair(&self, left: &[u8; 32], right: &[u8; 32]) -> [u8; 32] {
        let mut data = [0u8; 64];
        data[..32].copy_from_slice(left);
        data[32..].copy_from_slice(right);
        blake3::hash(&data).into()
    }

    /// Generate proof of inclusion for an entry
    pub fn generate_proof(&self, entry_index: u64) -> Result<MerkleProof, MerkleAuditError> {
        if entry_index >= self.entries.len() as u64 {
            return Err(MerkleAuditError::EntryNotFound);
        }

        let mut proof_hashes = Vec::new();
        let mut current_pos = TreePosition::leaf(entry_index);

        for _ in 0..self.height {
            let sibling_pos = current_pos.sibling();
            if let Some(&sibling_hash) = self.nodes.get(&sibling_pos) {
                let position = if current_pos.index % 2 == 0 {
                    ProofPosition::Right
                } else {
                    ProofPosition::Left
                };
                proof_hashes.push(ProofElement {
                    hash: sibling_hash,
                    position,
                });
            }
            current_pos = current_pos.parent();
        }

        Ok(MerkleProof {
            entry_index,
            proof_hashes,
            root_hash: self.get_root().unwrap_or([0u8; 32]),
            tree_size: self.entries.len() as u64,
        })
    }

    /// Get current root hash
    pub fn get_root(&self) -> Result<[u8; 32], MerkleAuditError> {
        if self.entries.is_empty() {
            return Err(MerkleAuditError::EmptyTree);
        }

        // Find the root at the highest level
        let root_pos = TreePosition::new(self.height, 0);
        self.nodes.get(&root_pos).copied().ok_or(MerkleAuditError::TreeCorrupted)
    }

    /// Verify a Merkle proof
    pub fn verify_proof(
        &self,
        entry_hash: &[u8; 32],
        proof: &MerkleProof,
    ) -> Result<bool, MerkleAuditError> {
        let mut current_hash = *entry_hash;

        for element in &proof.proof_hashes {
            current_hash = match element.position {
                ProofPosition::Left => self.hash_pair(&element.hash, &current_hash),
                ProofPosition::Right => self.hash_pair(&current_hash, &element.hash),
            };
        }

        Ok(current_hash == proof.root_hash)
    }

    /// Sign the current root
    pub fn sign_root(&mut self) -> Result<SignedRoot, MerkleAuditError> {
        let signing_key = self.signing_key.as_ref()
            .ok_or(MerkleAuditError::SignatureVerificationFailed)?;

        let root_hash = self.get_root()?;
        let timestamp = Utc::now();
        let tree_size = self.entries.len() as u64;

        // Create data to sign
        let mut sign_data = Vec::new();
        sign_data.extend_from_slice(&root_hash);
        sign_data.extend_from_slice(&tree_size.to_le_bytes());
        sign_data.extend_from_slice(timestamp.to_rfc3339().as_bytes());

        // Sign with Dilithium
        let signature = self.pqc.dilithium_sign(&sign_data, signing_key)
            .map_err(|_| MerkleAuditError::SignatureVerificationFailed)?;

        // Compute signer fingerprint
        let verification_key = self.verification_key.as_ref()
            .ok_or(MerkleAuditError::SignatureVerificationFailed)?;
        let signer_fingerprint: [u8; 32] = blake3::hash(verification_key).into();

        let signed_root = SignedRoot {
            root_hash,
            tree_size,
            timestamp,
            signature,
            signer_fingerprint,
        };

        self.signed_roots.push(signed_root.clone());

        Ok(signed_root)
    }

    /// Verify a signed root
    pub fn verify_signed_root(&self, signed_root: &SignedRoot) -> Result<bool, MerkleAuditError> {
        let verification_key = self.verification_key.as_ref()
            .ok_or(MerkleAuditError::SignatureVerificationFailed)?;

        // Recreate signed data
        let mut sign_data = Vec::new();
        sign_data.extend_from_slice(&signed_root.root_hash);
        sign_data.extend_from_slice(&signed_root.tree_size.to_le_bytes());
        sign_data.extend_from_slice(signed_root.timestamp.to_rfc3339().as_bytes());

        // Verify Dilithium signature
        self.pqc.dilithium_verify(&sign_data, &signed_root.signature, verification_key)
            .map_err(|_| MerkleAuditError::SignatureVerificationFailed)
    }

    /// Get entry by index
    pub fn get_entry(&self, index: u64) -> Option<&MerkleAuditEntry> {
        self.entries.get(index as usize)
    }

    /// Get entry count
    pub fn entry_count(&self) -> u64 {
        self.entries.len() as u64
    }

    /// Get all signed roots
    pub fn get_signed_roots(&self) -> &[SignedRoot] {
        &self.signed_roots
    }

    /// Query entries by actor
    pub fn query_by_actor(&self, user_id: &str) -> Vec<&MerkleAuditEntry> {
        self.entries.iter()
            .filter(|e| e.actor.user_id.as_deref() == Some(user_id))
            .collect()
    }

    /// Query entries in time range
    pub fn query_by_time_range(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Vec<&MerkleAuditEntry> {
        self.entries.iter()
            .filter(|e| e.timestamp >= start && e.timestamp <= end)
            .collect()
    }
}

impl Default for MerkleAuditTree {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_entry() -> MerkleAuditEntry {
        MerkleAuditEntry::new(
            AuditActor {
                user_id: Some("user123".to_string()),
                service_id: None,
                device_hash: [0u8; 32],
                ip_address: None,
                geo_location: None,
            },
            AuditAction::Login { method: "password".to_string() },
            "auth",
            true,
        )
    }

    #[test]
    fn test_append_and_proof() {
        let mut tree = MerkleAuditTree::new();

        let entry = create_test_entry();
        let proof = tree.append(entry).unwrap();

        assert_eq!(proof.entry_index, 0);
        assert_eq!(proof.tree_size, 1);
    }

    #[test]
    fn test_multiple_entries() {
        let mut tree = MerkleAuditTree::new();

        for i in 0..10 {
            let entry = create_test_entry();
            let proof = tree.append(entry).unwrap();
            assert_eq!(proof.entry_index, i);
        }

        assert_eq!(tree.entry_count(), 10);
    }

    #[test]
    fn test_proof_verification() {
        let mut tree = MerkleAuditTree::new();

        let entry = create_test_entry();
        let proof = tree.append(entry.clone()).unwrap();

        // Get the stored entry hash
        let stored_entry = tree.get_entry(0).unwrap();

        let valid = tree.verify_proof(&stored_entry.entry_hash, &proof).unwrap();
        assert!(valid);
    }

    #[test]
    fn test_query_by_actor() {
        let mut tree = MerkleAuditTree::new();

        for _ in 0..5 {
            let entry = create_test_entry();
            tree.append(entry).unwrap();
        }

        let results = tree.query_by_actor("user123");
        assert_eq!(results.len(), 5);
    }
}
