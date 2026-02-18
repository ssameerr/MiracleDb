//! Tamper-Evident Audit Log
//!
//! Implements a blockchain-style audit log where each entry is cryptographically
//! linked to the previous one via SHA-256 hashing. Any tampering with a past
//! entry breaks the chain and is immediately detectable via `verify_chain()`.

use sha2::{Sha256, Digest};
use serde::{Serialize, Deserialize};
use std::sync::{Arc, RwLock};
use chrono::{DateTime, Utc};

/// A single entry in the tamper-evident audit log.
///
/// Each entry stores a SHA-256 hash of the affected data and is linked to the
/// previous entry via `prev_hash`, forming an immutable chain.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEntry {
    /// Monotonically increasing sequence number (0-based).
    pub sequence: u64,
    /// UTC timestamp of when this entry was created.
    pub timestamp: DateTime<Utc>,
    /// Database operation type: INSERT, UPDATE, DELETE, SELECT, etc.
    pub operation: String,
    /// Name of the table affected by the operation.
    pub table: String,
    /// Identity of the user or service that performed the operation.
    pub user: String,
    /// SHA-256 digest of the affected data payload.
    pub data_hash: String,
    /// SHA-256 digest of the previous entry (`"genesis"` for the first entry).
    pub prev_hash: String,
    /// SHA-256 digest of this entry's own content (seals the record).
    pub entry_hash: String,
}

impl AuditEntry {
    /// Create a new `AuditEntry`, computing `data_hash` and `entry_hash`
    /// deterministically from the supplied arguments.
    ///
    /// # Arguments
    ///
    /// * `sequence`  - Position in the chain (0-based).
    /// * `operation` - SQL operation type (e.g. `"INSERT"`).
    /// * `table`     - Target table name.
    /// * `user`      - User or service identity.
    /// * `data`      - Raw data string whose hash will be stored.
    /// * `prev_hash` - `entry_hash` of the preceding entry, or `"genesis"`.
    pub fn new(
        sequence: u64,
        operation: &str,
        table: &str,
        user: &str,
        data: &str,
        prev_hash: &str,
    ) -> Self {
        let data_hash = format!("{:x}", Sha256::digest(data.as_bytes()));
        let content = format!("{sequence}{operation}{table}{user}{data_hash}{prev_hash}");
        let entry_hash = format!("{:x}", Sha256::digest(content.as_bytes()));
        Self {
            sequence,
            timestamp: Utc::now(),
            operation: operation.to_string(),
            table: table.to_string(),
            user: user.to_string(),
            data_hash,
            prev_hash: prev_hash.to_string(),
            entry_hash,
        }
    }
}

/// Thread-safe, append-only audit log with chain-integrity verification.
///
/// Entries are stored in memory and linked via SHA-256 hashes in the style of
/// a simplified blockchain.  The log is append-only by design; individual
/// entries cannot be modified once written.
///
/// # Example
///
/// ```rust
/// use miracledb::blockchain::audit_log::AuditLog;
///
/// let log = AuditLog::new();
/// log.append("INSERT", "users", "alice", r#"{"id":1,"name":"Alice"}"#);
/// log.append("SELECT", "users", "bob",   r#"{"id":1}"#);
///
/// assert!(log.verify_chain().is_ok());
/// ```
pub struct AuditLog {
    entries: Arc<RwLock<Vec<AuditEntry>>>,
}

impl AuditLog {
    /// Create an empty audit log.
    pub fn new() -> Self {
        Self {
            entries: Arc::new(RwLock::new(vec![])),
        }
    }

    /// Append a new entry to the log and return a clone of the stored entry.
    ///
    /// The previous entry's `entry_hash` (or `"genesis"` if the log is empty)
    /// is automatically woven into the new entry, maintaining the chain.
    pub fn append(
        &self,
        operation: &str,
        table: &str,
        user: &str,
        data: &str,
    ) -> AuditEntry {
        let mut entries = self.entries.write().unwrap();
        let sequence = entries.len() as u64;
        let prev_hash = entries
            .last()
            .map(|e| e.entry_hash.as_str())
            .unwrap_or("genesis");
        let entry = AuditEntry::new(sequence, operation, table, user, data, prev_hash);
        entries.push(entry.clone());
        entry
    }

    /// Walk every entry and verify that the chain of `prev_hash` links is
    /// intact.
    ///
    /// Returns `Ok(())` when the chain is unbroken, or an `Err` describing
    /// the first broken link detected.
    pub fn verify_chain(&self) -> Result<(), String> {
        let entries = self.entries.read().unwrap();
        for (i, entry) in entries.iter().enumerate() {
            let expected_prev = if i == 0 {
                "genesis".to_string()
            } else {
                entries[i - 1].entry_hash.clone()
            };
            if entry.prev_hash != expected_prev {
                return Err(format!(
                    "Chain broken at sequence {}: expected prev_hash '{}', got '{}'",
                    entry.sequence, expected_prev, entry.prev_hash
                ));
            }
        }
        Ok(())
    }

    /// Return all entries whose `table` field matches the given name.
    pub fn query_by_table(&self, table: &str) -> Vec<AuditEntry> {
        self.entries
            .read()
            .unwrap()
            .iter()
            .filter(|e| e.table == table)
            .cloned()
            .collect()
    }

    /// Return all entries whose `user` field matches the given identity.
    pub fn query_by_user(&self, user: &str) -> Vec<AuditEntry> {
        self.entries
            .read()
            .unwrap()
            .iter()
            .filter(|e| e.user == user)
            .cloned()
            .collect()
    }

    /// Return the total number of entries currently stored in the log.
    pub fn len(&self) -> usize {
        self.entries.read().unwrap().len()
    }

    /// Return `true` if the log contains no entries.
    pub fn is_empty(&self) -> bool {
        self.entries.read().unwrap().is_empty()
    }
}

impl Default for AuditLog {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Clone support for AuditLog (shares the underlying Arc)
// ---------------------------------------------------------------------------

impl Clone for AuditLog {
    fn clone(&self) -> Self {
        Self {
            entries: Arc::clone(&self.entries),
        }
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_append_increments_sequence() {
        let log = AuditLog::new();
        let e0 = log.append("INSERT", "users", "alice", r#"{"id":1}"#);
        let e1 = log.append("UPDATE", "users", "alice", r#"{"id":1,"name":"Alice"}"#);

        assert_eq!(e0.sequence, 0);
        assert_eq!(e1.sequence, 1);
        assert_eq!(log.len(), 2);
    }

    #[test]
    fn test_genesis_prev_hash() {
        let log = AuditLog::new();
        let first = log.append("INSERT", "orders", "bob", "data");
        assert_eq!(first.prev_hash, "genesis");
    }

    #[test]
    fn test_chaining() {
        let log = AuditLog::new();
        let e0 = log.append("INSERT", "t", "u", "d0");
        let e1 = log.append("INSERT", "t", "u", "d1");
        let e2 = log.append("DELETE", "t", "u", "d2");

        assert_eq!(e1.prev_hash, e0.entry_hash);
        assert_eq!(e2.prev_hash, e1.entry_hash);
    }

    #[test]
    fn test_verify_chain_intact() {
        let log = AuditLog::new();
        for i in 0..10 {
            log.append("SELECT", "products", "admin", &format!("row {i}"));
        }
        assert!(log.verify_chain().is_ok());
    }

    #[test]
    fn test_verify_chain_detects_tampering() {
        let log = AuditLog::new();
        log.append("INSERT", "accounts", "eve", "payload_a");
        log.append("UPDATE", "accounts", "eve", "payload_b");

        // Directly tamper with the first entry's hash by writing through the Arc.
        {
            let mut entries = log.entries.write().unwrap();
            entries[0].entry_hash = "tampered_hash_value".to_string();
        }

        // The second entry's prev_hash no longer matches.
        let result = log.verify_chain();
        assert!(result.is_err());
        let msg = result.unwrap_err();
        assert!(msg.contains("Chain broken at sequence 1"), "unexpected: {msg}");
    }

    #[test]
    fn test_query_by_table() {
        let log = AuditLog::new();
        log.append("INSERT", "users",    "alice", "u1");
        log.append("INSERT", "products", "alice", "p1");
        log.append("UPDATE", "users",    "alice", "u2");
        log.append("SELECT", "products", "bob",   "p2");

        let users = log.query_by_table("users");
        assert_eq!(users.len(), 2);
        assert!(users.iter().all(|e| e.table == "users"));

        let products = log.query_by_table("products");
        assert_eq!(products.len(), 2);
    }

    #[test]
    fn test_query_by_user() {
        let log = AuditLog::new();
        log.append("INSERT", "users", "alice", "d1");
        log.append("UPDATE", "users", "bob",   "d2");
        log.append("DELETE", "users", "alice", "d3");

        let alice_entries = log.query_by_user("alice");
        assert_eq!(alice_entries.len(), 2);
        assert!(alice_entries.iter().all(|e| e.user == "alice"));

        let bob_entries = log.query_by_user("bob");
        assert_eq!(bob_entries.len(), 1);
    }

    #[test]
    fn test_data_hash_is_deterministic() {
        // Same data must always produce the same data_hash.
        let e1 = AuditEntry::new(0, "INSERT", "t", "u", "hello", "genesis");
        let e2 = AuditEntry::new(0, "INSERT", "t", "u", "hello", "genesis");
        assert_eq!(e1.data_hash, e2.data_hash);
        assert_eq!(e1.entry_hash, e2.entry_hash);
    }

    #[test]
    fn test_different_data_produces_different_hashes() {
        let e1 = AuditEntry::new(0, "INSERT", "t", "u", "data_a", "genesis");
        let e2 = AuditEntry::new(0, "INSERT", "t", "u", "data_b", "genesis");
        assert_ne!(e1.data_hash, e2.data_hash);
        assert_ne!(e1.entry_hash, e2.entry_hash);
    }

    #[test]
    fn test_empty_log_verify_chain() {
        let log = AuditLog::new();
        assert!(log.verify_chain().is_ok());
        assert!(log.is_empty());
    }

    #[test]
    fn test_clone_shares_entries() {
        let log = AuditLog::new();
        log.append("INSERT", "t", "u", "d");

        let log2 = log.clone();
        log2.append("UPDATE", "t", "u", "d2");

        // Both handles see 2 entries because they share the Arc.
        assert_eq!(log.len(), 2);
        assert_eq!(log2.len(), 2);
        assert!(log.verify_chain().is_ok());
    }
}
