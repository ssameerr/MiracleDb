//! Blockchain Module - Immutable audit trails and hash chains

pub mod audit_log;

use std::collections::HashMap;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};
use sha2::{Sha256, Digest};

/// Block in the chain
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Block {
    pub index: u64,
    pub timestamp: i64,
    pub transactions: Vec<Transaction>,
    pub previous_hash: String,
    pub hash: String,
    pub nonce: u64,
}

/// Transaction record
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Transaction {
    pub id: String,
    pub operation: Operation,
    pub table: String,
    pub data_hash: String,
    pub user_id: String,
    pub timestamp: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Operation {
    Insert,
    Update,
    Delete,
}

/// Merkle tree node
#[derive(Clone, Debug)]
pub struct MerkleNode {
    pub hash: String,
    pub left: Option<Box<MerkleNode>>,
    pub right: Option<Box<MerkleNode>>,
}

impl MerkleNode {
    pub fn leaf(data: &[u8]) -> Self {
        Self {
            hash: hex::encode(Sha256::digest(data)),
            left: None,
            right: None,
        }
    }

    pub fn branch(left: MerkleNode, right: MerkleNode) -> Self {
        let combined = format!("{}{}", left.hash, right.hash);
        Self {
            hash: hex::encode(Sha256::digest(combined.as_bytes())),
            left: Some(Box::new(left)),
            right: Some(Box::new(right)),
        }
    }
}

/// Build Merkle tree from data
pub fn build_merkle_tree(data: &[Vec<u8>]) -> Option<MerkleNode> {
    if data.is_empty() {
        return None;
    }

    let mut nodes: Vec<MerkleNode> = data.iter()
        .map(|d| MerkleNode::leaf(d))
        .collect();

    while nodes.len() > 1 {
        let mut new_level = Vec::new();
        
        for chunk in nodes.chunks(2) {
            if chunk.len() == 2 {
                new_level.push(MerkleNode::branch(chunk[0].clone(), chunk[1].clone()));
            } else {
                new_level.push(chunk[0].clone());
            }
        }
        
        nodes = new_level;
    }

    nodes.into_iter().next()
}

/// Blockchain for immutable audit trail
pub struct Blockchain {
    chain: RwLock<Vec<Block>>,
    pending_transactions: RwLock<Vec<Transaction>>,
    difficulty: u32,
}

impl Blockchain {
    pub fn new(difficulty: u32) -> Self {
        // Create genesis block
        let genesis = Block {
            index: 0,
            timestamp: chrono::Utc::now().timestamp(),
            transactions: vec![],
            previous_hash: "0".repeat(64),
            hash: "0".repeat(64),
            nonce: 0,
        };
        
        Self {
            chain: RwLock::new(vec![genesis]),
            pending_transactions: RwLock::new(Vec::new()),
            difficulty,
        }
    }

    /// Calculate block hash
    fn calculate_hash(block: &Block) -> String {
        let data = format!(
            "{}{}{}{}{}",
            block.index,
            block.timestamp,
            serde_json::to_string(&block.transactions).unwrap_or_default(),
            block.previous_hash,
            block.nonce
        );
        hex::encode(Sha256::digest(data.as_bytes()))
    }

    /// Add transaction to pending pool
    pub async fn add_transaction(&self, tx: Transaction) {
        let mut pending = self.pending_transactions.write().await;
        pending.push(tx);
    }

    /// Mine a new block
    pub async fn mine_block(&self) -> Result<Block, String> {
        let mut pending = self.pending_transactions.write().await;
        if pending.is_empty() {
            return Err("No pending transactions".to_string());
        }

        let transactions = std::mem::take(&mut *pending);
        drop(pending);

        let chain = self.chain.read().await;
        let last_block = chain.last()
            .ok_or("Chain is empty")?;

        let mut new_block = Block {
            index: last_block.index + 1,
            timestamp: chrono::Utc::now().timestamp(),
            transactions,
            previous_hash: last_block.hash.clone(),
            hash: String::new(),
            nonce: 0,
        };
        drop(chain);

        // Proof of work (simplified)
        let target = "0".repeat(self.difficulty as usize);
        loop {
            new_block.hash = Self::calculate_hash(&new_block);
            if new_block.hash.starts_with(&target) {
                break;
            }
            new_block.nonce += 1;
        }

        let mut chain = self.chain.write().await;
        chain.push(new_block.clone());

        Ok(new_block)
    }

    /// Verify chain integrity
    pub async fn verify_chain(&self) -> bool {
        let chain = self.chain.read().await;
        
        for i in 1..chain.len() {
            let current = &chain[i];
            let previous = &chain[i - 1];

            // Verify hash
            if current.hash != Self::calculate_hash(current) {
                return false;
            }

            // Verify link
            if current.previous_hash != previous.hash {
                return false;
            }
        }

        true
    }

    /// Get block by index
    pub async fn get_block(&self, index: u64) -> Option<Block> {
        let chain = self.chain.read().await;
        chain.get(index as usize).cloned()
    }

    /// Get chain length
    pub async fn chain_length(&self) -> usize {
        let chain = self.chain.read().await;
        chain.len()
    }

    /// Get all transactions for a table
    pub async fn get_table_history(&self, table: &str) -> Vec<Transaction> {
        let chain = self.chain.read().await;
        chain.iter()
            .flat_map(|b| b.transactions.iter())
            .filter(|tx| tx.table == table)
            .cloned()
            .collect()
    }

    /// Verify a specific transaction exists
    pub async fn verify_transaction(&self, tx_id: &str) -> Option<(u64, Transaction)> {
        let chain = self.chain.read().await;
        for block in chain.iter() {
            for tx in &block.transactions {
                if tx.id == tx_id {
                    return Some((block.index, tx.clone()));
                }
            }
        }
        None
    }
}

impl Default for Blockchain {
    fn default() -> Self {
        Self::new(2)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_blockchain() {
        let chain = Blockchain::new(1);
        
        let tx = Transaction {
            id: "tx1".to_string(),
            operation: Operation::Insert,
            table: "users".to_string(),
            data_hash: "abc123".to_string(),
            user_id: "user1".to_string(),
            timestamp: chrono::Utc::now().timestamp(),
        };

        chain.add_transaction(tx).await;
        let block = chain.mine_block().await.unwrap();
        
        assert_eq!(block.index, 1);
        assert!(chain.verify_chain().await);
    }

    #[test]
    fn test_merkle_tree() {
        let data = vec![
            b"data1".to_vec(),
            b"data2".to_vec(),
            b"data3".to_vec(),
            b"data4".to_vec(),
        ];

        let tree = build_merkle_tree(&data).unwrap();
        assert!(!tree.hash.is_empty());
    }
}
