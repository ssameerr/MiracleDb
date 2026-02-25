//! FSM Module - Free Space Map management

use std::collections::HashMap;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

/// Free space category (0-255 representing available space)
pub type FsmCategory = u8;

/// FSM page
#[derive(Clone, Debug)]
pub struct FsmPage {
    pub block_number: u64,
    pub categories: Vec<FsmCategory>,
}

impl FsmPage {
    pub fn new(block_number: u64, heap_blocks: u64) -> Self {
        Self {
            block_number,
            categories: vec![255; heap_blocks as usize], // All empty initially
        }
    }

    pub fn get_category(&self, heap_block: u64) -> FsmCategory {
        self.categories.get(heap_block as usize).copied().unwrap_or(0)
    }

    pub fn set_category(&mut self, heap_block: u64, category: FsmCategory) {
        if let Some(cat) = self.categories.get_mut(heap_block as usize) {
            *cat = category;
        }
    }
}

/// Convert bytes to FSM category
pub fn bytes_to_category(bytes: u32, block_size: u32) -> FsmCategory {
    if bytes >= block_size {
        255
    } else {
        ((bytes as u64 * 255) / block_size as u64) as u8
    }
}

/// Convert FSM category to minimum bytes
pub fn category_to_bytes(category: FsmCategory, block_size: u32) -> u32 {
    (category as u64 * block_size as u64 / 255) as u32
}

/// Free space map
pub struct FreeSpaceMap {
    block_size: u32,
    relations: RwLock<HashMap<u64, RelationFsm>>,
}

/// Per-relation FSM
pub struct RelationFsm {
    pub rel_id: u64,
    pub pages: HashMap<u64, FsmPage>,
    pub last_search_block: u64,
}

impl RelationFsm {
    pub fn new(rel_id: u64) -> Self {
        Self {
            rel_id,
            pages: HashMap::new(),
            last_search_block: 0,
        }
    }
}

impl FreeSpaceMap {
    pub fn new() -> Self {
        Self {
            block_size: 8192,
            relations: RwLock::new(HashMap::new()),
        }
    }

    /// Record free space for a block
    pub async fn record_free_space(&self, rel_id: u64, block: u64, free_bytes: u32) {
        let category = bytes_to_category(free_bytes, self.block_size);
        
        let mut relations = self.relations.write().await;
        let rel_fsm = relations.entry(rel_id).or_insert_with(|| RelationFsm::new(rel_id));

        let fsm_page = rel_fsm.pages.entry(0).or_insert_with(|| FsmPage::new(0, block + 1));
        
        // Ensure categories vector is large enough
        while fsm_page.categories.len() <= block as usize {
            fsm_page.categories.push(0);
        }
        
        fsm_page.set_category(block, category);
    }

    /// Search for a block with at least min_bytes free
    pub async fn search_free_space(&self, rel_id: u64, min_bytes: u32) -> Option<u64> {
        let min_category = bytes_to_category(min_bytes, self.block_size);
        
        let mut relations = self.relations.write().await;
        let rel_fsm = relations.get_mut(&rel_id)?;

        let start = rel_fsm.last_search_block;
        let fsm_page = rel_fsm.pages.get(&0)?;

        // Search from last position
        for i in 0..fsm_page.categories.len() {
            let idx = (start as usize + i) % fsm_page.categories.len();
            if fsm_page.categories[idx] >= min_category {
                rel_fsm.last_search_block = idx as u64;
                return Some(idx as u64);
            }
        }

        None
    }

    /// Get free space for a block
    pub async fn get_free_space(&self, rel_id: u64, block: u64) -> Option<u32> {
        let relations = self.relations.read().await;
        let rel_fsm = relations.get(&rel_id)?;
        let fsm_page = rel_fsm.pages.get(&0)?;
        let category = fsm_page.get_category(block);
        Some(category_to_bytes(category, self.block_size))
    }

    /// Vacuum FSM - update all block categories
    pub async fn vacuum(&self, rel_id: u64, block_free_spaces: Vec<(u64, u32)>) {
        let mut relations = self.relations.write().await;
        let rel_fsm = relations.entry(rel_id).or_insert_with(|| RelationFsm::new(rel_id));

        for (block, free_bytes) in block_free_spaces {
            let category = bytes_to_category(free_bytes, self.block_size);
            let fsm_page = rel_fsm.pages.entry(0).or_insert_with(|| FsmPage::new(0, block + 1));
            
            while fsm_page.categories.len() <= block as usize {
                fsm_page.categories.push(0);
            }
            
            fsm_page.set_category(block, category);
        }
    }

    /// Truncate FSM
    pub async fn truncate(&self, rel_id: u64, n_blocks: u64) {
        let mut relations = self.relations.write().await;
        if let Some(rel_fsm) = relations.get_mut(&rel_id) {
            if let Some(fsm_page) = rel_fsm.pages.get_mut(&0) {
                fsm_page.categories.truncate(n_blocks as usize);
            }
        }
    }
}

impl Default for FreeSpaceMap {
    fn default() -> Self {
        Self::new()
    }
}
