//! Heap Module - Heap table access
//! Implements Slotted Page layout on top of BufferPool

use std::sync::Arc;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

use crate::buffer::{BufferPool, BufferTag};
use crate::smgr::{RelFileNode, ForkNum as ForkNumber};
use crate::storage::{PAGE_SIZE, Page, PageHeader, ItemPointer};

/// Heap tuple header
#[derive(Clone, Debug, Serialize, Deserialize)]
#[repr(C)]
pub struct HeapTupleHeader {
    pub t_xmin: u64,
    pub t_xmax: u64,
    pub t_cid: u32,
    pub t_ctid: (u64, u16), // block, offset
    pub t_infomask: u16,
    pub t_infomask2: u16,
    pub t_hoff: u8,
}

impl Default for HeapTupleHeader {
    fn default() -> Self {
        Self {
            t_xmin: 0,
            t_xmax: 0,
            t_cid: 0,
            t_ctid: (0, 0),
            t_infomask: 0,
            t_infomask2: 0,
            t_hoff: 24, // Size of header approx
        }
    }
}

/// Infomask flags
pub mod infomask {
    pub const HEAP_HASNULL: u16 = 0x0001;
    pub const HEAP_XMIN_COMMITTED: u16 = 0x0100;
    pub const HEAP_XMAX_INVALID: u16 = 0x0800;
}

/// Heap tuple
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HeapTuple {
    pub header: HeapTupleHeader,
    pub data: Vec<u8>,
}

impl HeapTuple {
    pub fn new(xmin: u64, data: Vec<u8>) -> Self {
        Self {
            header: HeapTupleHeader {
                t_xmin: xmin,
                t_xmax: 0,
                t_cid: 0,
                t_ctid: (0, 0),
                t_infomask: infomask::HEAP_XMAX_INVALID,
                t_infomask2: 0,
                t_hoff: 32, // Simplified fixed header size
            },
            data,
        }
    }

    pub fn is_visible(&self, sn_xmin: u64, sn_xmax: u64, active_xids: &[u64]) -> bool {
        // MVCC Visibility Rules
        let t_xmin = self.header.t_xmin;
        let t_xmax = self.header.t_xmax;

        // 1. Check Creator (xmin)
        let xmin_visible = if t_xmin < sn_xmin {
            true // Committed before snapshot
        } else if t_xmin >= sn_xmax {
            false // Future transaction
        } else {
            !active_xids.contains(&t_xmin) // Visible if not active (implies committed)
        };

        if !xmin_visible {
            return false;
        }

        // 2. Check Deleter (xmax)
        if t_xmax == 0 {
            return true; // Not deleted
        }

        let xmax_visible = if t_xmax < sn_xmin {
            true
        } else if t_xmax >= sn_xmax {
            false
        } else {
            !active_xids.contains(&t_xmax)
        };

        if xmax_visible {
            return false; // Deleted by visible transaction
        }

        true // Deleter is invisible (aborted or in progress or future), so version is valid
    }
}

// Slotted Page Layout Constants
const HEADER_SIZE: usize = 24; // PageHeader
const ITEM_ID_SIZE: usize = 4; // offset(2) + len(2)

/// Helper to access page layout
struct SlottedPage<'a> {
    page: &'a mut Page,
}

impl<'a> SlottedPage<'a> {
    fn new(page: &'a mut Page) -> Self {
        Self { page }
    }

    // Simplified manual getters/setters for header fields (assuming little endian)
    fn get_lower(&self) -> u16 {
        u16::from_le_bytes(self.page.raw[12..14].try_into().unwrap())
    }

    fn set_lower(&mut self, val: u16) {
        self.page.raw[12..14].copy_from_slice(&val.to_le_bytes());
    }

    fn get_upper(&self) -> u16 {
        u16::from_le_bytes(self.page.raw[14..16].try_into().unwrap())
    }

    fn set_upper(&mut self, val: u16) {
        self.page.raw[14..16].copy_from_slice(&val.to_le_bytes());
    }
    
    // Init page if empty
    fn init_if_new(&mut self) {
        if self.get_upper() == 0 {
             self.set_lower(HEADER_SIZE as u16);
             self.set_upper(PAGE_SIZE as u16);
        }
    }

    fn free_space(&self) -> usize {
        let lower = self.get_lower() as usize;
        let upper = self.get_upper() as usize;
        if upper < lower { return 0; }
        upper - lower
    }

    fn add_item(&mut self, data: &[u8]) -> Option<u16> {
        self.init_if_new();
        
        // Need space for itemId (4 bytes) + data
        if self.free_space() < data.len() + ITEM_ID_SIZE {
            return None;
        }

        let lower = self.get_lower() as usize;
        let upper = self.get_upper() as usize;
        
        let new_upper = upper - data.len();
        
        // Write data
        self.page.raw[new_upper..upper].copy_from_slice(data);
        self.set_upper(new_upper as u16);
        
        // Write ItemId at lower
        let item_idx = (lower - HEADER_SIZE) / ITEM_ID_SIZE;
        let offset = new_upper as u16;
        let len = data.len() as u16;
        
        // ItemId = offset(2) + len(2)
        self.page.raw[lower..lower+2].copy_from_slice(&offset.to_le_bytes());
        self.page.raw[lower+2..lower+4].copy_from_slice(&len.to_le_bytes());
        
        self.set_lower((lower + ITEM_ID_SIZE) as u16);
        
        Some(item_idx as u16)
    }

    fn get_item(&self, index: u16) -> Option<Vec<u8>> {
         let lower = self.get_lower() as usize;
         let item_offset = HEADER_SIZE + (index as usize * ITEM_ID_SIZE);
         
         if item_offset + ITEM_ID_SIZE > lower {
             return None; // Index out of bounds
         }
         
         let offset = u16::from_le_bytes(self.page.raw[item_offset..item_offset+2].try_into().unwrap()) as usize;
         let len = u16::from_le_bytes(self.page.raw[item_offset+2..item_offset+4].try_into().unwrap()) as usize;
         
         if offset == 0 || len == 0 { return None; } // Unused/Deleted
         
         Some(self.page.raw[offset..offset+len].to_vec())
    }
    
    fn item_count(&self) -> u16 {
        let lower = self.get_lower() as usize;
        if lower < HEADER_SIZE {
            return 0; // Uninitialized page
        }
        ((lower - HEADER_SIZE) / ITEM_ID_SIZE) as u16
    }
}

/// Heap relation accessor
pub struct HeapRelation {
    pub rel_file_node: RelFileNode,
    pub buffer_pool: Arc<BufferPool>,
    /// Cached last block number for fast inserts
    last_block: tokio::sync::RwLock<Option<u64>>,
    /// SMGR reference for relation extension
    smgr: Option<Arc<crate::smgr::StorageManagerInterface>>,
}

impl HeapRelation {
    pub fn new(rel_file_node: RelFileNode, buffer_pool: Arc<BufferPool>) -> Self {
        Self {
            rel_file_node,
            buffer_pool,
            last_block: tokio::sync::RwLock::new(None),
            smgr: None,
        }
    }
    
    /// Create with SMGR reference for full functionality
    pub fn with_smgr(rel_file_node: RelFileNode, buffer_pool: Arc<BufferPool>, smgr: Arc<crate::smgr::StorageManagerInterface>) -> Self {
        Self {
            rel_file_node,
            buffer_pool,
            last_block: tokio::sync::RwLock::new(None),
            smgr: Some(smgr),
        }
    }
    
    /// Get or refresh last block number
    async fn get_last_block(&self) -> u64 {
        let cached = self.last_block.read().await;
        if let Some(block) = *cached {
            return block;
        }
        drop(cached);
        
        // Query smgr for actual block count
        if let Some(ref smgr) = self.smgr {
            if let Ok(nblocks) = smgr.nblocks(&self.rel_file_node, ForkNumber::Main).await {
                if nblocks > 0 {
                    let last = nblocks - 1;
                    let mut cached = self.last_block.write().await;
                    *cached = Some(last);
                    return last;
                }
            }
        }
        0
    }
    
    /// Update last block cache after extension
    async fn update_last_block(&self, block: u64) {
        let mut cached = self.last_block.write().await;
        *cached = Some(block);
    }

    /// Insert a tuple
    pub async fn insert(&self, xid: u64, data: Vec<u8>) -> Result<(u64, u16), String> {
        // Simple serialization of HeapTuple: Header + Data
        let tuple = HeapTuple::new(xid, data);
        let mut encoded = bincode::serialize(&tuple.header).map_err(|e| e.to_string())?;
        encoded.extend_from_slice(&tuple.data);

        // Start with cached last block for efficiency
        let mut block = self.get_last_block().await;
        let mut attempts = 0;
        
        loop {
            let tag = BufferTag {
                rel_file_node: self.rel_file_node.clone(),
                fork_number: ForkNumber::Main,
                block_number: block,
            };
            
            match self.buffer_pool.read_buffer(tag.clone()).await {
                Ok(buf_id) => {
                    let mut page = self.buffer_pool.get_page(buf_id).await;
                    let mut slotted = SlottedPage::new(&mut page);
                    
                    if let Some(offset) = slotted.add_item(&encoded) {
                        self.buffer_pool.write_page(buf_id, page).await;
                        self.buffer_pool.mark_dirty(&tag).await;
                        // Update last block cache for next insert
                        self.update_last_block(block).await;
                        return Ok((block, offset));
                    }
                    // Page full, try next
                    block += 1;
                }
                Err(_) => {
                    // EOF - need to extend relation
                    if let Some(ref smgr) = self.smgr {
                        // Create new empty page
                        let mut page = Page::new();
                        let mut slotted = SlottedPage::new(&mut page);
                        slotted.init_if_new();
                        
                        if let Some(offset) = slotted.add_item(&encoded) {
                            // Extend relation via SMGR
                            match smgr.extend(&self.rel_file_node, ForkNumber::Main, &page.raw).await {
                                Ok(new_block) => {
                                    // Pre-load into buffer pool to ensure visibility
                                    let tag = BufferTag {
                                        rel_file_node: self.rel_file_node.clone(),
                                        fork_number: ForkNumber::Main,
                                        block_number: new_block,
                                    };
                                    let _ = self.buffer_pool.load_page(tag, page).await;

                                    // Update cache with new block
                                    self.update_last_block(new_block).await;
                                    return Ok((new_block, offset));
                                }
                                Err(e) => {
                                    return Err(format!("Failed to extend relation: {}", e));
                                }
                            }
                        }
                        return Err("Tuple too large for empty page".to_string());
                    } else {
                        // No SMGR available - try to create page in buffer pool directly
                        // This is a fallback for backward compatibility
                        let mut page = Page::new();
                        let mut slotted = SlottedPage::new(&mut page);
                        slotted.init_if_new();
                        
                        if slotted.add_item(&encoded).is_some() {
                            // Write through buffer pool (will create on flush)
                            let new_tag = BufferTag {
                                rel_file_node: self.rel_file_node.clone(),
                                fork_number: ForkNumber::Main,
                                block_number: block,
                            };
                            // Force allocation in buffer pool
                            if let Ok(buf_id) = self.buffer_pool.read_buffer(new_tag.clone()).await.or_else(|_| Ok::<u32, String>(0)) {
                                self.buffer_pool.write_page(buf_id, page).await;
                                self.buffer_pool.mark_dirty(&new_tag).await;
                                self.update_last_block(block).await;
                                return Ok((block, 0));
                            }
                        }
                        return Err("Relation extension requires SMGR reference".to_string());
                    }
                }
            }
            
            attempts += 1;
            // Limit loop for safety
            if attempts > 1000 { return Err("Heap Full (alloc limit)".to_string()); }
        }
    }
    
    /// Scan all tuples visible to the snapshot
    pub async fn scan(&self, xmin: u64, xmax: u64, active_xids: &[u64]) -> Vec<HeapTuple> {
        let mut tuples = Vec::new();
        let mut block = 0;
        
        loop {
             let tag = BufferTag {
                rel_file_node: self.rel_file_node.clone(),
                fork_number: ForkNumber::Main,
                block_number: block,
            };
            
            if let Ok(buf_id) = self.buffer_pool.read_buffer(tag).await {
                let page = self.buffer_pool.get_page(buf_id).await;
                let mut page_clone = page.clone();
                let mut slotted = SlottedPage::new(&mut page_clone); // operate on clone
                
                let count = slotted.item_count();
                for i in 0..count {
                    if let Some(data) = slotted.get_item(i) {
                        // Deserialize
                        // Header is fixed size (approx) or use bincode
                        // We used bincode for header
                        let header_size = bincode::serialized_size(&HeapTupleHeader::default()).unwrap() as usize; 
                        
                        if data.len() >= header_size {
                            if let Ok(header) = bincode::deserialize::<HeapTupleHeader>(&data[..header_size]) {
                                let tuple_data = data[header_size..].to_vec();
                                let tuple = HeapTuple { header, data: tuple_data };
                                if tuple.is_visible(xmin, xmax, active_xids) {
                                    tuples.push(tuple);
                                }
                            }
                        }
                    }
                }
                block += 1;
            } else {
                break; // EOF
            }
        }
        
        tuples
    }
}

