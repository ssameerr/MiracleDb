//! VM Module - Visibility Map management

use std::collections::HashMap;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

/// Visibility flags per block
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct VisibilityFlags {
    pub all_visible: bool,
    pub all_frozen: bool,
}

/// Visibility map page
#[derive(Clone, Debug)]
pub struct VmPage {
    pub block_number: u64,
    pub flags: Vec<VisibilityFlags>,
}

impl VmPage {
    pub fn new(block_number: u64, heap_blocks: u64) -> Self {
        Self {
            block_number,
            flags: vec![VisibilityFlags::default(); heap_blocks as usize],
        }
    }

    pub fn get_flags(&self, heap_block: u64) -> VisibilityFlags {
        self.flags.get(heap_block as usize).copied().unwrap_or_default()
    }

    pub fn set_flags(&mut self, heap_block: u64, flags: VisibilityFlags) {
        while self.flags.len() <= heap_block as usize {
            self.flags.push(VisibilityFlags::default());
        }
        self.flags[heap_block as usize] = flags;
    }
}

/// Per-relation VM
pub struct RelationVm {
    pub rel_id: u64,
    pub pages: HashMap<u64, VmPage>,
}

impl RelationVm {
    pub fn new(rel_id: u64) -> Self {
        Self {
            rel_id,
            pages: HashMap::new(),
        }
    }
}

/// Visibility map manager
pub struct VisibilityMap {
    relations: RwLock<HashMap<u64, RelationVm>>,
}

impl VisibilityMap {
    pub fn new() -> Self {
        Self {
            relations: RwLock::new(HashMap::new()),
        }
    }

    /// Set block as all-visible
    pub async fn set_all_visible(&self, rel_id: u64, block: u64, all_visible: bool) {
        let mut relations = self.relations.write().await;
        let rel_vm = relations.entry(rel_id).or_insert_with(|| RelationVm::new(rel_id));
        
        let vm_page = rel_vm.pages.entry(0).or_insert_with(|| VmPage::new(0, block + 1));
        let mut flags = vm_page.get_flags(block);
        flags.all_visible = all_visible;
        if !all_visible {
            flags.all_frozen = false;
        }
        vm_page.set_flags(block, flags);
    }

    /// Set block as all-frozen
    pub async fn set_all_frozen(&self, rel_id: u64, block: u64, all_frozen: bool) {
        let mut relations = self.relations.write().await;
        let rel_vm = relations.entry(rel_id).or_insert_with(|| RelationVm::new(rel_id));
        
        let vm_page = rel_vm.pages.entry(0).or_insert_with(|| VmPage::new(0, block + 1));
        let mut flags = vm_page.get_flags(block);
        flags.all_frozen = all_frozen;
        if all_frozen {
            flags.all_visible = true;
        }
        vm_page.set_flags(block, flags);
    }

    /// Check if block is all-visible
    pub async fn is_all_visible(&self, rel_id: u64, block: u64) -> bool {
        let relations = self.relations.read().await;
        if let Some(rel_vm) = relations.get(&rel_id) {
            if let Some(vm_page) = rel_vm.pages.get(&0) {
                return vm_page.get_flags(block).all_visible;
            }
        }
        false
    }

    /// Check if block is all-frozen
    pub async fn is_all_frozen(&self, rel_id: u64, block: u64) -> bool {
        let relations = self.relations.read().await;
        if let Some(rel_vm) = relations.get(&rel_id) {
            if let Some(vm_page) = rel_vm.pages.get(&0) {
                return vm_page.get_flags(block).all_frozen;
            }
        }
        false
    }

    /// Clear visibility for block
    pub async fn clear(&self, rel_id: u64, block: u64) {
        self.set_all_visible(rel_id, block, false).await;
    }

    /// Count all-visible blocks
    pub async fn count_visible(&self, rel_id: u64) -> u64 {
        let relations = self.relations.read().await;
        if let Some(rel_vm) = relations.get(&rel_id) {
            if let Some(vm_page) = rel_vm.pages.get(&0) {
                return vm_page.flags.iter().filter(|f| f.all_visible).count() as u64;
            }
        }
        0
    }

    /// Count all-frozen blocks
    pub async fn count_frozen(&self, rel_id: u64) -> u64 {
        let relations = self.relations.read().await;
        if let Some(rel_vm) = relations.get(&rel_id) {
            if let Some(vm_page) = rel_vm.pages.get(&0) {
                return vm_page.flags.iter().filter(|f| f.all_frozen).count() as u64;
            }
        }
        0
    }

    /// Truncate VM
    pub async fn truncate(&self, rel_id: u64, n_blocks: u64) {
        let mut relations = self.relations.write().await;
        if let Some(rel_vm) = relations.get_mut(&rel_id) {
            if let Some(vm_page) = rel_vm.pages.get_mut(&0) {
                vm_page.flags.truncate(n_blocks as usize);
            }
        }
    }
}

impl Default for VisibilityMap {
    fn default() -> Self {
        Self::new()
    }
}
