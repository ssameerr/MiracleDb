//! Storage Module - Low-level storage management

use std::collections::HashMap;
use std::path::PathBuf;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

/// Storage file
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StorageFile {
    pub rel_file_node: u64,
    pub segment: u32,
    pub size_bytes: u64,
    pub path: PathBuf,
    pub created_at: i64,
}

/// Page header
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PageHeader {
    pub lsn: u64,
    pub checksum: u16,
    pub flags: u16,
    pub lower: u16,
    pub upper: u16,
    pub special: u16,
    pub page_size: u16,
    pub version: u16,
    pub prune_xid: u64,
}

impl Default for PageHeader {
    fn default() -> Self {
        Self {
            lsn: 0,
            checksum: 0,
            flags: 0,
            lower: 24, // After header
            upper: 8192, // End of page
            special: 8192,
            page_size: 8192,
            version: 4,
            prune_xid: 0,
        }
    }
}

/// Item pointer
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct ItemPointer {
    pub block_number: u64,
    pub offset_number: u16,
}

impl ItemPointer {
    pub fn new(block: u64, offset: u16) -> Self {
        Self {
            block_number: block,
            offset_number: offset,
        }
    }

    pub fn invalid() -> Self {
        Self {
            block_number: u64::MAX,
            offset_number: 0,
        }
    }

    pub fn is_valid(&self) -> bool {
        self.block_number != u64::MAX
    }
}

/// Storage statistics
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct StorageStats {
    pub total_blocks: u64,
    pub used_blocks: u64,
    pub free_blocks: u64,
    pub total_bytes: u64,
    pub used_bytes: u64,
}

/// Fixed-size page (8KB)
pub const PAGE_SIZE: usize = 8192;

#[derive(Clone)]
pub struct Page {
    pub raw: [u8; PAGE_SIZE],
}

impl Page {
    pub fn new() -> Self {
        Self {
            raw: [0; PAGE_SIZE],
        }
    }

    /// Initialize empty page with header
    pub fn init(&mut self) {
        let header = PageHeader::default();
        // Serialize header to raw bytes (simplified manual serialization for now)
        // In reality, we'd use bincode or similar, but for fixed layout we might want explicit packing
        // For this prototype, we'll rely on serde_json for metadata or simple byte copying if using bincode
        // But to keep it simple and compile-safe:
        // We will just zero it out for now, header management is higher level logic
        self.raw.fill(0);
    }
    
    pub fn from_bytes(bytes: &[u8]) -> Self {
        let mut page = Self::new();
        let len = std::cmp::min(bytes.len(), PAGE_SIZE);
        page.raw[0..len].copy_from_slice(&bytes[0..len]);
        page
    }
}

impl Default for Page {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for Page {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Page(size={})", PAGE_SIZE)
    }
}

/// Storage manager (Legacy/In-Memory Simulation - Deprecated)
/// This is kept for compatibility with existing modules until fully migrated to smgr
pub struct StorageManager {
    data_dir: PathBuf,
    files: RwLock<HashMap<u64, StorageFile>>,
    stats: RwLock<StorageStats>,
    block_size: u32,
}

impl StorageManager {
    pub fn new(data_dir: PathBuf) -> Self {
        // Create data directory if it doesn't exist
        let _ = std::fs::create_dir_all(&data_dir);
        
        Self {
            data_dir,
            files: RwLock::new(HashMap::new()),
            stats: RwLock::new(StorageStats::default()),
            block_size: 8192,
        }
    }
    
    /// Get the file path for a relation
    fn get_file_path(&self, rel_file_node: u64, segment: u32) -> PathBuf {
        if segment == 0 {
            self.data_dir.join(format!("{}", rel_file_node))
        } else {
            self.data_dir.join(format!("{}.{}", rel_file_node, segment))
        }
    }
    
    /// Create a new relation file
    pub async fn create_relation(&self, rel_file_node: u64) -> Result<StorageFile, String> {
        let path = self.get_file_path(rel_file_node, 0);
        
        // Create the file
        std::fs::File::create(&path)
            .map_err(|e| format!("Failed to create relation file: {}", e))?;
        
        let file = StorageFile {
            rel_file_node,
            segment: 0,
            size_bytes: 0,
            path: path.clone(),
            created_at: chrono::Utc::now().timestamp(),
        };
        
        let mut files = self.files.write().await;
        files.insert(rel_file_node, file.clone());
        
        // Update stats
        let mut stats = self.stats.write().await;
        stats.total_blocks += 0;
        
        Ok(file)
    }
    
    /// Extend a relation by n_blocks
    pub async fn extend(&self, rel_file_node: u64, n_blocks: u32) -> Result<u64, String> {
        let path = self.get_file_path(rel_file_node, 0);
        
        // Get current size
        let current_size = std::fs::metadata(&path)
            .map(|m| m.len())
            .unwrap_or(0);
        
        let current_blocks = current_size / self.block_size as u64;
        let new_size = current_size + (n_blocks as u64 * self.block_size as u64);
        
        // Extend file with zeros
        let file = std::fs::OpenOptions::new()
            .write(true)
            .append(true)
            .open(&path)
            .map_err(|e| format!("Failed to open relation file: {}", e))?;
        
        file.set_len(new_size)
            .map_err(|e| format!("Failed to extend relation file: {}", e))?;
        
        // Update stats
        let mut stats = self.stats.write().await;
        stats.total_blocks += n_blocks as u64;
        stats.total_bytes += n_blocks as u64 * self.block_size as u64;
        
        Ok(current_blocks)
    }

    /// Truncate a relation to n_blocks
    pub async fn truncate(&self, rel_file_node: u64, n_blocks: u64) -> Result<(), String> {
        let path = self.get_file_path(rel_file_node, 0);
        let new_size = n_blocks * self.block_size as u64;
        
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .map_err(|e| format!("Failed to open relation file: {}", e))?;
        
        file.set_len(new_size)
            .map_err(|e| format!("Failed to truncate relation file: {}", e))?;
        
        Ok(())
    }

    /// Get the size of a relation in bytes
    pub async fn get_size(&self, rel_file_node: u64) -> Option<u64> {
        let path = self.get_file_path(rel_file_node, 0);
        std::fs::metadata(&path).map(|m| m.len()).ok()
    }

    /// Get the number of blocks in a relation
    pub async fn get_block_count(&self, rel_file_node: u64) -> Option<u64> {
        self.get_size(rel_file_node).await
            .map(|size| size / self.block_size as u64)
    }

    /// Drop a relation (delete files)
    pub async fn drop_relation(&self, rel_file_node: u64) -> Result<(), String> {
        // Remove all segments
        for segment in 0..100 {
            let path = self.get_file_path(rel_file_node, segment);
            if path.exists() {
                std::fs::remove_file(&path)
                    .map_err(|e| format!("Failed to remove segment {}: {}", segment, e))?;
            } else {
                break;
            }
        }
        
        // Remove from registry
        let mut files = self.files.write().await;
        files.remove(&rel_file_node);
        
        Ok(())
    }

    /// Get storage statistics
    pub async fn get_stats(&self) -> StorageStats {
        // Recalculate from actual files
        let mut stats = StorageStats::default();
        
        if let Ok(entries) = std::fs::read_dir(&self.data_dir) {
            for entry in entries.flatten() {
                if let Ok(meta) = entry.metadata() {
                    if meta.is_file() {
                        stats.total_bytes += meta.len();
                        stats.total_blocks += meta.len() / self.block_size as u64;
                        stats.used_blocks += meta.len() / self.block_size as u64;
                        stats.used_bytes += meta.len();
                    }
                }
            }
        }
        
        stats
    }
}

impl Default for StorageManager {
    fn default() -> Self {
        Self::new(PathBuf::from("/var/lib/miracledb/base"))
    }
}
