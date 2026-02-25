//! Smgr Module - Storage Manager Interface
//! Handles physical file I/O (read/write 8KB blocks)

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::path::PathBuf;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(windows)]
use std::os::windows::fs::FileExt;

pub const BLOCK_Size: usize = 8192;

/// Cross-platform file extension trait for positional I/O
trait FileExtCross {
    fn read_exact_at_cross(&self, buf: &mut [u8], offset: u64) -> io::Result<()>;
    fn write_all_at_cross(&self, buf: &[u8], offset: u64) -> io::Result<()>;
}

impl FileExtCross for File {
    #[cfg(unix)]
    fn read_exact_at_cross(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        self.read_exact_at(buf, offset)
    }

    #[cfg(windows)]
    fn read_exact_at_cross(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        let mut bytes_read = 0;
        while bytes_read < buf.len() {
             let n = self.seek_read(&mut buf[bytes_read..], offset + bytes_read as u64)?;
             if n == 0 {
                 return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "failed to fill whole buffer"));
             }
             bytes_read += n;
        }
        Ok(())
    }

    #[cfg(unix)]
    fn write_all_at_cross(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        self.write_all_at(buf, offset)
    }

    #[cfg(windows)]
    fn write_all_at_cross(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        let mut bytes_written = 0;
        while bytes_written < buf.len() {
            let n = self.seek_write(&buf[bytes_written..], offset + bytes_written as u64)?;
            if n == 0 {
                return Err(io::Error::new(io::ErrorKind::WriteZero, "failed to write whole buffer"));
            }
            bytes_written += n;
        }
        Ok(())
    }
}

/// Storage manager relation
#[derive(Debug)] // removed Clone/Serialize because File is not Clone/Serialize
pub struct SmgrRelation {
    pub rel_file_node: RelFileNode,
    pub relpersistence: RelPersistence,
    // File handles for forks. Using Option to lazily open or handle missing files.
    // Index matches ForkNum enum
    pub files: [Option<File>; 4], 
}

/// Relation file node
#[derive(Clone, Debug, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub struct RelFileNode {
    pub spc_oid: u64,
    pub db_oid: u64,
    pub rel_oid: u64,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub enum RelPersistence {
    #[default]
    Permanent,
    Unlogged,
    Temp,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum ForkNum {
    Main = 0,
    Fsm = 1,
    Vm = 2,
    Init = 3,
}

impl SmgrRelation {
    pub fn new(rel_file_node: RelFileNode, persistence: RelPersistence) -> Self {
        Self {
            rel_file_node,
            relpersistence: persistence,
            files: [None, None, None, None],
        }
    }
}

/// Storage manager
pub struct StorageManagerInterface {
    data_dir: PathBuf,
    // We use RwLock to protect the map of open relations
    relations: RwLock<HashMap<RelFileNode, SmgrRelation>>,
}

impl StorageManagerInterface {
    pub fn new(data_dir: PathBuf) -> Self {
        // Ensure data directory exists
        if !data_dir.exists() {
            std::fs::create_dir_all(&data_dir).expect("Failed to create data directory");
        }
        Self {
            data_dir,
            relations: RwLock::new(HashMap::new()),
        }
    }

    /// Open a file handle for a specific fork if not already open
    fn ensure_open(&self, rel: &mut SmgrRelation, fork: ForkNum) -> Result<(), std::io::Error> {
        if rel.files[fork as usize].is_some() {
            return Ok(());
        }

        let path = self.get_path(&rel.rel_file_node, fork);
        
        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;

        rel.files[fork as usize] = Some(file);
        Ok(())
    }

    /// Read a block from storage
    pub async fn read(&self, rel_node: &RelFileNode, fork: ForkNum, block: u64, buffer: &mut [u8]) -> Result<(), String> {
        let mut relations = self.relations.write().await;
        // Get or create relation entry
        let rel = relations.entry(rel_node.clone())
            .or_insert_with(|| SmgrRelation::new(rel_node.clone(), RelPersistence::Permanent));

        // Open file if needed
        self.ensure_open(rel, fork)
            .map_err(|e| format!("Failed to open relation file: {}", e))?;

        let file = rel.files[fork as usize].as_ref().unwrap();
        let offset = block * BLOCK_Size as u64;
        
        file.read_exact_at_cross(buffer, offset)
            .map_err(|e| format!("Failed to read block {}: {}", block, e))
    }

    /// Write a block to storage
    pub async fn write(&self, rel_node: &RelFileNode, fork: ForkNum, block: u64, buffer: &[u8]) -> Result<(), String> {
        let mut relations = self.relations.write().await;
        // println!("SMGR WRITE: {:?} fork={:?} block={} path={:?}", rel_node, fork, block, self.get_path(rel_node, fork));
        let rel = relations.entry(rel_node.clone())
            .or_insert_with(|| SmgrRelation::new(rel_node.clone(), RelPersistence::Permanent));

        self.ensure_open(rel, fork)
            .map_err(|e| format!("Failed to open relation file: {}", e))?;

        let file = rel.files[fork as usize].as_ref().unwrap();
        let offset = block * BLOCK_Size as u64;

        file.write_all_at_cross(buffer, offset)
            .map_err(|e| format!("Failed to write block {}: {}", block, e))
    }

    /// Extend relation by one block (append)
    pub async fn extend(&self, rel_node: &RelFileNode, fork: ForkNum, buffer: &[u8]) -> Result<u64, String> {
        let mut relations = self.relations.write().await;
        let rel = relations.entry(rel_node.clone())
            .or_insert_with(|| SmgrRelation::new(rel_node.clone(), RelPersistence::Permanent));

        self.ensure_open(rel, fork)
            .map_err(|e| format!("Failed to open relation file: {}", e))?;

        let file = rel.files[fork as usize].as_mut().unwrap();
        
        // Seek to end to get current size (block count)
        let current_len = file.metadata()
            .map_err(|e| format!("Failed to get metadata: {}", e))?
            .len();
        
        let block_num = current_len / BLOCK_Size as u64;
        
        // Write at end
        file.write_all_at_cross(buffer, current_len)
            .map_err(|e| format!("Failed to extend relation: {}", e))?;

        // Sync to ensure durability of extension
        file.sync_data()
             .map_err(|e| format!("Failed to sync file: {}", e))?;

        Ok(block_num)
    }

    /// Get number of blocks
    pub async fn nblocks(&self, rel_node: &RelFileNode, fork: ForkNum) -> Result<u64, String> {
        let mut relations = self.relations.write().await;
        let rel = relations.entry(rel_node.clone())
            .or_insert_with(|| SmgrRelation::new(rel_node.clone(), RelPersistence::Permanent));

        // Start by checking if file exists on disk without opening if we can, 
        // but for now, let's just open it to be safe and consistent.
        self.ensure_open(rel, fork)
             .map_err(|e| format!("Failed to open/stat relation file: {}", e))?;
             
        let file = rel.files[fork as usize].as_ref().unwrap();
        let len = file.metadata()
            .map_err(|e| format!("Failed to get metadata: {}", e))?
            .len();
            
        Ok(len / BLOCK_Size as u64)
    }

    /// Helper to construct file path
    pub fn get_path(&self, rel_node: &RelFileNode, fork: ForkNum) -> PathBuf {
        let mut base = self.data_dir.clone();
        base.push(format!("{}", rel_node.db_oid));
        
        // 0 means shared/global, usually we might have a global dir
        // For simplicity: data/db_oid/rel_oid
        
        let filename = match fork {
            ForkNum::Main => format!("{}", rel_node.rel_oid),
            ForkNum::Fsm => format!("{}_fsm", rel_node.rel_oid),
            ForkNum::Vm => format!("{}_vm", rel_node.rel_oid),
            ForkNum::Init => format!("{}_init", rel_node.rel_oid),
        };
        
        base.push(filename);
        base
    }
    
    /// Create directory for database if needed
    pub fn create_database_dir(&self, db_oid: u64) -> Result<(), std::io::Error> {
        let path = self.data_dir.join(format!("{}", db_oid));
        std::fs::create_dir_all(path)
    }
}

impl Default for StorageManagerInterface {
    fn default() -> Self {
        Self::new(PathBuf::from("./data"))
    }
}

