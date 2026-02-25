//! WAL Module - Write-Ahead Log management

use std::collections::VecDeque;
use std::path::PathBuf;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

/// WAL segment
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WalSegment {
    pub name: String,
    pub timeline: u32,
    pub log_id: u32,
    pub seg_id: u32,
    pub size_bytes: u64,
    pub status: SegmentStatus,
    pub created_at: i64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
pub enum SegmentStatus {
    #[default]
    Current,
    Filled,
    Archived,
    Recycled,
}

impl WalSegment {
    pub fn new(timeline: u32, log_id: u32, seg_id: u32) -> Self {
        Self {
            name: Self::generate_name(timeline, log_id, seg_id),
            timeline,
            log_id,
            seg_id,
            size_bytes: 0,
            status: SegmentStatus::default(),
            created_at: chrono::Utc::now().timestamp(),
        }
    }

    fn generate_name(timeline: u32, log_id: u32, seg_id: u32) -> String {
        format!("{:08X}{:08X}{:08X}", timeline, log_id, seg_id)
    }

    pub fn lsn_range(&self, segment_size: u64) -> (u64, u64) {
        let start = (self.log_id as u64 * 0x100000000) + (self.seg_id as u64 * segment_size);
        let end = start + segment_size;
        (start, end)
    }
}

/// WAL record
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WalRecord {
    pub lsn: u64,
    pub prev_lsn: u64,
    pub xid: u64,
    pub record_type: WalRecordType,
    pub data: Vec<u8>,
    pub timestamp: i64,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum WalRecordType {
    Insert,
    Update,
    Delete,
    Commit,
    Abort,
    Checkpoint,
    Truncate,
    Create,
    Drop,
    Heap,
    Index,
    Sequence,
    Xact,
    Standby,
    RelMap,
    FPI,
}

/// WAL statistics
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct WalStats {
    pub current_lsn: u64,
    pub insert_lsn: u64,
    pub flush_lsn: u64,
    pub write_lsn: u64,
    pub wal_buffers_full: u64,
    pub wal_write: u64,
    pub wal_sync: u64,
    pub wal_write_time: f64,
    pub wal_sync_time: f64,
}

/// WAL manager
pub struct WalManager {
    wal_dir: PathBuf,
    segment_size: u64,
    segments: RwLock<VecDeque<WalSegment>>,
    current_lsn: RwLock<u64>,
    flush_lsn: RwLock<u64>,
    stats: RwLock<WalStats>,
    buffer: RwLock<Vec<WalRecord>>,
    max_wal_size: u64,
}

impl WalManager {
    pub fn new(wal_dir: PathBuf) -> Self {
        if !wal_dir.exists() {
            std::fs::create_dir_all(&wal_dir).expect("Failed to create WAL directory");
        }
        Self {
            wal_dir,
            segment_size: 16 * 1024 * 1024, // 16MB
            segments: RwLock::new(VecDeque::new()),
            current_lsn: RwLock::new(0),
            flush_lsn: RwLock::new(0),
            stats: RwLock::new(WalStats::default()),
            buffer: RwLock::new(Vec::new()),
            max_wal_size: 1024 * 1024 * 1024, // 1GB
        }
    }

    pub async fn write(&self, record_type: WalRecordType, xid: u64, data: Vec<u8>) -> u64 {
        let mut current_lsn_guard = self.current_lsn.write().await;
        let mut buffer = self.buffer.write().await;

        let lsn = *current_lsn_guard;
        let prev_lsn = if lsn > 0 { lsn - 1 } else { 0 };

        let record = WalRecord {
            lsn,
            prev_lsn,
            xid,
            record_type,
            data,
            timestamp: chrono::Utc::now().timestamp(),
        };

        buffer.push(record);
        *current_lsn_guard += 1;

        lsn
    }

    /// Flush WAL buffer to disk
    pub async fn flush(&self) -> Result<u64, String> {
        let mut buffer = self.buffer.write().await;
        if buffer.is_empty() {
             return Ok(*self.flush_lsn.read().await);
        }

        let mut current_lsn = self.current_lsn.write().await;
        
        // ensure we have a segment to write to
        let segment_name = {
            let mut segments = self.segments.write().await;
            if segments.is_empty() {
                 let first_seg = WalSegment::new(1, 0, 0);
                 segments.push_back(first_seg.clone());
                 first_seg.name
            } else {
                 segments.back().unwrap().name.clone()
            }
        };

        let file_path = self.wal_dir.join(&segment_name);
        
        use std::fs::OpenOptions;
        use std::io::Write;
        
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_path)
            .map_err(|e| format!("Failed to open WAL file: {}", e))?;

        // Serialize and Write records
        // Using bincode for binary packing
        for record in buffer.iter() {
             let encoded = bincode::serialize(record).map_err(|e| e.to_string())?;
             file.write_all(&encoded).map_err(|e| e.to_string())?;
        }
        
        // Sync to disk
        file.sync_data().map_err(|e| format!("fsync failed: {}", e))?;

        let new_flush_lsn = buffer.last().map(|r| r.lsn).unwrap_or(*self.flush_lsn.read().await);
        
        // Update stats
        let mut flush_lsn_lock = self.flush_lsn.write().await;
        *flush_lsn_lock = new_flush_lsn;
        
        let mut stats = self.stats.write().await;
        stats.flush_lsn = new_flush_lsn;
        stats.wal_sync += 1;
        
        buffer.clear();

        Ok(new_flush_lsn)
    }

    /// Get current LSN
    pub async fn get_current_lsn(&self) -> u64 {
        *self.current_lsn.read().await
    }

    /// Get flush LSN
    pub async fn get_flush_lsn(&self) -> u64 {
        *self.flush_lsn.read().await
    }

    /// Switch to new WAL segment
    pub async fn switch_segment(&self) -> WalSegment {
        // Force flush first
        let _ = self.flush().await;

        let mut segments = self.segments.write().await;

        // Mark current as filled
        if let Some(current) = segments.back_mut() {
            current.status = SegmentStatus::Filled;
        }

        let timeline = 1;
        let (log_id, seg_id) = if let Some(last) = segments.back() {
            (last.log_id, last.seg_id + 1)
        } else {
            (0, 0)
        };

        let segment = WalSegment::new(timeline, log_id, seg_id);
        segments.push_back(segment.clone());
        
        // Create the file immediately
        let file_path = self.wal_dir.join(&segment.name);
        let _ = std::fs::File::create(file_path);

        segment
    }

    /// Archive segment
    pub async fn archive(&self, name: &str) -> Result<(), String> {
        let mut segments = self.segments.write().await;

        let segment = segments.iter_mut()
            .find(|s| s.name == name)
            .ok_or_else(|| format!("Segment {} not found", name))?;

        if segment.status != SegmentStatus::Filled {
            return Err("Cannot archive non-filled segment".to_string());
        }

        segment.status = SegmentStatus::Archived;
        Ok(())
    }

    /// Get WAL statistics
    pub async fn get_stats(&self) -> WalStats {
        self.stats.read().await.clone()
    }

    /// List segments
    pub async fn list_segments(&self) -> Vec<WalSegment> {
        let segments = self.segments.read().await;
        segments.iter().cloned().collect()
    }

    /// Calculate WAL size
    pub async fn wal_size(&self) -> u64 {
        let segments = self.segments.read().await;
        segments.iter()
            .filter(|s| s.status != SegmentStatus::Archived && s.status != SegmentStatus::Recycled)
            .map(|s| s.size_bytes)
            .sum()
    }

    /// Replay WAL records from the beginning (simplified recovery)
    pub async fn replay(&self) -> Result<Vec<WalRecord>, String> {
        let segments = self.segments.read().await;
        let mut records = Vec::new();
        
        // Scan all segments in order
        // Note: in a real DB we'd start from a checkpoint. Here we read all available segments.
        // If segments list is empty, scan directory
        let segments_to_read = if segments.is_empty() {
            // Read dir
            let mut found = Vec::new();
            if let Ok(entries) = std::fs::read_dir(&self.wal_dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.is_file() {
                        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                            // Filter valid segment names (hex)
                            if name.len() == 24 { // 8+8+8 chars
                                 found.push(path);
                            }
                        }
                    }
                }
            }
            found.sort(); // Ensure timeline/order
            found
        } else {
            segments.iter().map(|s| self.wal_dir.join(&s.name)).collect()
        };

        for path in segments_to_read {
            if let Ok(file) = std::fs::File::open(&path) {
                let mut reader = std::io::BufReader::new(file);
                // Read records until EOF
                loop {
                    match bincode::deserialize_from::<_, WalRecord>(&mut reader) {
                        Ok(record) => records.push(record),
                        Err(_) => break, // EOF or corrupt tail
                    }
                }
            }
        }
        
        Ok(records)
    }
}

impl Default for WalManager {
    fn default() -> Self {
        Self::new(PathBuf::from("/var/lib/miracledb/pg_wal"))
    }
}
