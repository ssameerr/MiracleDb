//! Clog Module - Commit Log (Transaction Status)

use std::collections::HashMap;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

/// Transaction status bits
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum XidStatus {
    InProgress = 0,
    Committed = 1,
    Aborted = 2,
    SubCommitted = 3,
}

impl XidStatus {
    pub fn from_bits(bits: u8) -> Self {
        match bits & 0x03 {
            0 => XidStatus::InProgress,
            1 => XidStatus::Committed,
            2 => XidStatus::Aborted,
            3 => XidStatus::SubCommitted,
            _ => XidStatus::InProgress,
        }
    }

    pub fn to_bits(self) -> u8 {
        self as u8
    }
}

/// CLOG page (stores status for many XIDs)
#[derive(Clone, Debug)]
pub struct ClogPage {
    pub page_number: u64,
    pub data: Vec<u8>, // 2 bits per XID
}

impl ClogPage {
    pub fn new(page_number: u64) -> Self {
        // Each page stores 32K XIDs (2 bits each = 8KB)
        Self {
            page_number,
            data: vec![0; 8192],
        }
    }

    const XIDS_PER_PAGE: u64 = 32768;

    pub fn get_status(&self, xid: u64) -> XidStatus {
        let local_xid = xid % Self::XIDS_PER_PAGE;
        let byte_offset = (local_xid / 4) as usize;
        let bit_offset = ((local_xid % 4) * 2) as u8;

        if byte_offset < self.data.len() {
            let bits = (self.data[byte_offset] >> bit_offset) & 0x03;
            XidStatus::from_bits(bits)
        } else {
            XidStatus::InProgress
        }
    }

    pub fn set_status(&mut self, xid: u64, status: XidStatus) {
        let local_xid = xid % Self::XIDS_PER_PAGE;
        let byte_offset = (local_xid / 4) as usize;
        let bit_offset = ((local_xid % 4) * 2) as u8;

        if byte_offset < self.data.len() {
            let mask = !(0x03 << bit_offset);
            self.data[byte_offset] = (self.data[byte_offset] & mask) | (status.to_bits() << bit_offset);
        }
    }
}

/// Commit log manager
pub struct CommitLog {
    pages: RwLock<HashMap<u64, ClogPage>>,
    oldest_xid: u64,
}

impl CommitLog {
    pub fn new() -> Self {
        Self {
            pages: RwLock::new(HashMap::new()),
            oldest_xid: 3,
        }
    }

    fn get_page_number(xid: u64) -> u64 {
        xid / ClogPage::XIDS_PER_PAGE
    }

    /// Get transaction status
    pub async fn get_status(&self, xid: u64) -> XidStatus {
        let page_num = Self::get_page_number(xid);
        let pages = self.pages.read().await;

        if let Some(page) = pages.get(&page_num) {
            page.get_status(xid)
        } else {
            XidStatus::InProgress
        }
    }

    /// Set transaction status
    pub async fn set_status(&self, xid: u64, status: XidStatus) {
        let page_num = Self::get_page_number(xid);
        let mut pages = self.pages.write().await;

        let page = pages.entry(page_num).or_insert_with(|| ClogPage::new(page_num));
        page.set_status(xid, status);
    }

    /// Mark transaction as committed
    pub async fn commit(&self, xid: u64) {
        self.set_status(xid, XidStatus::Committed).await;
    }

    /// Mark transaction as aborted
    pub async fn abort(&self, xid: u64) {
        self.set_status(xid, XidStatus::Aborted).await;
    }

    /// Check if committed
    pub async fn is_committed(&self, xid: u64) -> bool {
        self.get_status(xid).await == XidStatus::Committed
    }

    /// Check if aborted
    pub async fn is_aborted(&self, xid: u64) -> bool {
        self.get_status(xid).await == XidStatus::Aborted
    }

    /// Check if in progress
    pub async fn is_in_progress(&self, xid: u64) -> bool {
        self.get_status(xid).await == XidStatus::InProgress
    }

    /// Truncate old pages
    pub async fn truncate(&self, oldest_xid: u64) {
        let oldest_page = Self::get_page_number(oldest_xid);
        let mut pages = self.pages.write().await;
        pages.retain(|&page_num, _| page_num >= oldest_page);
    }

    /// Get page count
    pub async fn page_count(&self) -> usize {
        self.pages.read().await.len()
    }
}

impl Default for CommitLog {
    fn default() -> Self {
        Self::new()
    }
}
