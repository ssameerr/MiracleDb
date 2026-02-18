//! Buffer Pool - Page cache for I/O reduction
use std::sync::Arc;
use tokio::sync::RwLock;
use lru::LruCache;
use std::num::NonZeroUsize;

pub const PAGE_SIZE: usize = 8192; // 8KB pages

#[derive(Clone, Debug)]
pub struct Page {
    pub id: u64,
    pub data: Vec<u8>,
    pub dirty: bool,
    pub pin_count: u32,
}

impl Page {
    pub fn new(id: u64) -> Self {
        Self {
            id,
            data: vec![0u8; PAGE_SIZE],
            dirty: false,
            pin_count: 0,
        }
    }
}

pub struct PageBufferPool {
    pages: RwLock<LruCache<u64, Page>>,
    capacity: usize,
    hits: std::sync::atomic::AtomicU64,
    misses: std::sync::atomic::AtomicU64,
}

impl PageBufferPool {
    pub fn new(capacity_pages: usize) -> Self {
        Self {
            pages: RwLock::new(LruCache::new(NonZeroUsize::new(capacity_pages).unwrap())),
            capacity: capacity_pages,
            hits: std::sync::atomic::AtomicU64::new(0),
            misses: std::sync::atomic::AtomicU64::new(0),
        }
    }

    pub async fn fetch_page(&self, page_id: u64) -> Option<Page> {
        let mut pages = self.pages.write().await;
        if let Some(page) = pages.get(&page_id) {
            self.hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Some(page.clone())
        } else {
            self.misses.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            None
        }
    }

    pub async fn store_page(&self, page: Page) {
        let mut pages = self.pages.write().await;
        pages.put(page.id, page);
    }

    pub async fn mark_dirty(&self, page_id: u64) {
        let mut pages = self.pages.write().await;
        if let Some(page) = pages.get_mut(&page_id) {
            page.dirty = true;
        }
    }

    pub async fn flush_dirty(&self) -> usize {
        let mut pages = self.pages.write().await;
        let mut flushed = 0;
        for (_, page) in pages.iter_mut() {
            if page.dirty {
                page.dirty = false;
                flushed += 1;
            }
        }
        flushed
    }

    pub fn hit_rate(&self) -> f64 {
        let hits = self.hits.load(std::sync::atomic::Ordering::Relaxed);
        let misses = self.misses.load(std::sync::atomic::Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 { 0.0 } else { hits as f64 / total as f64 }
    }

    pub fn stats(&self) -> (u64, u64, f64) {
        let hits = self.hits.load(std::sync::atomic::Ordering::Relaxed);
        let misses = self.misses.load(std::sync::atomic::Ordering::Relaxed);
        (hits, misses, self.hit_rate())
    }
}

impl Default for PageBufferPool {
    fn default() -> Self {
        Self::new(1024) // 8MB default (1024 * 8KB)
    }
}
