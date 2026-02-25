//! Cursor Module - Database cursors for result iteration

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

/// Cursor
#[derive(Clone, Debug)]
pub struct Cursor {
    pub id: String,
    pub name: Option<String>,
    pub query: String,
    pub rows: Vec<serde_json::Value>,
    pub position: usize,
    pub fetch_size: usize,
    pub created_at: i64,
    pub last_accessed: i64,
    pub scrollable: bool,
    pub holdable: bool,
}

/// Cursor state
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum CursorPosition {
    BeforeFirst,
    AtRow(usize),
    AfterLast,
}

/// Fetch direction
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum FetchDirection {
    Next,
    Prior,
    First,
    Last,
    Absolute(i64),
    Relative(i64),
}

/// Fetch result
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FetchResult {
    pub rows: Vec<serde_json::Value>,
    pub position: CursorPosition,
    pub has_more: bool,
}

impl Cursor {
    pub fn new(id: &str, query: &str, rows: Vec<serde_json::Value>) -> Self {
        let now = chrono::Utc::now().timestamp();
        Self {
            id: id.to_string(),
            name: None,
            query: query.to_string(),
            rows,
            position: 0,
            fetch_size: 100,
            created_at: now,
            last_accessed: now,
            scrollable: true,
            holdable: false,
        }
    }

    pub fn with_name(mut self, name: &str) -> Self {
        self.name = Some(name.to_string());
        self
    }

    pub fn with_fetch_size(mut self, size: usize) -> Self {
        self.fetch_size = size;
        self
    }

    pub fn with_scrollable(mut self, scrollable: bool) -> Self {
        self.scrollable = scrollable;
        self
    }

    pub fn with_holdable(mut self, holdable: bool) -> Self {
        self.holdable = holdable;
        self
    }

    /// Fetch rows
    pub fn fetch(&mut self, direction: FetchDirection, count: Option<usize>) -> FetchResult {
        self.last_accessed = chrono::Utc::now().timestamp();
        let count = count.unwrap_or(self.fetch_size);

        match direction {
            FetchDirection::Next => self.fetch_next(count),
            FetchDirection::Prior => self.fetch_prior(count),
            FetchDirection::First => self.fetch_first(count),
            FetchDirection::Last => self.fetch_last(count),
            FetchDirection::Absolute(pos) => self.fetch_absolute(pos, count),
            FetchDirection::Relative(offset) => self.fetch_relative(offset, count),
        }
    }

    fn fetch_next(&mut self, count: usize) -> FetchResult {
        let start = self.position;
        let end = (start + count).min(self.rows.len());
        let rows = self.rows[start..end].to_vec();
        self.position = end;

        FetchResult {
            rows,
            position: if end >= self.rows.len() {
                CursorPosition::AfterLast
            } else {
                CursorPosition::AtRow(self.position)
            },
            has_more: end < self.rows.len(),
        }
    }

    fn fetch_prior(&mut self, count: usize) -> FetchResult {
        if !self.scrollable {
            return FetchResult {
                rows: vec![],
                position: CursorPosition::AtRow(self.position),
                has_more: false,
            };
        }

        let end = self.position;
        let start = end.saturating_sub(count);
        let rows = self.rows[start..end].to_vec();
        self.position = start;

        FetchResult {
            rows,
            position: if start == 0 {
                CursorPosition::BeforeFirst
            } else {
                CursorPosition::AtRow(self.position)
            },
            has_more: start > 0,
        }
    }

    fn fetch_first(&mut self, count: usize) -> FetchResult {
        self.position = 0;
        self.fetch_next(count)
    }

    fn fetch_last(&mut self, count: usize) -> FetchResult {
        self.position = self.rows.len().saturating_sub(count);
        self.fetch_next(count)
    }

    fn fetch_absolute(&mut self, pos: i64, count: usize) -> FetchResult {
        let new_pos = if pos >= 0 {
            pos as usize
        } else {
            self.rows.len().saturating_sub((-pos) as usize)
        };
        self.position = new_pos.min(self.rows.len());
        self.fetch_next(count)
    }

    fn fetch_relative(&mut self, offset: i64, count: usize) -> FetchResult {
        let new_pos = if offset >= 0 {
            self.position.saturating_add(offset as usize)
        } else {
            self.position.saturating_sub((-offset) as usize)
        };
        self.position = new_pos.min(self.rows.len());
        self.fetch_next(count)
    }

    /// Move cursor position
    pub fn move_to(&mut self, direction: FetchDirection) -> CursorPosition {
        match direction {
            FetchDirection::Next => {
                self.position = (self.position + 1).min(self.rows.len());
            }
            FetchDirection::Prior if self.scrollable => {
                self.position = self.position.saturating_sub(1);
            }
            FetchDirection::First => {
                self.position = 0;
            }
            FetchDirection::Last => {
                self.position = self.rows.len().saturating_sub(1);
            }
            FetchDirection::Absolute(pos) => {
                self.position = (pos.max(0) as usize).min(self.rows.len());
            }
            FetchDirection::Relative(offset) => {
                let new_pos = (self.position as i64 + offset).max(0) as usize;
                self.position = new_pos.min(self.rows.len());
            }
            _ => {}
        }

        if self.position == 0 && self.rows.is_empty() {
            CursorPosition::AfterLast
        } else if self.position >= self.rows.len() {
            CursorPosition::AfterLast
        } else {
            CursorPosition::AtRow(self.position)
        }
    }

    /// Get current row
    pub fn current(&self) -> Option<&serde_json::Value> {
        self.rows.get(self.position)
    }

    /// Get row count
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    /// Check if at end
    pub fn is_at_end(&self) -> bool {
        self.position >= self.rows.len()
    }

    /// Check if at beginning
    pub fn is_at_beginning(&self) -> bool {
        self.position == 0
    }
}

/// Cursor manager
pub struct CursorManager {
    cursors: RwLock<HashMap<String, Cursor>>,
    max_cursors: usize,
    default_timeout_seconds: u64,
}

impl CursorManager {
    pub fn new(max_cursors: usize) -> Self {
        Self {
            cursors: RwLock::new(HashMap::new()),
            max_cursors,
            default_timeout_seconds: 3600,
        }
    }

    /// Declare a cursor
    pub async fn declare(&self, query: &str, rows: Vec<serde_json::Value>) -> String {
        let id = uuid::Uuid::new_v4().to_string();
        let cursor = Cursor::new(&id, query, rows);

        let mut cursors = self.cursors.write().await;

        // Remove old cursors if at limit
        if cursors.len() >= self.max_cursors {
            let oldest = cursors.values()
                .min_by_key(|c| c.last_accessed)
                .map(|c| c.id.clone());
            if let Some(old_id) = oldest {
                cursors.remove(&old_id);
            }
        }

        cursors.insert(id.clone(), cursor);
        id
    }

    /// Fetch from cursor
    pub async fn fetch(&self, cursor_id: &str, direction: FetchDirection, count: Option<usize>) -> Option<FetchResult> {
        let mut cursors = self.cursors.write().await;
        let cursor = cursors.get_mut(cursor_id)?;
        Some(cursor.fetch(direction, count))
    }

    /// Close cursor
    pub async fn close(&self, cursor_id: &str) {
        let mut cursors = self.cursors.write().await;
        cursors.remove(cursor_id);
    }

    /// Get cursor info
    pub async fn get_info(&self, cursor_id: &str) -> Option<CursorInfo> {
        let cursors = self.cursors.read().await;
        let cursor = cursors.get(cursor_id)?;

        Some(CursorInfo {
            id: cursor.id.clone(),
            name: cursor.name.clone(),
            position: cursor.position,
            row_count: cursor.rows.len(),
            created_at: cursor.created_at,
            last_accessed: cursor.last_accessed,
        })
    }

    /// Cleanup expired cursors
    pub async fn cleanup(&self) {
        let now = chrono::Utc::now().timestamp();
        let timeout = self.default_timeout_seconds as i64;

        let mut cursors = self.cursors.write().await;
        cursors.retain(|_, c| {
            c.holdable || (now - c.last_accessed) < timeout
        });
    }
}

impl Default for CursorManager {
    fn default() -> Self {
        Self::new(1000)
    }
}

/// Cursor info
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CursorInfo {
    pub id: String,
    pub name: Option<String>,
    pub position: usize,
    pub row_count: usize,
    pub created_at: i64,
    pub last_accessed: i64,
}
