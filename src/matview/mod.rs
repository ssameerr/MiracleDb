//! Materialized View Module - Materialized view management
//!
//! Also provides OLAP helpers (ROLLUP, CUBE, window functions) via the
//! `olap` submodule.

pub mod olap;
pub use olap::{
    rollup_combinations,
    cube_combinations,
    GroupingSets,
    FrameBound,
    FrameUnit,
    WindowFrame,
    OlapFunction,
    WindowSpec,
    OlapAggregation,
};

use std::collections::HashMap;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

/// Materialized view definition
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MaterializedView {
    pub name: String,
    pub schema: String,
    pub query: String,
    pub columns: Vec<MViewColumn>,
    pub owner: String,
    pub tablespace: Option<String>,
    pub with_data: bool,
    pub created_at: i64,
    pub last_refresh: Option<i64>,
    pub row_count: u64,
    pub size_bytes: u64,
    pub indexes: Vec<String>,
    pub refresh_policy: Option<RefreshPolicy>,
}

/// Materialized view column
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MViewColumn {
    pub name: String,
    pub data_type: String,
    pub position: u32,
}

/// Refresh policy
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RefreshPolicy {
    pub auto_refresh: bool,
    pub refresh_interval_seconds: u64,
    pub concurrent: bool,
    pub on_commit: bool,
}

impl Default for RefreshPolicy {
    fn default() -> Self {
        Self {
            auto_refresh: false,
            refresh_interval_seconds: 3600,
            concurrent: false,
            on_commit: false,
        }
    }
}

impl MaterializedView {
    pub fn new(schema: &str, name: &str, query: &str) -> Self {
        Self {
            name: name.to_string(),
            schema: schema.to_string(),
            query: query.to_string(),
            columns: vec![],
            owner: "postgres".to_string(),
            tablespace: None,
            with_data: true,
            created_at: chrono::Utc::now().timestamp(),
            last_refresh: None,
            row_count: 0,
            size_bytes: 0,
            indexes: vec![],
            refresh_policy: None,
        }
    }

    pub fn without_data(mut self) -> Self {
        self.with_data = false;
        self
    }

    pub fn with_tablespace(mut self, tablespace: &str) -> Self {
        self.tablespace = Some(tablespace.to_string());
        self
    }

    pub fn with_refresh_policy(mut self, policy: RefreshPolicy) -> Self {
        self.refresh_policy = Some(policy);
        self
    }

    pub fn full_name(&self) -> String {
        format!("{}.{}", self.schema, self.name)
    }

    /// Check if refresh is needed based on policy
    pub fn needs_refresh(&self) -> bool {
        if let Some(ref policy) = self.refresh_policy {
            if !policy.auto_refresh {
                return false;
            }

            if let Some(last_refresh) = self.last_refresh {
                let now = chrono::Utc::now().timestamp();
                let elapsed = now - last_refresh;
                return elapsed >= policy.refresh_interval_seconds as i64;
            }

            return true; // Never refreshed
        }

        false
    }
}

/// Refresh options
#[derive(Clone, Debug, Default)]
pub struct RefreshOptions {
    pub concurrent: bool,
    pub with_data: bool,
}

/// Materialized view manager
pub struct MaterializedViewManager {
    mviews: RwLock<HashMap<String, MaterializedView>>,
}

impl MaterializedViewManager {
    pub fn new() -> Self {
        Self {
            mviews: RwLock::new(HashMap::new()),
        }
    }

    /// Create a materialized view
    pub async fn create(&self, mview: MaterializedView) -> Result<(), String> {
        let mut mviews = self.mviews.write().await;
        let key = mview.full_name();

        if mviews.contains_key(&key) {
            return Err(format!("Materialized view {} already exists", key));
        }

        mviews.insert(key, mview);
        Ok(())
    }

    /// Drop a materialized view
    pub async fn drop(&self, schema: &str, name: &str) -> Result<(), String> {
        let mut mviews = self.mviews.write().await;
        let key = format!("{}.{}", schema, name);

        mviews.remove(&key)
            .ok_or_else(|| format!("Materialized view {} not found", key))?;
        Ok(())
    }

    /// Get a materialized view
    pub async fn get(&self, schema: &str, name: &str) -> Option<MaterializedView> {
        let mviews = self.mviews.read().await;
        let key = format!("{}.{}", schema, name);
        mviews.get(&key).cloned()
    }

    /// List materialized views
    pub async fn list(&self, schema: Option<&str>) -> Vec<MaterializedView> {
        let mviews = self.mviews.read().await;
        mviews.values()
            .filter(|mv| schema.map(|s| mv.schema == s).unwrap_or(true))
            .cloned()
            .collect()
    }

    /// Refresh a materialized view
    pub async fn refresh(&self, schema: &str, name: &str, options: RefreshOptions) -> Result<RefreshResult, String> {
        let mut mviews = self.mviews.write().await;
        let key = format!("{}.{}", schema, name);

        let mview = mviews.get_mut(&key)
            .ok_or_else(|| format!("Materialized view {} not found", key))?;

        let start_time = std::time::Instant::now();

        // In production: execute query and populate data
        mview.last_refresh = Some(chrono::Utc::now().timestamp());
        mview.row_count = 1000; // Simulated
        mview.size_bytes = 50000; // Simulated

        Ok(RefreshResult {
            mview_name: key,
            rows_inserted: mview.row_count,
            duration_ms: start_time.elapsed().as_millis() as u64,
            concurrent: options.concurrent,
        })
    }

    /// Get views that need refresh
    pub async fn get_needing_refresh(&self) -> Vec<MaterializedView> {
        let mviews = self.mviews.read().await;
        mviews.values()
            .filter(|mv| mv.needs_refresh())
            .cloned()
            .collect()
    }

    /// Update statistics
    pub async fn update_stats(&self, schema: &str, name: &str, row_count: u64, size_bytes: u64) {
        let mut mviews = self.mviews.write().await;
        let key = format!("{}.{}", schema, name);

        if let Some(mview) = mviews.get_mut(&key) {
            mview.row_count = row_count;
            mview.size_bytes = size_bytes;
        }
    }
}

impl Default for MaterializedViewManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Refresh result
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RefreshResult {
    pub mview_name: String,
    pub rows_inserted: u64,
    pub duration_ms: u64,
    pub concurrent: bool,
}

/// Convenience alias – shorter name used by the Wave 2 OLAP task and external
/// callers that prefer the abbreviated form.
pub type MatViewManager = MaterializedViewManager;
