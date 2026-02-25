//! Configuration for spatial indexing

use serde::{Serialize, Deserialize};

/// Configuration for creating a spatial R-tree index
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SpatialIndexConfig {
    /// Minimum X coordinate bound (longitude min)
    pub min_x: f64,
    /// Maximum X coordinate bound (longitude max)
    pub max_x: f64,
    /// Minimum Y coordinate bound (latitude min)
    pub min_y: f64,
    /// Maximum Y coordinate bound (latitude max)
    pub max_y: f64,
    /// Path where the index will be persisted
    pub index_path: Option<String>,
}

impl Default for SpatialIndexConfig {
    fn default() -> Self {
        Self {
            // Default: full geographic bounds (WGS84)
            min_x: -180.0,
            max_x: 180.0,
            min_y: -90.0,
            max_y: 90.0,
            index_path: None,
        }
    }
}

impl SpatialIndexConfig {
    /// Create a new configuration with custom bounds
    pub fn with_bounds(min_x: f64, min_y: f64, max_x: f64, max_y: f64) -> Self {
        Self {
            min_x,
            max_x,
            min_y,
            max_y,
            index_path: None,
        }
    }

    /// Set the index persistence path
    pub fn with_path(mut self, path: String) -> Self {
        self.index_path = Some(path);
        self
    }

    /// Validate that the bounds are correct
    pub fn validate(&self) -> Result<(), String> {
        if self.min_x >= self.max_x {
            return Err(format!("min_x ({}) must be less than max_x ({})", self.min_x, self.max_x));
        }
        if self.min_y >= self.max_y {
            return Err(format!("min_y ({}) must be less than max_y ({})", self.min_y, self.max_y));
        }
        Ok(())
    }
}
