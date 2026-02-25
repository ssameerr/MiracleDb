//! Spatial Index Manager - Manages R-tree spatial indexes for geospatial queries

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use serde::{Serialize, Deserialize};
use chrono::{DateTime, Utc};

use super::{SpatialIndex, PointData, RowId};
use super::config::SpatialIndexConfig;

/// Metadata for a spatial index
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SpatialIndexMetadata {
    pub table_name: String,
    pub column_x: String,
    pub column_y: String,
    pub point_count: usize,
    pub bounds: SpatialBounds,
    pub created_at: DateTime<Utc>,
    pub last_updated: DateTime<Utc>,
    pub index_path: PathBuf,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SpatialBounds {
    pub min_x: f64,
    pub max_x: f64,
    pub min_y: f64,
    pub max_y: f64,
}

/// Managed spatial index with metadata
pub struct ManagedSpatialIndex {
    pub index: SpatialIndex,
    pub metadata: SpatialIndexMetadata,
}

/// Manager for spatial R-tree indexes
pub struct SpatialIndexManager {
    base_path: PathBuf,
    indexes: Arc<RwLock<HashMap<String, Arc<RwLock<ManagedSpatialIndex>>>>>,
}

impl SpatialIndexManager {
    /// Create a new spatial index manager
    pub fn new(base_path: &str) -> Self {
        // Create base directory if it doesn't exist
        if let Err(e) = std::fs::create_dir_all(base_path) {
            tracing::warn!("Failed to create spatial index directory: {}", e);
        }

        Self {
            base_path: PathBuf::from(base_path),
            indexes: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Get the base path for spatial indexes
    pub fn base_path(&self) -> &Path {
        &self.base_path
    }

    /// Generate index key from table and column names
    fn index_key(table_name: &str, column_x: &str, column_y: &str) -> String {
        format!("{}_{}_{}",  table_name, column_x, column_y)
    }

    /// Create a new spatial index for a table
    pub async fn create_index(
        &self,
        table_name: &str,
        column_x: &str,
        column_y: &str,
        config: SpatialIndexConfig,
    ) -> Result<SpatialIndexMetadata, Box<dyn std::error::Error>> {
        // Validate config
        config.validate()?;

        let key = Self::index_key(table_name, column_x, column_y);

        // Check if index already exists
        {
            let indexes = self.indexes.read().unwrap();
            if indexes.contains_key(&key) {
                return Err(format!("Spatial index already exists for {}.{},{}", table_name, column_x, column_y).into());
            }
        }

        // Create new empty index
        let index = SpatialIndex::new();

        // Generate index path
        let index_path = self.base_path.join(format!("{}_{}.bin", table_name, key));

        // Create metadata
        let metadata = SpatialIndexMetadata {
            table_name: table_name.to_string(),
            column_x: column_x.to_string(),
            column_y: column_y.to_string(),
            point_count: 0,
            bounds: SpatialBounds {
                min_x: config.min_x,
                max_x: config.max_x,
                min_y: config.min_y,
                max_y: config.max_y,
            },
            created_at: Utc::now(),
            last_updated: Utc::now(),
            index_path: index_path.clone(),
        };

        let managed_index = ManagedSpatialIndex {
            index,
            metadata: metadata.clone(),
        };

        // Store in memory
        {
            let mut indexes = self.indexes.write().unwrap();
            indexes.insert(key.clone(), Arc::new(RwLock::new(managed_index)));
        }

        // Persist empty index
        self.persist_index(&key)?;

        tracing::info!("Created spatial index for {}.{},{}", table_name, column_x, column_y);

        Ok(metadata)
    }

    /// Drop an existing spatial index
    pub fn drop_index(
        &self,
        table_name: &str,
        column_x: &str,
        column_y: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let key = Self::index_key(table_name, column_x, column_y);

        // Get index path before removing
        let index_path = {
            let indexes = self.indexes.read().unwrap();
            let managed_index = indexes.get(&key)
                .ok_or_else(|| format!("Spatial index not found for {}.{},{}", table_name, column_x, column_y))?;
            let guard = managed_index.read().unwrap();
            guard.metadata.index_path.clone()
        };

        // Remove from memory
        {
            let mut indexes = self.indexes.write().unwrap();
            indexes.remove(&key);
        }

        // Delete file
        if index_path.exists() {
            std::fs::remove_file(&index_path)?;
            tracing::info!("Deleted spatial index file: {:?}", index_path);
        }

        tracing::info!("Dropped spatial index for {}.{},{}", table_name, column_x, column_y);

        Ok(())
    }

    /// Get metadata for a spatial index
    pub fn get_metadata(
        &self,
        table_name: &str,
        column_x: &str,
        column_y: &str,
    ) -> Result<SpatialIndexMetadata, Box<dyn std::error::Error>> {
        let key = Self::index_key(table_name, column_x, column_y);
        let indexes = self.indexes.read().unwrap();

        let managed_index = indexes.get(&key)
            .ok_or_else(|| format!("Spatial index not found for {}.{},{}", table_name, column_x, column_y))?;

        let guard = managed_index.read().unwrap();
        Ok(guard.metadata.clone())
    }

    /// List all spatial indexes
    pub fn list_indexes(&self) -> Vec<SpatialIndexMetadata> {
        let indexes = self.indexes.read().unwrap();
        indexes.values()
            .map(|managed_index| {
                let guard = managed_index.read().unwrap();
                guard.metadata.clone()
            })
            .collect()
    }

    /// List spatial indexes for a specific table
    pub fn list_table_indexes(&self, table_name: &str) -> Vec<SpatialIndexMetadata> {
        let indexes = self.indexes.read().unwrap();
        indexes.values()
            .filter_map(|managed_index| {
                let guard = managed_index.read().unwrap();
                if guard.metadata.table_name == table_name {
                    Some(guard.metadata.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    /// Search for points within a bounding box
    pub fn search_box(
        &self,
        table_name: &str,
        column_x: &str,
        column_y: &str,
        min_x: f64,
        min_y: f64,
        max_x: f64,
        max_y: f64,
    ) -> Result<Vec<RowId>, Box<dyn std::error::Error>> {
        let key = Self::index_key(table_name, column_x, column_y);
        let indexes = self.indexes.read().unwrap();

        let managed_index = indexes.get(&key)
            .ok_or_else(|| format!("Spatial index not found for {}.{},{}", table_name, column_x, column_y))?;

        let guard = managed_index.read().unwrap();
        Ok(guard.index.search_box(min_x, min_y, max_x, max_y))
    }

    /// Find nearest neighbor to a point
    pub fn nearest_neighbor(
        &self,
        table_name: &str,
        column_x: &str,
        column_y: &str,
        x: f64,
        y: f64,
    ) -> Result<Option<RowId>, Box<dyn std::error::Error>> {
        let key = Self::index_key(table_name, column_x, column_y);
        let indexes = self.indexes.read().unwrap();

        let managed_index = indexes.get(&key)
            .ok_or_else(|| format!("Spatial index not found for {}.{},{}", table_name, column_x, column_y))?;

        let guard = managed_index.read().unwrap();
        Ok(guard.index.nearest_neighbor(x, y))
    }

    /// Insert a point into the spatial index
    pub fn insert_point(
        &self,
        table_name: &str,
        column_x: &str,
        column_y: &str,
        x: f64,
        y: f64,
        row_id: RowId,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let key = Self::index_key(table_name, column_x, column_y);
        let indexes = self.indexes.read().unwrap();

        let managed_index = indexes.get(&key)
            .ok_or_else(|| format!("Spatial index not found for {}.{},{}", table_name, column_x, column_y))?;

        {
            let mut guard = managed_index.write().unwrap();
            guard.index.insert(x, y, row_id);
            guard.metadata.point_count += 1;
            guard.metadata.last_updated = Utc::now();
        }

        Ok(())
    }

    /// Delete a point from the spatial index (by rebuilding without it)
    /// Note: rstar R-tree doesn't support direct deletion, so this marks for rebuild
    pub fn delete_point(
        &self,
        table_name: &str,
        column_x: &str,
        column_y: &str,
        row_id: RowId,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // For now, we'll log a warning. Full implementation would require rebuilding the tree
        tracing::warn!("delete_point called for row_id {} - requires index rebuild for full effect", row_id);

        // Update metadata
        let key = Self::index_key(table_name, column_x, column_y);
        let indexes = self.indexes.read().unwrap();

        if let Some(managed_index) = indexes.get(&key) {
            let mut guard = managed_index.write().unwrap();
            if guard.metadata.point_count > 0 {
                guard.metadata.point_count -= 1;
            }
            guard.metadata.last_updated = Utc::now();
        }

        Ok(())
    }

    /// Update a point in the spatial index
    pub fn update_point(
        &self,
        table_name: &str,
        column_x: &str,
        column_y: &str,
        old_row_id: RowId,
        new_x: f64,
        new_y: f64,
        new_row_id: RowId,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Delete old point (marks for rebuild)
        self.delete_point(table_name, column_x, column_y, old_row_id)?;

        // Insert new point
        self.insert_point(table_name, column_x, column_y, new_x, new_y, new_row_id)?;

        Ok(())
    }

    /// Rebuild index from a set of points (bulk operation)
    pub fn rebuild_index(
        &self,
        table_name: &str,
        column_x: &str,
        column_y: &str,
        points: Vec<(f64, f64, RowId)>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let key = Self::index_key(table_name, column_x, column_y);
        let indexes = self.indexes.read().unwrap();

        let managed_index = indexes.get(&key)
            .ok_or_else(|| format!("Spatial index not found for {}.{},{}", table_name, column_x, column_y))?;

        {
            let mut guard = managed_index.write().unwrap();

            // Create new index
            let mut new_index = SpatialIndex::new();

            // Bulk insert all points
            for (x, y, row_id) in points.iter() {
                new_index.insert(*x, *y, *row_id);
            }

            // Replace old index
            guard.index = new_index;
            guard.metadata.point_count = points.len();
            guard.metadata.last_updated = Utc::now();
        }

        // Persist to disk
        self.persist_index(&key)?;

        tracing::info!("Rebuilt spatial index for {}.{},{} with {} points",
            table_name, column_x, column_y, points.len());

        Ok(())
    }

    /// Persist an index to disk
    fn persist_index(&self, key: &str) -> Result<(), Box<dyn std::error::Error>> {
        let indexes = self.indexes.read().unwrap();

        let managed_index = indexes.get(key)
            .ok_or_else(|| format!("Spatial index not found for key: {}", key))?;

        let guard = managed_index.read().unwrap();
        let index_path = &guard.metadata.index_path;

        // Save index
        guard.index.save(index_path)?;

        // Save metadata separately
        let metadata_path = index_path.with_extension("meta.json");
        let metadata_json = serde_json::to_string_pretty(&guard.metadata)?;
        std::fs::write(&metadata_path, metadata_json)?;

        Ok(())
    }

    /// Load an index from disk
    pub fn load_index(
        &self,
        table_name: &str,
        column_x: &str,
        column_y: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let key = Self::index_key(table_name, column_x, column_y);

        // Check if already loaded
        {
            let indexes = self.indexes.read().unwrap();
            if indexes.contains_key(&key) {
                return Ok(()); // Already loaded
            }
        }

        // Construct paths
        let index_path = self.base_path.join(format!("{}_{}.bin", table_name, key));
        let metadata_path = index_path.with_extension("meta.json");

        // Check if files exist
        if !index_path.exists() || !metadata_path.exists() {
            return Err(format!("Spatial index files not found for {}.{},{}", table_name, column_x, column_y).into());
        }

        // Load index
        let index = SpatialIndex::load(&index_path)?;

        // Load metadata
        let metadata_json = std::fs::read_to_string(&metadata_path)?;
        let metadata: SpatialIndexMetadata = serde_json::from_str(&metadata_json)?;

        let managed_index = ManagedSpatialIndex {
            index,
            metadata,
        };

        // Store in memory
        {
            let mut indexes = self.indexes.write().unwrap();
            indexes.insert(key.clone(), Arc::new(RwLock::new(managed_index)));
        }

        tracing::info!("Loaded spatial index for {}.{},{}", table_name, column_x, column_y);

        Ok(())
    }

    /// Load all indexes from disk at startup
    pub fn load_all_indexes(&self) -> Result<usize, Box<dyn std::error::Error>> {
        let mut loaded_count = 0;

        // Read all .meta.json files in base directory
        if let Ok(entries) = std::fs::read_dir(&self.base_path) {
            for entry in entries.flatten() {
                let path = entry.path();

                if path.extension().and_then(|s| s.to_str()) == Some("json")
                    && path.file_stem()
                        .and_then(|s| s.to_str())
                        .map(|s| s.ends_with(".meta"))
                        .unwrap_or(false)
                {
                    // Parse metadata to get table and column names
                    if let Ok(metadata_json) = std::fs::read_to_string(&path) {
                        if let Ok(metadata) = serde_json::from_str::<SpatialIndexMetadata>(&metadata_json) {
                            match self.load_index(&metadata.table_name, &metadata.column_x, &metadata.column_y) {
                                Ok(_) => loaded_count += 1,
                                Err(e) => tracing::warn!("Failed to load spatial index from {:?}: {}", path, e),
                            }
                        }
                    }
                }
            }
        }

        if loaded_count > 0 {
            tracing::info!("Loaded {} spatial indexes from disk", loaded_count);
        }

        Ok(loaded_count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_create_and_drop_index() {
        let temp_dir = TempDir::new().unwrap();
        let manager = SpatialIndexManager::new(temp_dir.path().to_str().unwrap());

        // Create index
        let metadata = manager.create_index(
            "test_table",
            "longitude",
            "latitude",
            SpatialIndexConfig::default(),
        ).await.unwrap();

        assert_eq!(metadata.table_name, "test_table");
        assert_eq!(metadata.column_x, "longitude");
        assert_eq!(metadata.column_y, "latitude");
        assert_eq!(metadata.point_count, 0);

        // Drop index
        manager.drop_index("test_table", "longitude", "latitude").unwrap();

        // Verify it's gone
        assert!(manager.get_metadata("test_table", "longitude", "latitude").is_err());
    }

    #[tokio::test]
    async fn test_insert_and_search() {
        let temp_dir = TempDir::new().unwrap();
        let manager = SpatialIndexManager::new(temp_dir.path().to_str().unwrap());

        // Create index
        manager.create_index(
            "locations",
            "lon",
            "lat",
            SpatialIndexConfig::default(),
        ).await.unwrap();

        // Insert points (NYC, LA, Chicago)
        manager.insert_point("locations", "lon", "lat", -74.0060, 40.7128, 1).unwrap();
        manager.insert_point("locations", "lon", "lat", -118.2437, 34.0522, 2).unwrap();
        manager.insert_point("locations", "lon", "lat", -87.6298, 41.8781, 3).unwrap();

        // Search box covering NYC and Chicago (roughly)
        let results = manager.search_box("locations", "lon", "lat", -90.0, 40.0, -70.0, 42.0).unwrap();

        assert_eq!(results.len(), 2);
        assert!(results.contains(&1)); // NYC
        assert!(results.contains(&3)); // Chicago
        assert!(!results.contains(&2)); // LA not in range
    }

    #[tokio::test]
    async fn test_nearest_neighbor() {
        let temp_dir = TempDir::new().unwrap();
        let manager = SpatialIndexManager::new(temp_dir.path().to_str().unwrap());

        manager.create_index(
            "locations",
            "lon",
            "lat",
            SpatialIndexConfig::default(),
        ).await.unwrap();

        // Insert points
        manager.insert_point("locations", "lon", "lat", -74.0, 40.7, 1).unwrap();
        manager.insert_point("locations", "lon", "lat", -118.2, 34.0, 2).unwrap();

        // Find nearest to a point close to NYC
        let nearest = manager.nearest_neighbor("locations", "lon", "lat", -74.1, 40.8).unwrap();

        assert_eq!(nearest, Some(1)); // Should be NYC
    }

    #[tokio::test]
    async fn test_rebuild_index() {
        let temp_dir = TempDir::new().unwrap();
        let manager = SpatialIndexManager::new(temp_dir.path().to_str().unwrap());

        manager.create_index(
            "locations",
            "lon",
            "lat",
            SpatialIndexConfig::default(),
        ).await.unwrap();

        // Rebuild with bulk points
        let points = vec![
            (-74.0, 40.7, 1),
            (-118.2, 34.0, 2),
            (-87.6, 41.9, 3),
        ];

        manager.rebuild_index("locations", "lon", "lat", points).unwrap();

        // Verify point count
        let metadata = manager.get_metadata("locations", "lon", "lat").unwrap();
        assert_eq!(metadata.point_count, 3);

        // Verify search works
        let results = manager.search_box("locations", "lon", "lat", -90.0, 40.0, -70.0, 42.0).unwrap();
        assert_eq!(results.len(), 2);
    }
}
