pub mod config;
pub mod manager;
pub mod optimizer;
pub mod table_provider;

pub use config::SpatialIndexConfig;
pub use manager::{SpatialIndexManager, SpatialIndexMetadata, SpatialBounds};
pub use optimizer::SpatialFilterPushdown;
pub use table_provider::SpatialTableProvider;

use rstar::{RTree, RTreeObject, AABB};
use serde::{Serialize, Deserialize};
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::Path;

pub type RowId = u64;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Copy)]
pub struct PointData {
    pub x: f64,
    pub y: f64,
    pub row_id: RowId,
}

impl rstar::Point for PointData {
    type Scalar = f64;
    const DIMENSIONS: usize = 2;

    fn generate(mut generator: impl FnMut(usize) -> Self::Scalar) -> Self {
        PointData {
            x: generator(0),
            y: generator(1),
            row_id: 0, // Placeholder
        }
    }

    fn nth(&self, index: usize) -> Self::Scalar {
        match index {
            0 => self.x,
            1 => self.y,
            _ => unreachable!(),
        }
    }

    fn nth_mut(&mut self, index: usize) -> &mut Self::Scalar {
         match index {
            0 => &mut self.x,
            1 => &mut self.y,
            _ => unreachable!(),
        }
    }
}

pub struct SpatialIndex {
    tree: RTree<PointData>,
}

impl SpatialIndex {
    pub fn new() -> Self {
        Self {
            tree: RTree::new(),
        }
    }

    pub fn insert(&mut self, x: f64, y: f64, row_id: RowId) {
        let point = PointData { x, y, row_id };
        self.tree.insert(point);
    }

    pub fn search_box(&self, min_x: f64, min_y: f64, max_x: f64, max_y: f64) -> Vec<RowId> {
        let min = PointData { x: min_x, y: min_y, row_id: 0 };
        let max = PointData { x: max_x, y: max_y, row_id: 0 };
        let aabb = AABB::from_corners(min, max);
        
        self.tree.locate_in_envelope(&aabb)
            .map(|p| p.row_id)
            .collect()
    }

    pub fn nearest_neighbor(&self, x: f64, y: f64) -> Option<RowId> {
        let point = PointData { x, y, row_id: 0 };
        self.tree.nearest_neighbor(&point).map(|p| p.row_id)
    }

    pub fn save(&self, path: &Path) -> Result<(), Box<dyn std::error::Error>> {
        let file = File::create(path)?;
        let writer = BufWriter::new(file);
        // RTree might not implement Serialize directly? 
        // rstar 0.12 supports serde if feature is enabled.
        // But RTree serializes as a list of nodes/elements?
        // We might need to serialize the elements and rebuild on load if RTree def doesn't derive Serialize.
        // rstar doc says: "RTree implements Serialize and Deserialize" if feature "serde" is enabled.
        bincode::serialize_into(writer, &self.tree)?;
        Ok(())
    }

    pub fn load(path: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let tree: RTree<PointData> = bincode::deserialize_from(reader)?;
        Ok(Self { tree })
    }
}
