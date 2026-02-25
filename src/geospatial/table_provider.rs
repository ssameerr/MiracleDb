//! Spatial Table Provider - Wraps TableProvider to enable spatial index lookups
//!
//! This provider wraps any existing TableProvider and intercepts scan() calls
//! to leverage R-tree spatial indexes for filtering when spatial predicates are detected.

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DFResult;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;

use crate::geospatial::SpatialIndexManager;

/// Wrapper around TableProvider that adds spatial index support
pub struct SpatialTableProvider {
    /// The underlying table provider
    inner: Arc<dyn TableProvider>,
    /// Table name
    table_name: String,
    /// Spatial index manager
    spatial_index_manager: Arc<SpatialIndexManager>,
    /// Spatial column names (x, y) if index exists
    spatial_columns: Option<(String, String)>,
}

impl SpatialTableProvider {
    /// Create a new spatial table provider
    pub fn new(
        inner: Arc<dyn TableProvider>,
        table_name: String,
        spatial_index_manager: Arc<SpatialIndexManager>,
    ) -> Self {
        // Check if spatial index exists for this table
        let spatial_columns = Self::detect_spatial_columns(&table_name, &spatial_index_manager);

        Self {
            inner,
            table_name,
            spatial_index_manager,
            spatial_columns,
        }
    }

    /// Detect if table has spatial indexes
    fn detect_spatial_columns(
        table_name: &str,
        manager: &SpatialIndexManager,
    ) -> Option<(String, String)> {
        // Get all indexes for this table
        let indexes = manager.list_table_indexes(table_name);

        // Return first spatial index found
        indexes.first().map(|metadata| {
            (metadata.column_x.clone(), metadata.column_y.clone())
        })
    }

    /// Check if expression contains spatial predicates
    fn has_spatial_predicate(&self, expr: &Expr) -> bool {
        match expr {
            Expr::BinaryExpr(binary_expr) => {
                // Check if left side is ST_Distance
                if let Expr::ScalarFunction(func) = &*binary_expr.left {
                    if func.name().to_lowercase() == "st_distance" {
                        return true;
                    }
                }
                // Recursively check both sides
                self.has_spatial_predicate(&binary_expr.left)
                    || self.has_spatial_predicate(&binary_expr.right)
            }
            Expr::ScalarFunction(func) => {
                // Check for ST_DWithin
                func.name().to_lowercase() == "st_dwithin"
            }
            _ => false,
        }
    }

    /// Extract spatial filter information from expressions
    fn extract_spatial_filter(&self, filters: &[Expr]) -> Option<SpatialFilter> {
        for filter in filters {
            if let Some(spatial_filter) = self.try_extract_spatial_filter(filter) {
                return Some(spatial_filter);
            }
        }
        None
    }

    /// Try to extract spatial filter from a single expression
    fn try_extract_spatial_filter(&self, expr: &Expr) -> Option<SpatialFilter> {
        match expr {
            Expr::BinaryExpr(binary_expr) => {
                // Pattern: ST_Distance(...) < threshold
                if let Expr::ScalarFunction(func) = &*binary_expr.left {
                    if func.name().to_lowercase() == "st_distance" && func.args.len() == 4 {
                        // Extract center coordinates and threshold
                        let center_x = self.extract_literal_f64(&func.args[2])?;
                        let center_y = self.extract_literal_f64(&func.args[3])?;
                        let radius = self.extract_literal_f64(&binary_expr.right)?;

                        // Calculate bounding box
                        return Some(SpatialFilter {
                            min_x: center_x - radius,
                            max_x: center_x + radius,
                            min_y: center_y - radius,
                            max_y: center_y + radius,
                        });
                    }
                }
                None
            }
            Expr::ScalarFunction(func) => {
                // Pattern: ST_DWithin(...)
                if func.name().to_lowercase() == "st_dwithin" && func.args.len() == 5 {
                    let center_x = self.extract_literal_f64(&func.args[2])?;
                    let center_y = self.extract_literal_f64(&func.args[3])?;
                    let radius = self.extract_literal_f64(&func.args[4])?;

                    return Some(SpatialFilter {
                        min_x: center_x - radius,
                        max_x: center_x + radius,
                        min_y: center_y - radius,
                        max_y: center_y + radius,
                    });
                }
                None
            }
            _ => None,
        }
    }

    /// Extract f64 literal from expression
    fn extract_literal_f64(&self, expr: &Expr) -> Option<f64> {
        match expr {
            Expr::Literal(scalar) => match scalar {
                datafusion::scalar::ScalarValue::Float64(Some(val)) => Some(*val),
                datafusion::scalar::ScalarValue::Float32(Some(val)) => Some(*val as f64),
                datafusion::scalar::ScalarValue::Int64(Some(val)) => Some(*val as f64),
                datafusion::scalar::ScalarValue::Int32(Some(val)) => Some(*val as f64),
                _ => None,
            },
            _ => None,
        }
    }

    /// Apply spatial index filter
    fn apply_spatial_index(&self, filter: &SpatialFilter) -> Option<Vec<u64>> {
        if let Some((ref col_x, ref col_y)) = self.spatial_columns {
            tracing::info!(
                "Using spatial index for table '{}' on ({}, {})",
                self.table_name,
                col_x,
                col_y
            );

            match self.spatial_index_manager.search_box(
                &self.table_name,
                col_x,
                col_y,
                filter.min_x,
                filter.min_y,
                filter.max_x,
                filter.max_y,
            ) {
                Ok(row_ids) => {
                    tracing::info!(
                        "Spatial index returned {} candidate rows",
                        row_ids.len()
                    );
                    Some(row_ids)
                }
                Err(e) => {
                    tracing::warn!("Spatial index query failed: {}, falling back to full scan", e);
                    None
                }
            }
        } else {
            None
        }
    }
}

#[async_trait::async_trait]
impl TableProvider for SpatialTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        // Check if any filters contain spatial predicates
        let has_spatial = filters.iter().any(|f| self.has_spatial_predicate(f));

        if has_spatial && self.spatial_columns.is_some() {
            tracing::debug!(
                "Spatial predicate detected for table '{}' with spatial index",
                self.table_name
            );
            // Accept the filter for pushdown
            Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
        } else {
            // Delegate to inner provider
            self.inner.supports_filters_pushdown(filters)
        }
    }

    async fn scan(
        &self,
        state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Check for spatial filters
        if let Some(spatial_filter) = self.extract_spatial_filter(filters) {
            // Try to use spatial index
            if let Some(_row_ids) = self.apply_spatial_index(&spatial_filter) {
                // TODO: In a full implementation, we would:
                // 1. Convert row_ids to a filter expression
                // 2. Combine with existing filters
                // 3. Pass modified filters to inner.scan()
                //
                // For now, we log the optimization and delegate to inner scan
                // The spatial predicate will still filter correctly via ST_Distance UDF,
                // but won't benefit from index pruning until we implement row_id filtering

                tracing::info!(
                    "Spatial index optimization detected but row_id filtering not yet implemented"
                );
            }
        }

        // Delegate to inner provider
        self.inner.scan(state, projection, filters, limit).await
    }
}

/// Spatial filter bounding box
#[derive(Debug, Clone)]
struct SpatialFilter {
    min_x: f64,
    max_x: f64,
    min_y: f64,
    max_y: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spatial_filter_creation() {
        let filter = SpatialFilter {
            min_x: -74.1,
            max_x: -73.9,
            min_y: 40.6,
            max_y: 40.8,
        };

        assert_eq!(filter.min_x, -74.1);
        assert_eq!(filter.max_y, 40.8);
    }
}
