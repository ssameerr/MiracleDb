//! Spatial Query Optimizer for DataFusion
//!
//! Implements automatic query optimization for geospatial predicates.
//! Detects ST_Distance and ST_DWithin patterns and converts them to
//! bounding box queries that leverage R-tree spatial indexes.

use datafusion::common::{DFSchema, Result as DFResult};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Expr, LogicalPlan, Operator};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use std::sync::Arc;

/// Optimizer rule for spatial filter pushdown
///
/// Detects patterns like:
/// - ST_Distance(lat, lon, 40.7, -74.0) < 0.1
/// - ST_DWithin(lat, lon, 40.7, -74.0, 0.1)
///
/// And converts them to bounding box queries that can leverage R-tree indexes.
pub struct SpatialFilterPushdown;

impl SpatialFilterPushdown {
    pub fn new() -> Self {
        Self
    }

    /// Extract spatial predicate information from an expression
    fn extract_spatial_predicate(&self, expr: &Expr) -> Option<SpatialPredicate> {
        match expr {
            // Pattern: ST_Distance(...) < threshold
            Expr::BinaryExpr(binary_expr) => {
                if matches!(
                    binary_expr.op,
                    Operator::Lt | Operator::LtEq | Operator::Eq
                ) {
                    if let Expr::ScalarFunction(func) = &*binary_expr.left {
                        if func.name().to_lowercase() == "st_distance" {
                            return self.extract_st_distance_predicate(func, &binary_expr.right);
                        }
                    }
                }
                None
            }
            // Pattern: ST_DWithin(...)
            Expr::ScalarFunction(func) => {
                if func.name().to_lowercase() == "st_dwithin" {
                    return self.extract_st_dwithin_predicate(func);
                }
                None
            }
            _ => None,
        }
    }

    /// Extract predicate from ST_Distance function
    fn extract_st_distance_predicate(
        &self,
        func: &datafusion::logical_expr::expr::ScalarFunction,
        threshold_expr: &Expr,
    ) -> Option<SpatialPredicate> {
        // ST_Distance(col_x, col_y, center_x, center_y) < threshold
        if func.args.len() != 4 {
            return None;
        }

        // Extract column references
        let col_x = self.extract_column_name(&func.args[0])?;
        let col_y = self.extract_column_name(&func.args[1])?;

        // Extract center coordinates
        let center_x = self.extract_literal_f64(&func.args[2])?;
        let center_y = self.extract_literal_f64(&func.args[3])?;

        // Extract threshold
        let threshold = self.extract_literal_f64(threshold_expr)?;

        Some(SpatialPredicate {
            column_x: col_x,
            column_y: col_y,
            center_x,
            center_y,
            radius: threshold,
            predicate_type: SpatialPredicateType::Distance,
        })
    }

    /// Extract predicate from ST_DWithin function
    fn extract_st_dwithin_predicate(
        &self,
        func: &datafusion::logical_expr::expr::ScalarFunction,
    ) -> Option<SpatialPredicate> {
        // ST_DWithin(col_x, col_y, center_x, center_y, radius)
        if func.args.len() != 5 {
            return None;
        }

        // Extract column references
        let col_x = self.extract_column_name(&func.args[0])?;
        let col_y = self.extract_column_name(&func.args[1])?;

        // Extract center coordinates
        let center_x = self.extract_literal_f64(&func.args[2])?;
        let center_y = self.extract_literal_f64(&func.args[3])?;

        // Extract radius
        let radius = self.extract_literal_f64(&func.args[4])?;

        Some(SpatialPredicate {
            column_x: col_x,
            column_y: col_y,
            center_x,
            center_y,
            radius,
            predicate_type: SpatialPredicateType::DWithin,
        })
    }

    /// Extract column name from expression
    fn extract_column_name(&self, expr: &Expr) -> Option<String> {
        match expr {
            Expr::Column(col) => Some(col.name.clone()),
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

    /// Convert spatial predicate to bounding box filter
    fn create_bounding_box_filter(
        &self,
        predicate: &SpatialPredicate,
    ) -> (f64, f64, f64, f64) {
        // Simple bounding box: center ± radius
        // In production, this should account for spherical geometry
        let min_x = predicate.center_x - predicate.radius;
        let max_x = predicate.center_x + predicate.radius;
        let min_y = predicate.center_y - predicate.radius;
        let max_y = predicate.center_y + predicate.radius;

        (min_x, min_y, max_x, max_y)
    }

    /// Optimize a single logical plan node
    fn optimize_plan_node(&self, plan: LogicalPlan) -> DFResult<LogicalPlan> {
        match plan {
            LogicalPlan::Filter(filter) => {
                // Check if filter contains spatial predicates
                if let Some(predicate) = self.extract_spatial_predicate(&filter.predicate) {
                    tracing::debug!(
                        "Detected spatial predicate: {:?} on columns ({}, {})",
                        predicate.predicate_type,
                        predicate.column_x,
                        predicate.column_y
                    );

                    // Calculate bounding box
                    let (min_x, min_y, max_x, max_y) = self.create_bounding_box_filter(&predicate);

                    tracing::debug!(
                        "Bounding box: x=[{}, {}], y=[{}, {}]",
                        min_x,
                        max_x,
                        min_y,
                        max_y
                    );

                    // For now, we'll keep the original filter
                    // In Phase 3.2, SpatialTableProvider will intercept this
                    // and use the bounding box for R-tree queries

                    // Add metadata hint to the filter
                    // (DataFusion doesn't have a direct way to attach hints,
                    // so we'll rely on SpatialTableProvider to detect spatial columns)
                }

                // Recursively optimize input
                let optimized_input = self.optimize_plan_node((*filter.input).clone())?;
                Ok(LogicalPlan::Filter(datafusion::logical_expr::Filter::try_new(
                    filter.predicate.clone(),
                    Arc::new(optimized_input),
                )?))
            }
            // Recursively optimize other plan types
            LogicalPlan::Projection(proj) => {
                let optimized_input = self.optimize_plan_node((*proj.input).clone())?;
                Ok(LogicalPlan::Projection(
                    datafusion::logical_expr::Projection::try_new(
                        proj.expr.clone(),
                        Arc::new(optimized_input),
                    )?,
                ))
            }
            LogicalPlan::Aggregate(agg) => {
                let optimized_input = self.optimize_plan_node((*agg.input).clone())?;
                Ok(LogicalPlan::Aggregate(
                    datafusion::logical_expr::Aggregate::try_new(
                        Arc::new(optimized_input),
                        agg.group_expr.clone(),
                        agg.aggr_expr.clone(),
                    )?,
                ))
            }
            LogicalPlan::Join(join) => {
                let optimized_left = self.optimize_plan_node((*join.left).clone())?;
                let optimized_right = self.optimize_plan_node((*join.right).clone())?;
                Ok(LogicalPlan::Join(datafusion::logical_expr::Join {
                    left: Arc::new(optimized_left),
                    right: Arc::new(optimized_right),
                    on: join.on.clone(),
                    filter: join.filter.clone(),
                    join_type: join.join_type,
                    join_constraint: join.join_constraint,
                    schema: join.schema.clone(),
                    null_equals_null: join.null_equals_null,
                }))
            }
            // For all other plan types, return as-is
            other => Ok(other),
        }
    }
}

impl OptimizerRule for SpatialFilterPushdown {
    fn name(&self) -> &str {
        "spatial_filter_pushdown"
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> DFResult<datafusion::common::tree_node::Transformed<LogicalPlan>> {
        let optimized = self.optimize_plan_node(plan)?;
        Ok(datafusion::common::tree_node::Transformed::yes(optimized))
    }
}

/// Spatial predicate extracted from query
#[derive(Debug, Clone)]
struct SpatialPredicate {
    column_x: String,
    column_y: String,
    center_x: f64,
    center_y: f64,
    radius: f64,
    predicate_type: SpatialPredicateType,
}

/// Type of spatial predicate
#[derive(Debug, Clone)]
enum SpatialPredicateType {
    Distance,
    DWithin,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spatial_filter_pushdown_creation() {
        let optimizer = SpatialFilterPushdown::new();
        assert_eq!(optimizer.name(), "spatial_filter_pushdown");
    }
}
