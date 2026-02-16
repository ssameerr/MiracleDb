//! Geospatial User-Defined Functions
//!
//! PostGIS-compatible geospatial functions for MiracleDb using the geo crate.

use std::sync::Arc;
use datafusion::logical_expr::{ScalarUDF, Volatility, create_udf};
use datafusion::physical_plan::functions::make_scalar_function;
use datafusion::arrow::array::{ArrayRef, Float64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result;
use geo::{
    EuclideanDistance,
    Geometry,
};

/// Register all geospatial functions with DataFusion
pub fn register_geo_functions(ctx: &datafusion::execution::context::SessionContext) {
    // Task 1: ST_Distance (WKT-based)
    ctx.register_udf(create_st_distance_udf());

    tracing::info!("Registered 1 geospatial function");
}

// ============================================================================
// WKT Parser Utility
// ============================================================================

/// Parse WKT string into geo::Geometry
fn parse_wkt(wkt_str: &str) -> Result<Geometry<f64>> {
    // Parse WKT string using the wkt crate
    use wkt::TryFromWkt;

    let geometry: Geometry<f64> = Geometry::try_from_wkt_str(wkt_str)
        .map_err(|e| datafusion::error::DataFusionError::Execution(
            format!("WKT parse error: {:?}", e)
        ))?;

    Ok(geometry)
}

// ============================================================================
// Task 1: ST_Distance Function
// ============================================================================

/// ST_Distance(geometry1_wkt, geometry2_wkt) -> DOUBLE
/// Calculate Euclidean distance between two WKT geometries (PostGIS-compatible)
fn create_st_distance_udf() -> ScalarUDF {
    create_udf(
        "ST_Distance",
        vec![DataType::Utf8, DataType::Utf8],
        Arc::new(DataType::Float64),
        Volatility::Immutable,
        make_scalar_function(st_distance_udf_impl),
    )
}

fn st_distance_udf_impl(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 2 {
        return Err(datafusion::error::DataFusionError::Execution(
            "ST_Distance requires 2 arguments (geometry1_wkt, geometry2_wkt)".to_string()
        ));
    }

    let geom1_array = datafusion::common::cast::as_string_array(&args[0])?;
    let geom2_array = datafusion::common::cast::as_string_array(&args[1])?;

    let len = geom1_array.len();
    let mut result = Vec::with_capacity(len);

    for i in 0..len {
        // Handle NULL values
        if geom1_array.is_null(i) || geom2_array.is_null(i) {
            result.push(None);
            continue;
        }

        let wkt1 = geom1_array.value(i);
        let wkt2 = geom2_array.value(i);

        // Parse WKT strings to geometries
        let geom1 = match parse_wkt(wkt1) {
            Ok(g) => g,
            Err(e) => {
                return Err(datafusion::error::DataFusionError::Execution(
                    format!("Failed to parse geometry 1: {}", e)
                ));
            }
        };

        let geom2 = match parse_wkt(wkt2) {
            Ok(g) => g,
            Err(e) => {
                return Err(datafusion::error::DataFusionError::Execution(
                    format!("Failed to parse geometry 2: {}", e)
                ));
            }
        };

        // Calculate Euclidean distance
        let distance = geom1.euclidean_distance(&geom2);
        result.push(Some(distance));
    }

    Ok(Arc::new(Float64Array::from(result)))
}

