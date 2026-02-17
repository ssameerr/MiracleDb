//! Geospatial User-Defined Functions
//!
//! PostGIS-compatible geospatial functions for MiracleDb using the geo crate.

use std::sync::Arc;
use datafusion::logical_expr::{ScalarUDF, Volatility, create_udf};
// TODO: Migrate from deprecated make_scalar_function to ScalarUDFImpl trait (DataFusion 41.0.0+)
use datafusion::physical_plan::functions::make_scalar_function;
use datafusion::arrow::array::{ArrayRef, Float64Array, BooleanArray, Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result;
use geo::{
    Distance,         // Replaces deprecated EuclideanDistance trait
    Euclidean,        // Distance metric for Euclidean distance calculation
    Contains,         // Trait for containment checks
    Intersects,       // Trait for intersection checks
    Within,           // Trait for within checks
    Area,             // Trait for area calculations
    EuclideanLength,  // Trait for Euclidean length calculations
    Geometry,
};

/// Register all geospatial functions with DataFusion
pub fn register_geo_functions(ctx: &datafusion::execution::context::SessionContext) {
    // Task 1: ST_Distance (WKT-based)
    ctx.register_udf(create_st_distance_udf());
    // Task 2: ST_Contains (WKT-based)
    ctx.register_udf(create_st_contains_udf());
    // Task 3: ST_Intersects (WKT-based)
    ctx.register_udf(create_st_intersects_udf());
    // Task 3: ST_Within (WKT-based)
    ctx.register_udf(create_st_within_udf());
    // Task 3: ST_Area (WKT-based)
    ctx.register_udf(create_st_area_udf());
    // Task 3: ST_Length (WKT-based)
    ctx.register_udf(create_st_length_udf());

    tracing::info!("Registered 6 geospatial functions: ST_Distance, ST_Contains, ST_Intersects, ST_Within, ST_Area, ST_Length");
}

// ============================================================================
// WKT Parser Utility
// ============================================================================

/// Parse WKT string into geo::Geometry
fn parse_wkt(wkt_str: &str) -> Result<Geometry<f64>> {
    // Optional: Input validation to prevent DoS attacks
    const MAX_WKT_LENGTH: usize = 1_000_000; // 1MB limit

    if wkt_str.len() > MAX_WKT_LENGTH {
        return Err(datafusion::error::DataFusionError::Execution(
            format!("WKT string exceeds maximum length of {} bytes", MAX_WKT_LENGTH)
        ));
    }

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

        // Calculate Euclidean distance using new Distance trait API
        let distance = Euclidean.distance(&geom1, &geom2);
        result.push(Some(distance));
    }

    Ok(Arc::new(Float64Array::from(result)))
}

// ============================================================================
// Task 2: ST_Contains Function
// ============================================================================

/// ST_Contains(geometry1_wkt, geometry2_wkt) -> BOOLEAN
/// Check if geometry1 completely contains geometry2 (PostGIS-compatible)
fn create_st_contains_udf() -> ScalarUDF {
    // TODO: Migrate from deprecated make_scalar_function to ScalarUDFImpl trait (DataFusion 41.0.0+)
    create_udf(
        "ST_Contains",
        vec![DataType::Utf8, DataType::Utf8],
        Arc::new(DataType::Boolean),
        Volatility::Immutable,
        make_scalar_function(st_contains_udf_impl),
    )
}

fn st_contains_udf_impl(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 2 {
        return Err(datafusion::error::DataFusionError::Execution(
            "ST_Contains requires exactly 2 arguments".to_string()
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

        // Check containment using geo::Contains trait
        let contains = geom1.contains(&geom2);
        result.push(Some(contains));
    }

    Ok(Arc::new(BooleanArray::from(result)))
}

// ============================================================================
// Task 3: ST_Intersects Function
// ============================================================================

/// ST_Intersects(geometry1_wkt, geometry2_wkt) -> BOOLEAN
/// Check if two WKT geometries intersect (PostGIS-compatible)
fn create_st_intersects_udf() -> ScalarUDF {
    create_udf(
        "ST_Intersects",
        vec![DataType::Utf8, DataType::Utf8],
        Arc::new(DataType::Boolean),
        Volatility::Immutable,
        make_scalar_function(st_intersects_udf_impl),
    )
}

fn st_intersects_udf_impl(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 2 {
        return Err(datafusion::error::DataFusionError::Execution(
            "ST_Intersects requires exactly 2 arguments".to_string()
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

        // Check intersection using geo::Intersects trait
        let intersects = geom1.intersects(&geom2);
        result.push(Some(intersects));
    }

    Ok(Arc::new(BooleanArray::from(result)))
}

// ============================================================================
// Task 3: ST_Within Function
// ============================================================================

/// ST_Within(geometry1_wkt, geometry2_wkt) -> BOOLEAN
/// Check if geometry1 is completely within geometry2 (PostGIS-compatible)
fn create_st_within_udf() -> ScalarUDF {
    create_udf(
        "ST_Within",
        vec![DataType::Utf8, DataType::Utf8],
        Arc::new(DataType::Boolean),
        Volatility::Immutable,
        make_scalar_function(st_within_udf_impl),
    )
}

fn st_within_udf_impl(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 2 {
        return Err(datafusion::error::DataFusionError::Execution(
            "ST_Within requires exactly 2 arguments".to_string()
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

        // Check within using geo::Within trait
        let within = geom1.is_within(&geom2);
        result.push(Some(within));
    }

    Ok(Arc::new(BooleanArray::from(result)))
}

// ============================================================================
// Task 3: ST_Area Function
// ============================================================================

/// ST_Area(geometry_wkt) -> DOUBLE
/// Calculate the area of a WKT geometry (PostGIS-compatible)
fn create_st_area_udf() -> ScalarUDF {
    create_udf(
        "ST_Area",
        vec![DataType::Utf8],
        Arc::new(DataType::Float64),
        Volatility::Immutable,
        make_scalar_function(st_area_udf_impl),
    )
}

fn st_area_udf_impl(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 {
        return Err(datafusion::error::DataFusionError::Execution(
            "ST_Area requires exactly 1 argument".to_string()
        ));
    }

    let geom_array = datafusion::common::cast::as_string_array(&args[0])?;

    let len = geom_array.len();
    let mut result = Vec::with_capacity(len);

    for i in 0..len {
        // Handle NULL values
        if geom_array.is_null(i) {
            result.push(None);
            continue;
        }

        let wkt = geom_array.value(i);

        // Parse WKT string to geometry
        let geom = match parse_wkt(wkt) {
            Ok(g) => g,
            Err(e) => {
                return Err(datafusion::error::DataFusionError::Execution(
                    format!("Failed to parse geometry: {}", e)
                ));
            }
        };

        // Calculate area using geo::Area trait
        let area = match geom {
            Geometry::Polygon(p) => p.unsigned_area(),
            _ => 0.0,
        };
        result.push(Some(area));
    }

    Ok(Arc::new(Float64Array::from(result)))
}

// ============================================================================
// Task 3: ST_Length Function
// ============================================================================

/// ST_Length(geometry_wkt) -> DOUBLE
/// Calculate the Euclidean length of a WKT geometry (PostGIS-compatible)
fn create_st_length_udf() -> ScalarUDF {
    create_udf(
        "ST_Length",
        vec![DataType::Utf8],
        Arc::new(DataType::Float64),
        Volatility::Immutable,
        make_scalar_function(st_length_udf_impl),
    )
}

fn st_length_udf_impl(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 {
        return Err(datafusion::error::DataFusionError::Execution(
            "ST_Length requires exactly 1 argument".to_string()
        ));
    }

    let geom_array = datafusion::common::cast::as_string_array(&args[0])?;

    let len = geom_array.len();
    let mut result = Vec::with_capacity(len);

    for i in 0..len {
        // Handle NULL values
        if geom_array.is_null(i) {
            result.push(None);
            continue;
        }

        let wkt = geom_array.value(i);

        // Parse WKT string to geometry
        let geom = match parse_wkt(wkt) {
            Ok(g) => g,
            Err(e) => {
                return Err(datafusion::error::DataFusionError::Execution(
                    format!("Failed to parse geometry: {}", e)
                ));
            }
        };

        // Calculate Euclidean length using geo::EuclideanLength trait
        let length = match geom {
            Geometry::LineString(ls) => ls.euclidean_length(),
            _ => 0.0,
        };
        result.push(Some(length));
    }

    Ok(Arc::new(Float64Array::from(result)))
}
