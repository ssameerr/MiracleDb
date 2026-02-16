//! Geospatial User-Defined Functions
//!
//! PostGIS-compatible geospatial functions for MiracleDb using the geo crate.
//! Supports points, lines, polygons, and various spatial operations.

use std::sync::Arc;
use datafusion::logical_expr::{ScalarUDF, Volatility, create_udf};
use datafusion::physical_plan::functions::make_scalar_function;
use datafusion::arrow::array::{Array, ArrayRef, Float64Array, BooleanArray, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::common::cast::as_float64_array;
use geo::{
    EuclideanDistance,
    EuclideanLength,
    Area,
    Centroid,
    Contains,
    Point,
    LineString,
    Polygon,
    Geometry,
};

/// Register all geospatial functions with DataFusion
pub fn register_geo_functions(ctx: &datafusion::execution::context::SessionContext) {
    // Distance functions
    ctx.register_udf(create_st_distance());
    ctx.register_udf(create_st_distance_wkt()); // WKT-based ST_Distance
    ctx.register_udf(create_st_dwithin());

    // Measurement functions
    ctx.register_udf(create_st_area());
    ctx.register_udf(create_st_length());

    // Spatial relationships
    ctx.register_udf(create_st_intersects());
    ctx.register_udf(create_st_contains());

    // Geometry operations
    ctx.register_udf(create_st_centroid_x());
    ctx.register_udf(create_st_centroid_y());

    tracing::info!("Registered {} geospatial functions", 9);
}

// ============================================================================
// WKT Parser Utility
// ============================================================================

/// Parse WKT string into geo::Geometry
fn parse_wkt(wkt_str: &str) -> Result<Geometry<f64>> {
    // Parse WKT string
    let wkt_geom: wkt::Geometry<f64> = wkt_str.parse()
        .map_err(|e: wkt::WktParseError| datafusion::error::DataFusionError::Execution(
            format!("WKT parse error: {}", e)
        ))?;

    // Convert wkt::Geometry to geo::Geometry
    let geometry: Geometry<f64> = wkt_geom.try_into()
        .map_err(|_| datafusion::error::DataFusionError::Execution(
            "Failed to convert WKT to geo geometry".to_string()
        ))?;

    Ok(geometry)
}

// ============================================================================
// Distance Functions
// ============================================================================

/// ST_Distance(x1, y1, x2, y2) -> DOUBLE
/// Calculate Euclidean distance between two points
fn create_st_distance() -> ScalarUDF {
    create_udf(
        "st_distance",
        vec![DataType::Float64, DataType::Float64, DataType::Float64, DataType::Float64],
        Arc::new(DataType::Float64),
        Volatility::Immutable,
        make_scalar_function(st_distance_impl),
    )
}

fn st_distance_impl(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 4 {
        return Err(datafusion::error::DataFusionError::Execution(
            "ST_Distance requires 4 arguments (x1, y1, x2, y2)".to_string()
        ));
    }

    let x1 = as_float64_array(&args[0])?;
    let y1 = as_float64_array(&args[1])?;
    let x2 = as_float64_array(&args[2])?;
    let y2 = as_float64_array(&args[3])?;

    let len = x1.len();
    let mut result = Vec::with_capacity(len);

    for i in 0..len {
        if !x1.is_valid(i) || !y1.is_valid(i) || !x2.is_valid(i) || !y2.is_valid(i) {
            result.push(None);
            continue;
        }

        let p1 = Point::new(x1.value(i), y1.value(i));
        let p2 = Point::new(x2.value(i), y2.value(i));

        let dist = p1.euclidean_distance(&p2);
        result.push(Some(dist));
    }

    Ok(Arc::new(Float64Array::from(result)))
}

/// ST_Distance(geometry1_wkt, geometry2_wkt) -> DOUBLE
/// Calculate Euclidean distance between two WKT geometries (PostGIS-compatible)
fn create_st_distance_wkt() -> ScalarUDF {
    create_udf(
        "ST_Distance",
        vec![DataType::Utf8, DataType::Utf8],
        Arc::new(DataType::Float64),
        Volatility::Immutable,
        make_scalar_function(st_distance_wkt_impl),
    )
}

fn st_distance_wkt_impl(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 2 {
        return Err(datafusion::error::DataFusionError::Execution(
            "ST_Distance (WKT) requires 2 arguments (geometry1_wkt, geometry2_wkt)".to_string()
        ));
    }

    let geom1_array = args[0].as_any().downcast_ref::<StringArray>()
        .ok_or_else(|| datafusion::error::DataFusionError::Execution(
            "First argument must be string (WKT)".to_string()
        ))?;

    let geom2_array = args[1].as_any().downcast_ref::<StringArray>()
        .ok_or_else(|| datafusion::error::DataFusionError::Execution(
            "Second argument must be string (WKT)".to_string()
        ))?;

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

/// ST_DWithin(x1, y1, x2, y2, distance) -> BOOLEAN
/// Check if two points are within a specified distance
fn create_st_dwithin() -> ScalarUDF {
    create_udf(
        "st_dwithin",
        vec![
            DataType::Float64,
            DataType::Float64,
            DataType::Float64,
            DataType::Float64,
            DataType::Float64,
        ],
        Arc::new(DataType::Boolean),
        Volatility::Immutable,
        make_scalar_function(st_dwithin_impl),
    )
}

fn st_dwithin_impl(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 5 {
        return Err(datafusion::error::DataFusionError::Execution(
            "ST_DWithin requires 5 arguments (x1, y1, x2, y2, distance)".to_string()
        ));
    }

    let x1 = as_float64_array(&args[0])?;
    let y1 = as_float64_array(&args[1])?;
    let x2 = as_float64_array(&args[2])?;
    let y2 = as_float64_array(&args[3])?;
    let dist_threshold = as_float64_array(&args[4])?;

    let len = x1.len();
    let mut result = Vec::with_capacity(len);

    for i in 0..len {
        if !x1.is_valid(i) || !y1.is_valid(i) || !x2.is_valid(i) || !y2.is_valid(i) || !dist_threshold.is_valid(i) {
            result.push(None);
            continue;
        }

        let p1 = Point::new(x1.value(i), y1.value(i));
        let p2 = Point::new(x2.value(i), y2.value(i));
        let threshold = dist_threshold.value(i);

        let dist = p1.euclidean_distance(&p2);
        result.push(Some(dist <= threshold));
    }

    Ok(Arc::new(BooleanArray::from(result)))
}

// ============================================================================
// Measurement Functions
// ============================================================================

/// ST_Area(x1, y1, x2, y2, x3, y3) -> DOUBLE
/// Calculate area of a triangle (simplified polygon)
fn create_st_area() -> ScalarUDF {
    create_udf(
        "st_area",
        vec![
            DataType::Float64,
            DataType::Float64,
            DataType::Float64,
            DataType::Float64,
            DataType::Float64,
            DataType::Float64,
        ],
        Arc::new(DataType::Float64),
        Volatility::Immutable,
        make_scalar_function(st_area_impl),
    )
}

fn st_area_impl(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 6 {
        return Err(datafusion::error::DataFusionError::Execution(
            "ST_Area requires 6 arguments (x1, y1, x2, y2, x3, y3) for triangle".to_string()
        ));
    }

    let x1 = as_float64_array(&args[0])?;
    let y1 = as_float64_array(&args[1])?;
    let x2 = as_float64_array(&args[2])?;
    let y2 = as_float64_array(&args[3])?;
    let x3 = as_float64_array(&args[4])?;
    let y3 = as_float64_array(&args[5])?;

    let len = x1.len();
    let mut result = Vec::with_capacity(len);

    for i in 0..len {
        if !x1.is_valid(i) || !y1.is_valid(i) || !x2.is_valid(i)
           || !y2.is_valid(i) || !x3.is_valid(i) || !y3.is_valid(i) {
            result.push(None);
            continue;
        }

        // Create a triangle polygon
        let coords = vec![
            (x1.value(i), y1.value(i)),
            (x2.value(i), y2.value(i)),
            (x3.value(i), y3.value(i)),
            (x1.value(i), y1.value(i)), // Close the polygon
        ];

        let polygon = Polygon::new(LineString::from(coords), vec![]);
        let area = polygon.unsigned_area();

        result.push(Some(area));
    }

    Ok(Arc::new(Float64Array::from(result)))
}

/// ST_Length(x1, y1, x2, y2) -> DOUBLE
/// Calculate length of a line segment
fn create_st_length() -> ScalarUDF {
    create_udf(
        "st_length",
        vec![
            DataType::Float64,
            DataType::Float64,
            DataType::Float64,
            DataType::Float64,
        ],
        Arc::new(DataType::Float64),
        Volatility::Immutable,
        make_scalar_function(st_length_impl),
    )
}

fn st_length_impl(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 4 {
        return Err(datafusion::error::DataFusionError::Execution(
            "ST_Length requires 4 arguments (x1, y1, x2, y2)".to_string()
        ));
    }

    let x1 = as_float64_array(&args[0])?;
    let y1 = as_float64_array(&args[1])?;
    let x2 = as_float64_array(&args[2])?;
    let y2 = as_float64_array(&args[3])?;

    let len = x1.len();
    let mut result = Vec::with_capacity(len);

    for i in 0..len {
        if !x1.is_valid(i) || !y1.is_valid(i) || !x2.is_valid(i) || !y2.is_valid(i) {
            result.push(None);
            continue;
        }

        let coords = vec![
            (x1.value(i), y1.value(i)),
            (x2.value(i), y2.value(i)),
        ];

        let line = LineString::from(coords);
        let length = line.euclidean_length();

        result.push(Some(length));
    }

    Ok(Arc::new(Float64Array::from(result)))
}

// ============================================================================
// Spatial Relationship Functions
// ============================================================================

/// ST_Intersects(x1, y1, r1, x2, y2, r2) -> BOOLEAN
/// Check if two circles intersect
fn create_st_intersects() -> ScalarUDF {
    create_udf(
        "st_intersects",
        vec![
            DataType::Float64,
            DataType::Float64,
            DataType::Float64,
            DataType::Float64,
            DataType::Float64,
            DataType::Float64,
        ],
        Arc::new(DataType::Boolean),
        Volatility::Immutable,
        make_scalar_function(st_intersects_impl),
    )
}

fn st_intersects_impl(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 6 {
        return Err(datafusion::error::DataFusionError::Execution(
            "ST_Intersects requires 6 arguments (x1, y1, r1, x2, y2, r2) for circles".to_string()
        ));
    }

    let x1 = as_float64_array(&args[0])?;
    let y1 = as_float64_array(&args[1])?;
    let r1 = as_float64_array(&args[2])?;
    let x2 = as_float64_array(&args[3])?;
    let y2 = as_float64_array(&args[4])?;
    let r2 = as_float64_array(&args[5])?;

    let len = x1.len();
    let mut result = Vec::with_capacity(len);

    for i in 0..len {
        if !x1.is_valid(i) || !y1.is_valid(i) || !r1.is_valid(i)
           || !x2.is_valid(i) || !y2.is_valid(i) || !r2.is_valid(i) {
            result.push(None);
            continue;
        }

        let p1 = Point::new(x1.value(i), y1.value(i));
        let p2 = Point::new(x2.value(i), y2.value(i));
        let radius1 = r1.value(i);
        let radius2 = r2.value(i);

        let center_dist = p1.euclidean_distance(&p2);
        let intersects = center_dist <= (radius1 + radius2);

        result.push(Some(intersects));
    }

    Ok(Arc::new(BooleanArray::from(result)))
}

/// ST_Contains(px, py, x1, y1, x2, y2, x3, y3) -> BOOLEAN
/// Check if a point is contained within a triangle
fn create_st_contains() -> ScalarUDF {
    create_udf(
        "st_contains",
        vec![
            DataType::Float64,  // point x
            DataType::Float64,  // point y
            DataType::Float64,  // triangle x1
            DataType::Float64,  // triangle y1
            DataType::Float64,  // triangle x2
            DataType::Float64,  // triangle y2
            DataType::Float64,  // triangle x3
            DataType::Float64,  // triangle y3
        ],
        Arc::new(DataType::Boolean),
        Volatility::Immutable,
        make_scalar_function(st_contains_impl),
    )
}

fn st_contains_impl(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 8 {
        return Err(datafusion::error::DataFusionError::Execution(
            "ST_Contains requires 8 arguments (px, py, x1, y1, x2, y2, x3, y3)".to_string()
        ));
    }

    let px = as_float64_array(&args[0])?;
    let py = as_float64_array(&args[1])?;
    let x1 = as_float64_array(&args[2])?;
    let y1 = as_float64_array(&args[3])?;
    let x2 = as_float64_array(&args[4])?;
    let y2 = as_float64_array(&args[5])?;
    let x3 = as_float64_array(&args[6])?;
    let y3 = as_float64_array(&args[7])?;

    let len = px.len();
    let mut result = Vec::with_capacity(len);

    for i in 0..len {
        if !px.is_valid(i) || !py.is_valid(i) || !x1.is_valid(i) || !y1.is_valid(i)
           || !x2.is_valid(i) || !y2.is_valid(i) || !x3.is_valid(i) || !y3.is_valid(i) {
            result.push(None);
            continue;
        }

        let point = Point::new(px.value(i), py.value(i));

        let coords = vec![
            (x1.value(i), y1.value(i)),
            (x2.value(i), y2.value(i)),
            (x3.value(i), y3.value(i)),
            (x1.value(i), y1.value(i)),
        ];

        let polygon = Polygon::new(LineString::from(coords), vec![]);
        let contains = polygon.contains(&point);

        result.push(Some(contains));
    }

    Ok(Arc::new(BooleanArray::from(result)))
}

// ============================================================================
// Geometry Operation Functions
// ============================================================================

/// ST_Centroid_X(x1, y1, x2, y2, x3, y3) -> DOUBLE
/// Get X coordinate of centroid of a triangle
fn create_st_centroid_x() -> ScalarUDF {
    create_udf(
        "st_centroid_x",
        vec![
            DataType::Float64,
            DataType::Float64,
            DataType::Float64,
            DataType::Float64,
            DataType::Float64,
            DataType::Float64,
        ],
        Arc::new(DataType::Float64),
        Volatility::Immutable,
        make_scalar_function(st_centroid_x_impl),
    )
}

fn st_centroid_x_impl(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 6 {
        return Err(datafusion::error::DataFusionError::Execution(
            "ST_Centroid_X requires 6 arguments (x1, y1, x2, y2, x3, y3)".to_string()
        ));
    }

    let x1 = as_float64_array(&args[0])?;
    let y1 = as_float64_array(&args[1])?;
    let x2 = as_float64_array(&args[2])?;
    let y2 = as_float64_array(&args[3])?;
    let x3 = as_float64_array(&args[4])?;
    let y3 = as_float64_array(&args[5])?;

    let len = x1.len();
    let mut result = Vec::with_capacity(len);

    for i in 0..len {
        if !x1.is_valid(i) || !y1.is_valid(i) || !x2.is_valid(i)
           || !y2.is_valid(i) || !x3.is_valid(i) || !y3.is_valid(i) {
            result.push(None);
            continue;
        }

        let coords = vec![
            (x1.value(i), y1.value(i)),
            (x2.value(i), y2.value(i)),
            (x3.value(i), y3.value(i)),
            (x1.value(i), y1.value(i)),
        ];

        let polygon = Polygon::new(LineString::from(coords), vec![]);

        if let Some(centroid) = polygon.centroid() {
            result.push(Some(centroid.x()));
        } else {
            result.push(None);
        }
    }

    Ok(Arc::new(Float64Array::from(result)))
}

/// ST_Centroid_Y(x1, y1, x2, y2, x3, y3) -> DOUBLE
/// Get Y coordinate of centroid of a triangle
fn create_st_centroid_y() -> ScalarUDF {
    create_udf(
        "st_centroid_y",
        vec![
            DataType::Float64,
            DataType::Float64,
            DataType::Float64,
            DataType::Float64,
            DataType::Float64,
            DataType::Float64,
        ],
        Arc::new(DataType::Float64),
        Volatility::Immutable,
        make_scalar_function(st_centroid_y_impl),
    )
}

fn st_centroid_y_impl(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 6 {
        return Err(datafusion::error::DataFusionError::Execution(
            "ST_Centroid_Y requires 6 arguments (x1, y1, x2, y2, x3, y3)".to_string()
        ));
    }

    let x1 = as_float64_array(&args[0])?;
    let y1 = as_float64_array(&args[1])?;
    let x2 = as_float64_array(&args[2])?;
    let y2 = as_float64_array(&args[3])?;
    let x3 = as_float64_array(&args[4])?;
    let y3 = as_float64_array(&args[5])?;

    let len = x1.len();
    let mut result = Vec::with_capacity(len);

    for i in 0..len {
        if !x1.is_valid(i) || !y1.is_valid(i) || !x2.is_valid(i)
           || !y2.is_valid(i) || !x3.is_valid(i) || !y3.is_valid(i) {
            result.push(None);
            continue;
        }

        let coords = vec![
            (x1.value(i), y1.value(i)),
            (x2.value(i), y2.value(i)),
            (x3.value(i), y3.value(i)),
            (x1.value(i), y1.value(i)),
        ];

        let polygon = Polygon::new(LineString::from(coords), vec![]);

        if let Some(centroid) = polygon.centroid() {
            result.push(Some(centroid.y()));
        } else {
            result.push(None);
        }
    }

    Ok(Arc::new(Float64Array::from(result)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_euclidean_distance() {
        let p1 = Point::new(0.0, 0.0);
        let p2 = Point::new(3.0, 4.0);
        let dist = p1.euclidean_distance(&p2);
        assert_eq!(dist, 5.0);
    }

    #[test]
    fn test_polygon_area() {
        let coords = vec![
            (0.0, 0.0),
            (4.0, 0.0),
            (4.0, 3.0),
            (0.0, 0.0),
        ];
        let polygon = Polygon::new(LineString::from(coords), vec![]);
        let area = polygon.unsigned_area();
        assert_eq!(area, 6.0);
    }
}
