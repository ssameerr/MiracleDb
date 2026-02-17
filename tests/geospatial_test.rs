use miracledb::engine::MiracleEngine;
use std::sync::Arc;
use arrow::array::BooleanArray;

#[tokio::test]
async fn test_st_distance_points() {
    let engine = Arc::new(MiracleEngine::new().await.expect("Failed to create engine"));

    let sql = "SELECT ST_Distance('POINT(0 0)', 'POINT(3 4)') as distance";
    let result = engine.query(sql).await.expect("Query failed");

    // Should return 5.0 (3-4-5 triangle)
    let batches = result.collect().await.expect("Failed to collect results");
    assert_eq!(batches.len(), 1);

    let batch = &batches[0];
    let distance_col = batch.column(0)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .expect("Expected Float64Array");

    let distance = distance_col.value(0);
    assert!((distance - 5.0).abs() < 0.001, "Expected distance ~5.0, got {}", distance);
}

#[tokio::test]
async fn test_st_contains_polygon_point() {
    let engine = Arc::new(MiracleEngine::new().await.expect("Failed to create engine"));

    // Polygon: square from (0,0) to (10,10)
    let polygon = "'POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))'";

    // Test point inside polygon (5,5) - should be true
    let sql = format!("SELECT ST_Contains({}, 'POINT(5 5)') as inside", polygon);
    let result = engine.query(&sql).await.expect("Query failed");
    let batches = result.collect().await.expect("Failed to collect");
    let inside_col = batches[0].column(0).as_any().downcast_ref::<BooleanArray>().unwrap();
    assert_eq!(inside_col.value(0), true, "Point (5,5) should be inside polygon");

    // Test point outside polygon (15,15) - should be false
    let sql2 = format!("SELECT ST_Contains({}, 'POINT(15 15)') as outside", polygon);
    let result2 = engine.query(&sql2).await.expect("Query failed");
    let batches2 = result2.collect().await.expect("Failed to collect");
    let outside_col = batches2[0].column(0).as_any().downcast_ref::<BooleanArray>().unwrap();
    assert_eq!(outside_col.value(0), false, "Point (15,15) should be outside polygon");
}

#[tokio::test]
async fn test_st_intersects() {
    let engine = Arc::new(MiracleEngine::new().await.expect("Failed to create engine"));

    // Two overlapping polygons: both overlap in the region (5,5)-(10,10)
    let poly1 = "'POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))'";
    let poly2 = "'POLYGON((5 5, 15 5, 15 15, 5 15, 5 5))'";

    // Overlapping polygons should intersect -> true
    let sql = format!("SELECT ST_Intersects({}, {}) as result", poly1, poly2);
    let result = engine.query(&sql).await.expect("Query failed");
    let batches = result.collect().await.expect("Failed to collect");
    let col = batches[0].column(0).as_any().downcast_ref::<BooleanArray>().unwrap();
    assert_eq!(col.value(0), true, "Overlapping polygons should intersect");

    // Non-overlapping polygons should not intersect -> false
    let poly3 = "'POLYGON((20 20, 30 20, 30 30, 20 30, 20 20))'";
    let sql2 = format!("SELECT ST_Intersects({}, {}) as result", poly1, poly3);
    let result2 = engine.query(&sql2).await.expect("Query failed");
    let batches2 = result2.collect().await.expect("Failed to collect");
    let col2 = batches2[0].column(0).as_any().downcast_ref::<BooleanArray>().unwrap();
    assert_eq!(col2.value(0), false, "Non-overlapping polygons should not intersect");
}

#[tokio::test]
async fn test_st_area() {
    let engine = Arc::new(MiracleEngine::new().await.expect("Failed to create engine"));

    // Unit square from (0,0) to (1,1) should have area 1.0
    let sql = "SELECT ST_Area('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))') as area";
    let result = engine.query(sql).await.expect("Query failed");
    let batches = result.collect().await.expect("Failed to collect results");
    assert_eq!(batches.len(), 1);

    let batch = &batches[0];
    let area_col = batch.column(0)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .expect("Expected Float64Array");

    let area = area_col.value(0);
    assert!((area - 1.0).abs() < 0.001, "Expected area ~1.0, got {}", area);
}
