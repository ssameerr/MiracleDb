use miracledb::engine::MiracleEngine;
use std::sync::Arc;

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
