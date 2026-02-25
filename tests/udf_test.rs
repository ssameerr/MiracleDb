use miracledb::engine::MiracleEngine;
use datafusion::arrow::array::Float64Array;

#[tokio::test]
async fn test_geo_udf() {
    let engine = MiracleEngine::new().await.unwrap();
    
    // st_distance(0,0, 3,4) should be 5.0
    let sql = "SELECT st_distance(0.0, 0.0, 3.0, 4.0) as dist";
    let df = engine.query(sql).await.unwrap();
    let batches = df.collect().await.unwrap();
    
    assert_eq!(batches.len(), 1);
    let col = batches[0].column(0);
    let dist = col.as_any().downcast_ref::<Float64Array>().unwrap();
    
    assert_eq!(dist.value(0), 5.0);
}
