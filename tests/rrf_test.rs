#[cfg(test)]
mod rrf_tests {
    use miracledb::engine::MiracleEngine;
    use std::sync::Arc;

    /// Test basic RRF function with two rank lists
    #[tokio::test]
    async fn test_rrf_basic() {
        let engine = MiracleEngine::new().await.expect("Failed to create engine");

        // Create test table with ranks
        engine.query("CREATE TABLE test_ranks (id INT, vector_rank INT, text_rank INT)").await.unwrap();
        engine.query("INSERT INTO test_ranks VALUES (1, 1, 2), (2, 2, 1), (3, 3, 3)").await.unwrap();

        // Test RRF
        let sql = "SELECT id, rrf(vector_rank, text_rank) as rrf_score FROM test_ranks ORDER BY rrf_score DESC";
        let result = engine.query(sql).await;

        assert!(result.is_ok(), "RRF query should succeed");

        let df = result.unwrap();
        let batches = df.collect().await.unwrap();

        assert!(!batches.is_empty(), "Should have results");
        assert_eq!(batches[0].num_rows(), 3, "Should have 3 rows");

        // Verify RRF scores are calculated
        let score_col = batches[0].column(1);
        let scores = score_col.as_any().downcast_ref::<datafusion::arrow::array::Float64Array>().unwrap();

        // All scores should be positive
        for i in 0..3 {
            assert!(scores.value(i) > 0.0, "RRF score should be positive");
        }

        // Scores should be ordered (highest first due to ORDER BY DESC)
        assert!(scores.value(0) >= scores.value(1));
        assert!(scores.value(1) >= scores.value(2));
    }

    /// Test RRF with ROW_NUMBER() for ranking
    #[tokio::test]
    async fn test_rrf_with_row_number() {
        let engine = MiracleEngine::new().await.expect("Failed to create engine");

        // Create test table with similarity scores
        engine.query("CREATE TABLE documents (id INT, title TEXT, vector_score DOUBLE, text_score DOUBLE)").await.unwrap();
        engine.query("
            INSERT INTO documents VALUES
            (1, 'Machine Learning', 0.95, 0.70),
            (2, 'Deep Learning', 0.85, 0.90),
            (3, 'Neural Networks', 0.75, 0.60),
            (4, 'AI Research', 0.65, 0.80)
        ").await.unwrap();

        // Use ROW_NUMBER() to create ranks, then apply RRF
        let sql = "
            SELECT
                id,
                title,
                rrf(vector_rank, text_rank) as hybrid_score
            FROM (
                SELECT
                    id,
                    title,
                    ROW_NUMBER() OVER (ORDER BY vector_score DESC) as vector_rank,
                    ROW_NUMBER() OVER (ORDER BY text_score DESC) as text_rank
                FROM documents
            )
            ORDER BY hybrid_score DESC
        ";

        let result = engine.query(sql).await;
        assert!(result.is_ok(), "RRF with ROW_NUMBER should work");

        let df = result.unwrap();
        let batches = df.collect().await.unwrap();

        assert!(!batches.is_empty());
        assert_eq!(batches[0].num_rows(), 4);

        // Verify results are ordered by hybrid score
        let scores = batches[0].column(2).as_any().downcast_ref::<datafusion::arrow::array::Float64Array>().unwrap();

        for i in 0..3 {
            assert!(scores.value(i) >= scores.value(i + 1), "Scores should be descending");
        }
    }

    /// Test RRF with custom k parameter
    #[tokio::test]
    async fn test_rrf_k_custom() {
        let engine = MiracleEngine::new().await.expect("Failed to create engine");

        engine.query("CREATE TABLE test_ranks (id INT, rank1 INT, rank2 INT)").await.unwrap();
        engine.query("INSERT INTO test_ranks VALUES (1, 1, 1), (2, 2, 2)").await.unwrap();

        // Test with k=30 (more aggressive ranking than default k=60)
        let sql = "SELECT id, rrf_k(rank1, rank2, 30.0) as rrf_score FROM test_ranks ORDER BY rrf_score DESC";
        let result = engine.query(sql).await;

        assert!(result.is_ok(), "RRF_K should work");

        let df = result.unwrap();
        let batches = df.collect().await.unwrap();

        assert_eq!(batches[0].num_rows(), 2);

        let scores = batches[0].column(1).as_any().downcast_ref::<datafusion::arrow::array::Float64Array>().unwrap();

        // With k=30, rank 1 should have higher score than with k=60
        // Score for rank 1: 2 * (1 / (30 + 1)) = 2 * 0.03226 = 0.06452
        assert!((scores.value(0) - 0.06452).abs() < 0.001);

        // Score for rank 2: 2 * (1 / (30 + 2)) = 2 * 0.03125 = 0.0625
        assert!((scores.value(1) - 0.0625).abs() < 0.001);
    }

    /// Test hybrid_search() UDF
    #[tokio::test]
    async fn test_hybrid_search_udf() {
        let engine = MiracleEngine::new().await.expect("Failed to create engine");

        engine.query("CREATE TABLE search_results (id INT, title TEXT, vector_score DOUBLE, text_score DOUBLE)").await.unwrap();
        engine.query("
            INSERT INTO search_results VALUES
            (1, 'Exact Match', 0.95, 0.90),
            (2, 'Good Vector', 0.90, 0.60),
            (3, 'Good Text', 0.50, 0.85),
            (4, 'Average', 0.70, 0.70)
        ").await.unwrap();

        // Test hybrid_search with equal weights (default 0.5, 0.5)
        let sql = "
            SELECT
                id,
                title,
                hybrid_search(vector_score, text_score) as relevance
            FROM search_results
            ORDER BY relevance DESC
        ";

        let result = engine.query(sql).await;
        assert!(result.is_ok(), "hybrid_search should work");

        let df = result.unwrap();
        let batches = df.collect().await.unwrap();

        assert_eq!(batches[0].num_rows(), 4);

        // First result should be "Exact Match" (highest combined score)
        let id_col = batches[0].column(0).as_any().downcast_ref::<datafusion::arrow::array::Int64Array>().unwrap();
        assert_eq!(id_col.value(0), 1, "Exact Match should rank first");

        // Verify relevance scores
        let rel_col = batches[0].column(2).as_any().downcast_ref::<datafusion::arrow::array::Float64Array>().unwrap();

        // Exact Match: 0.5*0.95 + 0.5*0.90 = 0.925
        assert!((rel_col.value(0) - 0.925).abs() < 0.01);
    }

    /// Test RRF with NULL ranks (documents not in all result sets)
    #[tokio::test]
    async fn test_rrf_with_nulls() {
        let engine = MiracleEngine::new().await.expect("Failed to create engine");

        engine.query("CREATE TABLE partial_results (id INT, vector_rank INT, text_rank INT)").await.unwrap();
        engine.query("
            INSERT INTO partial_results VALUES
            (1, 1, 1),
            (2, 2, NULL),
            (3, NULL, 2)
        ").await.unwrap();

        let sql = "SELECT id, rrf(vector_rank, text_rank) as rrf_score FROM partial_results ORDER BY rrf_score DESC";
        let result = engine.query(sql).await;

        assert!(result.is_ok(), "RRF should handle NULLs");

        let df = result.unwrap();
        let batches = df.collect().await.unwrap();

        assert_eq!(batches[0].num_rows(), 3);

        let scores = batches[0].column(1).as_any().downcast_ref::<datafusion::arrow::array::Float64Array>().unwrap();

        // Document 1 appears in both lists (highest score)
        // Document 2 only in vector (lower score)
        // Document 3 only in text (lower score)
        assert!(scores.value(0) > scores.value(1), "Doc with both ranks should score highest");
        assert!(scores.value(0) > scores.value(2), "Doc with both ranks should score highest");
    }

    /// Test RRF with three ranking sources
    #[tokio::test]
    async fn test_rrf_multiple_sources() {
        let engine = MiracleEngine::new().await.expect("Failed to create engine");

        engine.query("CREATE TABLE multi_rank (id INT, rank1 INT, rank2 INT, rank3 INT)").await.unwrap();
        engine.query("
            INSERT INTO multi_rank VALUES
            (1, 1, 1, 1),
            (2, 2, 2, 2),
            (3, 3, 3, 3)
        ").await.unwrap();

        let sql = "SELECT id, rrf(rank1, rank2, rank3) as rrf_score FROM multi_rank ORDER BY rrf_score DESC";
        let result = engine.query(sql).await;

        assert!(result.is_ok(), "RRF should work with 3+ sources");

        let df = result.unwrap();
        let batches = df.collect().await.unwrap();

        let scores = batches[0].column(1).as_any().downcast_ref::<datafusion::arrow::array::Float64Array>().unwrap();

        // All scores should be positive
        for i in 0..3 {
            assert!(scores.value(i) > 0.0);
        }

        // Should be ordered
        assert!(scores.value(0) > scores.value(1));
        assert!(scores.value(1) > scores.value(2));
    }

    /// Test complete hybrid search workflow
    #[tokio::test]
    async fn test_hybrid_search_workflow() {
        let engine = MiracleEngine::new().await.expect("Failed to create engine");

        // Create a documents table with embeddings and text content
        engine.query("
            CREATE TABLE articles (
                id INT,
                title TEXT,
                content TEXT,
                vector_similarity DOUBLE,
                text_relevance DOUBLE
            )
        ").await.unwrap();

        engine.query("
            INSERT INTO articles VALUES
            (1, 'Introduction to Machine Learning', 'ML basics...', 0.95, 0.85),
            (2, 'Deep Learning Tutorial', 'Neural networks...', 0.90, 0.70),
            (3, 'Machine Learning in Practice', 'Real ML...', 0.85, 0.90),
            (4, 'AI Research Papers', 'Latest research...', 0.70, 0.65),
            (5, 'Data Science Guide', 'Data analysis...', 0.60, 0.75)
        ").await.unwrap();

        // Method 1: Using RRF with ranks
        let sql_rrf = "
            SELECT
                id,
                title,
                rrf(vector_rank, text_rank) as hybrid_score
            FROM (
                SELECT
                    id,
                    title,
                    ROW_NUMBER() OVER (ORDER BY vector_similarity DESC) as vector_rank,
                    ROW_NUMBER() OVER (ORDER BY text_relevance DESC) as text_rank
                FROM articles
            )
            ORDER BY hybrid_score DESC
            LIMIT 3
        ";

        let result_rrf = engine.query(sql_rrf).await;
        assert!(result_rrf.is_ok());

        let df_rrf = result_rrf.unwrap();
        let batches_rrf = df_rrf.collect().await.unwrap();

        assert_eq!(batches_rrf[0].num_rows(), 3, "Should return top 3 results");

        // Method 2: Using hybrid_search() directly
        let sql_hybrid = "
            SELECT
                id,
                title,
                hybrid_search(vector_similarity, text_relevance) as relevance
            FROM articles
            ORDER BY relevance DESC
            LIMIT 3
        ";

        let result_hybrid = engine.query(sql_hybrid).await;
        assert!(result_hybrid.is_ok());

        let df_hybrid = result_hybrid.unwrap();
        let batches_hybrid = df_hybrid.collect().await.unwrap();

        assert_eq!(batches_hybrid[0].num_rows(), 3);

        // Both methods should produce meaningful rankings
        let rrf_scores = batches_rrf[0].column(2).as_any().downcast_ref::<datafusion::arrow::array::Float64Array>().unwrap();
        let hybrid_scores = batches_hybrid[0].column(2).as_any().downcast_ref::<datafusion::arrow::array::Float64Array>().unwrap();

        // All scores should be positive and ordered
        for i in 0..2 {
            assert!(rrf_scores.value(i) > 0.0);
            assert!(rrf_scores.value(i) >= rrf_scores.value(i + 1));

            assert!(hybrid_scores.value(i) > 0.0);
            assert!(hybrid_scores.value(i) >= hybrid_scores.value(i + 1));
        }
    }

    /// Test RRF error handling
    #[tokio::test]
    async fn test_rrf_error_handling() {
        let engine = MiracleEngine::new().await.expect("Failed to create engine");

        // Test with no arguments (should fail)
        let result = engine.query("SELECT rrf()").await;
        assert!(result.is_err(), "RRF with no arguments should fail");

        // Test with non-numeric types (should fail)
        engine.query("CREATE TABLE bad_ranks (id INT, rank TEXT)").await.unwrap();
        engine.query("INSERT INTO bad_ranks VALUES (1, 'first')").await.unwrap();

        let result = engine.query("SELECT rrf(rank) FROM bad_ranks").await;
        // May succeed at query time but fail at execution
        if result.is_ok() {
            let df = result.unwrap();
            let collect_result = df.collect().await;
            assert!(collect_result.is_err(), "RRF with text should fail at execution");
        }
    }

    /// Benchmark RRF performance
    #[tokio::test]
    #[ignore] // Run with --ignored flag for benchmarks
    async fn bench_rrf_performance() {
        let engine = MiracleEngine::new().await.expect("Failed to create engine");

        // Create large dataset
        engine.query("CREATE TABLE large_results (id INT, vector_rank INT, text_rank INT)").await.unwrap();

        // Insert 10,000 rows
        for i in 0..100 {
            let values: Vec<String> = (0..100)
                .map(|j| {
                    let id = i * 100 + j;
                    let v_rank = (id * 13) % 10000 + 1;
                    let t_rank = (id * 17) % 10000 + 1;
                    format!("({}, {}, {})", id, v_rank, t_rank)
                })
                .collect();

            let sql = format!("INSERT INTO large_results VALUES {}", values.join(", "));
            engine.query(&sql).await.unwrap();
        }

        // Benchmark RRF calculation
        let start = std::time::Instant::now();

        let sql = "SELECT id, rrf(vector_rank, text_rank) as score FROM large_results ORDER BY score DESC LIMIT 100";
        let result = engine.query(sql).await.unwrap();
        let _batches = result.collect().await.unwrap();

        let duration = start.elapsed();

        println!("RRF on 10,000 rows: {:?}", duration);
        assert!(duration.as_millis() < 1000, "RRF should complete in under 1 second for 10K rows");
    }
}
