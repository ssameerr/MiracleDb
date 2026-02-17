//! RRF Hybrid Search Example - Combining Vector and Text Search
//!
//! This example demonstrates Reciprocal Rank Fusion (RRF) for creating
//! powerful hybrid search systems that combine:
//! - Vector search (semantic similarity using embeddings)
//! - Text search (keyword relevance using BM25/full-text)
//!
//! RRF intelligently merges multiple ranking signals into a single
//! unified ranking, producing better search results than either method alone.
//!
//! Run with:
//! ```bash
//! cargo run --example rrf_hybrid_search
//! ```

use miracledb::engine::{MiracleEngine, EngineConfig};
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    println!("=== MiracleDb RRF Hybrid Search Example ===\n");

    // 1. Create engine
    let engine = MiracleEngine::new().await?;
    println!("✓ Engine initialized with RRF support\n");

    // 2. Create a documents table with both embeddings and text content
    println!("Creating documents table...");

    engine.query("
        CREATE TABLE articles (
            id BIGINT,
            title TEXT,
            content TEXT,
            category TEXT,
            author TEXT,
            published_date BIGINT
        )
    ").await?;

    // Insert sample articles
    engine.query("
        INSERT INTO articles VALUES
        (1, 'Introduction to Machine Learning', 'Machine learning is a subset of AI that enables systems to learn from data...', 'AI', 'Alice Chen', 1706100000),
        (2, 'Deep Learning Fundamentals', 'Deep learning uses neural networks with multiple layers to model complex patterns...', 'AI', 'Bob Smith', 1706110000),
        (3, 'Natural Language Processing Guide', 'NLP combines linguistics and machine learning to understand human language...', 'AI', 'Carol Lee', 1706120000),
        (4, 'Computer Vision Techniques', 'Computer vision enables machines to interpret visual information from images...', 'AI', 'Dave Kumar', 1706130000),
        (5, 'Reinforcement Learning Explained', 'RL trains agents to make decisions by rewarding desired behaviors...', 'AI', 'Eve Zhang', 1706140000),
        (6, 'Data Science for Beginners', 'Data science combines statistics, programming, and domain knowledge...', 'Data', 'Frank Wilson', 1706150000),
        (7, 'Python Programming Basics', 'Python is a versatile language used in web development, data science, and AI...', 'Programming', 'Grace Park', 1706160000),
        (8, 'Machine Learning Algorithms Comparison', 'Compare decision trees, random forests, SVM, and neural networks...', 'AI', 'Alice Chen', 1706170000),
        (9, 'Building Recommendation Systems', 'Recommendation systems use collaborative filtering and content-based methods...', 'AI', 'Bob Smith', 1706180000),
        (10, 'Time Series Forecasting with ML', 'Apply ARIMA, LSTM, and Prophet for predicting future values...', 'AI', 'Carol Lee', 1706190000)
    ").await?;

    println!("✓ Inserted 10 sample articles\n");

    // 3. Simulate vector similarity scores (in production, these would come from embeddings)
    println!("Adding vector similarity scores...");

    engine.query("
        ALTER TABLE articles ADD COLUMN vector_score DOUBLE
    ").await.ok(); // May fail if column exists

    // Simulate vector search results for query: 'machine learning tutorial'
    engine.query("
        CREATE TABLE vector_search_results AS
        SELECT
            id,
            title,
            CASE
                WHEN id = 1 THEN 0.95  -- 'Introduction to Machine Learning' - exact match
                WHEN id = 8 THEN 0.90  -- 'ML Algorithms' - related
                WHEN id = 2 THEN 0.85  -- 'Deep Learning' - related
                WHEN id = 3 THEN 0.75  -- 'NLP' - somewhat related
                WHEN id = 10 THEN 0.70 -- 'Time Series' - related
                WHEN id = 5 THEN 0.65  -- 'RL' - somewhat related
                WHEN id = 9 THEN 0.60  -- 'Recommendation' - related
                ELSE 0.40              -- Lower relevance
            END as vector_score
        FROM articles
    ").await?;

    // Simulate text search results (BM25 scores)
    engine.query("
        CREATE TABLE text_search_results AS
        SELECT
            id,
            title,
            CASE
                WHEN id = 1 THEN 0.92  -- Contains 'machine learning'
                WHEN id = 8 THEN 0.88  -- Contains 'machine learning algorithms'
                WHEN id = 10 THEN 0.82 -- Contains 'ML'
                WHEN id = 2 THEN 0.75  -- Contains 'learning'
                WHEN id = 3 THEN 0.70  -- Contains 'machine learning' in content
                WHEN id = 6 THEN 0.65  -- Mentions 'machine learning' in data science
                ELSE 0.35              -- Lower text relevance
            END as text_score
        FROM articles
    ").await?;

    println!("✓ Created search result tables\n");

    // 4. Method 1: Using RRF with ROW_NUMBER() ranks
    println!("Method 1: RRF with ROW_NUMBER() ranks");
    println!("======================================\n");

    let rrf_query = "
        SELECT
            id,
            title,
            vector_rank,
            text_rank,
            rrf(vector_rank, text_rank) as hybrid_score
        FROM (
            SELECT
                a.id,
                a.title,
                ROW_NUMBER() OVER (ORDER BY COALESCE(v.vector_score, 0) DESC) as vector_rank,
                ROW_NUMBER() OVER (ORDER BY COALESCE(t.text_score, 0) DESC) as text_rank
            FROM articles a
            LEFT JOIN vector_search_results v ON a.id = v.id
            LEFT JOIN text_search_results t ON a.id = t.id
        )
        ORDER BY hybrid_score DESC
        LIMIT 5
    ";

    println!("Query:\n{}\n", rrf_query);

    let result = engine.query(rrf_query).await?;
    let batches = result.collect().await?;

    println!("Top 5 Results (RRF Method):");
    println!("---------------------------");

    for batch in &batches {
        for row in 0..batch.num_rows() {
            let id = batch.column(0).as_any().downcast_ref::<datafusion::arrow::array::Int64Array>().unwrap().value(row);
            let title_scalar = datafusion::common::ScalarValue::try_from_array(batch.column(1), row)?;
            let v_rank = batch.column(2).as_any().downcast_ref::<datafusion::arrow::array::Int64Array>().unwrap().value(row);
            let t_rank = batch.column(3).as_any().downcast_ref::<datafusion::arrow::array::Int64Array>().unwrap().value(row);
            let score = batch.column(4).as_any().downcast_ref::<datafusion::arrow::array::Float64Array>().unwrap().value(row);

            println!("{}. ID:{} - {}", row + 1, id, title_scalar);
            println!("   Vector Rank: {}, Text Rank: {}, RRF Score: {:.4}", v_rank, t_rank, score);
        }
    }
    println!();

    // 5. Method 2: Using hybrid_search() UDF directly with scores
    println!("Method 2: hybrid_search() with direct scores");
    println!("============================================\n");

    let hybrid_query = "
        SELECT
            a.id,
            a.title,
            COALESCE(v.vector_score, 0) as vector_score,
            COALESCE(t.text_score, 0) as text_score,
            hybrid_search(COALESCE(v.vector_score, 0), COALESCE(t.text_score, 0)) as relevance
        FROM articles a
        LEFT JOIN vector_search_results v ON a.id = v.id
        LEFT JOIN text_search_results t ON a.id = t.id
        ORDER BY relevance DESC
        LIMIT 5
    ";

    println!("Query:\n{}\n", hybrid_query);

    let result = engine.query(hybrid_query).await?;
    let batches = result.collect().await?;

    println!("Top 5 Results (hybrid_search Method):");
    println!("--------------------------------------");

    for batch in &batches {
        for row in 0..batch.num_rows() {
            let id = batch.column(0).as_any().downcast_ref::<datafusion::arrow::array::Int64Array>().unwrap().value(row);
            let title_scalar = datafusion::common::ScalarValue::try_from_array(batch.column(1), row)?;
            let v_score = batch.column(2).as_any().downcast_ref::<datafusion::arrow::array::Float64Array>().unwrap().value(row);
            let t_score = batch.column(3).as_any().downcast_ref::<datafusion::arrow::array::Float64Array>().unwrap().value(row);
            let relevance = batch.column(4).as_any().downcast_ref::<datafusion::arrow::array::Float64Array>().unwrap().value(row);

            println!("{}. ID:{} - {}", row + 1, id, title_scalar);
            println!("   Vector: {:.2}, Text: {:.2}, Relevance: {:.3}", v_score, t_score, relevance);
        }
    }
    println!();

    // 6. Method 3: Custom RRF with different k parameter
    println!("Method 3: RRF with custom k parameter (k=30)");
    println!("============================================\n");

    let custom_k_query = "
        SELECT
            id,
            title,
            rrf_k(vector_rank, text_rank, 30.0) as aggressive_score
        FROM (
            SELECT
                a.id,
                a.title,
                ROW_NUMBER() OVER (ORDER BY COALESCE(v.vector_score, 0) DESC) as vector_rank,
                ROW_NUMBER() OVER (ORDER BY COALESCE(t.text_score, 0) DESC) as text_rank
            FROM articles a
            LEFT JOIN vector_search_results v ON a.id = v.id
            LEFT JOIN text_search_results t ON a.id = t.id
        )
        ORDER BY aggressive_score DESC
        LIMIT 5
    ";

    println!("Query:\n{}\n", custom_k_query);

    let result = engine.query(custom_k_query).await?;
    let batches = result.collect().await?;

    println!("Top 5 Results (Custom k=30):");
    println!("----------------------------");

    for batch in &batches {
        for row in 0..batch.num_rows() {
            let id = batch.column(0).as_any().downcast_ref::<datafusion::arrow::array::Int64Array>().unwrap().value(row);
            let title_scalar = datafusion::common::ScalarValue::try_from_array(batch.column(1), row)?;
            let score = batch.column(2).as_any().downcast_ref::<datafusion::arrow::array::Float64Array>().unwrap().value(row);

            println!("{}. ID:{} - {}", row + 1, id, title_scalar);
            println!("   Aggressive RRF Score (k=30): {:.4}", score);
        }
    }
    println!();

    // 7. Comparison: Vector-only vs Text-only vs Hybrid
    println!("Comparison: Different Search Methods");
    println!("====================================\n");

    // Vector-only
    println!("Vector Search Only (Top 3):");
    let vector_only = engine.query("
        SELECT id, title, vector_score
        FROM vector_search_results
        ORDER BY vector_score DESC
        LIMIT 3
    ").await?;
    let batches = vector_only.collect().await?;
    for batch in &batches {
        for row in 0..batch.num_rows() {
            let id = batch.column(0).as_any().downcast_ref::<datafusion::arrow::array::Int64Array>().unwrap().value(row);
            let title_scalar = datafusion::common::ScalarValue::try_from_array(batch.column(1), row)?;
            let score = batch.column(2).as_any().downcast_ref::<datafusion::arrow::array::Float64Array>().unwrap().value(row);
            println!("  {}. ID:{} - {} (score: {:.2})", row + 1, id, title_scalar, score);
        }
    }
    println!();

    // Text-only
    println!("Text Search Only (Top 3):");
    let text_only = engine.query("
        SELECT id, title, text_score
        FROM text_search_results
        ORDER BY text_score DESC
        LIMIT 3
    ").await?;
    let batches = text_only.collect().await?;
    for batch in &batches {
        for row in 0..batch.num_rows() {
            let id = batch.column(0).as_any().downcast_ref::<datafusion::arrow::array::Int64Array>().unwrap().value(row);
            let title_scalar = datafusion::common::ScalarValue::try_from_array(batch.column(1), row)?;
            let score = batch.column(2).as_any().downcast_ref::<datafusion::arrow::array::Float64Array>().unwrap().value(row);
            println!("  {}. ID:{} - {} (score: {:.2})", row + 1, id, title_scalar, score);
        }
    }
    println!();

    // Hybrid
    println!("Hybrid Search (RRF) (Top 3):");
    println!("  (See results above)\n");

    // 8. Advanced use case: Filter + Hybrid search
    println!("Advanced: Filtered Hybrid Search");
    println!("=================================\n");

    let filtered_hybrid = "
        SELECT
            a.id,
            a.title,
            a.author,
            rrf(vector_rank, text_rank) as hybrid_score
        FROM (
            SELECT
                a.id,
                a.title,
                a.author,
                ROW_NUMBER() OVER (ORDER BY COALESCE(v.vector_score, 0) DESC) as vector_rank,
                ROW_NUMBER() OVER (ORDER BY COALESCE(t.text_score, 0) DESC) as text_rank
            FROM articles a
            LEFT JOIN vector_search_results v ON a.id = v.id
            LEFT JOIN text_search_results t ON a.id = t.id
            WHERE a.category = 'AI'  -- Filter by category
              AND a.published_date > 1706100000  -- Recent articles only
        )
        ORDER BY hybrid_score DESC
        LIMIT 3
    ";

    println!("Query: Filter by category='AI' and recent date\n");

    let result = engine.query(filtered_hybrid).await?;
    let batches = result.collect().await?;

    println!("Top 3 Filtered Results:");
    for batch in &batches {
        for row in 0..batch.num_rows() {
            let id = batch.column(0).as_any().downcast_ref::<datafusion::arrow::array::Int64Array>().unwrap().value(row);
            let title_scalar = datafusion::common::ScalarValue::try_from_array(batch.column(1), row)?;
            let author_scalar = datafusion::common::ScalarValue::try_from_array(batch.column(2), row)?;
            let score = batch.column(3).as_any().downcast_ref::<datafusion::arrow::array::Float64Array>().unwrap().value(row);
            println!("  {}. ID:{} - {} by {}", row + 1, id, title_scalar, author_scalar);
            println!("     RRF Score: {:.4}", score);
        }
    }
    println!();

    // 9. Performance and scaling tips
    println!("Performance Tips:");
    println!("=================");
    println!("1. Indexing: Create indexes on score columns for faster sorting");
    println!("   CREATE INDEX idx_vector_score ON vector_search_results(vector_score DESC);");
    println!();
    println!("2. Materialized Views: Pre-compute rankings for frequently used queries");
    println!("   CREATE MATERIALIZED VIEW top_results AS SELECT ... ORDER BY rrf(...);");
    println!();
    println!("3. Batch Processing: Process multiple queries in parallel");
    println!("   Use connection pooling and async queries");
    println!();
    println!("4. k Parameter Tuning:");
    println!("   - Lower k (e.g., 30): More aggressive, emphasizes top-ranked items");
    println!("   - Higher k (e.g., 100): More balanced, considers broader range");
    println!("   - Default k=60: Good balance for most use cases");
    println!();

    // 10. Integration examples
    println!("Integration Examples:");
    println!("=====================\n");

    println!("RRF + Vector Search (Lance):");
    println!("  SELECT id, rrf(");
    println!("    ROW_NUMBER() OVER (ORDER BY vector_distance(embedding, query_vector)),");
    println!("    ROW_NUMBER() OVER (ORDER BY text_score(content, 'machine learning') DESC)");
    println!("  ) as hybrid_score");
    println!("  FROM documents;");
    println!();

    println!("RRF + Full-Text Search (Tantivy):");
    println!("  SELECT id, rrf(");
    println!("    ROW_NUMBER() OVER (ORDER BY semantic_similarity DESC),");
    println!("    ROW_NUMBER() OVER (ORDER BY fts_score(content, 'query') DESC)");
    println!("  ) as hybrid_score");
    println!("  FROM articles;");
    println!();

    println!("RRF + ML Predictions:");
    println!("  SELECT id, rrf(");
    println!("    ROW_NUMBER() OVER (ORDER BY vector_score DESC),");
    println!("    ROW_NUMBER() OVER (ORDER BY predict('relevance_model', features) DESC)");
    println!("  ) as hybrid_score");
    println!("  FROM search_results;");
    println!();

    println!("✓ Example complete!\n");

    println!("Key Takeaways:");
    println!("==============");
    println!("✓ RRF combines multiple ranking signals effectively");
    println!("✓ Works with vector search, text search, ML predictions, and more");
    println!("✓ Three methods: rrf(), rrf_k(), hybrid_search()");
    println!("✓ Tune k parameter based on your ranking aggressiveness needs");
    println!("✓ Combine with filters, ordering, and other SQL features");
    println!();

    Ok(())
}
