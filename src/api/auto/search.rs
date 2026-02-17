//! Multi-Type Search System
//!
//! Supports multiple search types in a single query:
//! - Exact match
//! - Full-text search
//! - Vector similarity
//! - Semantic search
//! - SQL WHERE clauses
//! - Range queries
//! - Geospatial queries
//! - Time series queries

use axum::{
    extract::State,
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

use super::schema::TableSchema;
use super::crud::ErrorResponse;

/// Multi-type search request
#[derive(Debug, Clone, Deserialize)]
pub struct SearchRequest {
    /// List of search types to apply
    pub search_types: Vec<SearchType>,
    /// How to combine results: "union" or "intersection"
    #[serde(default = "default_combine")]
    pub combine: CombineStrategy,
    /// Pagination
    #[serde(default)]
    pub pagination: PaginationConfig,
    /// Sorting
    #[serde(default)]
    pub sort: Vec<SortConfig>,
    /// Include related records
    #[serde(default)]
    pub include: Vec<String>,
}

fn default_combine() -> CombineStrategy {
    CombineStrategy::Union
}

/// Strategy for combining multiple search results
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum CombineStrategy {
    Union,       // OR - combine all results
    Intersection, // AND - only records matching all searches
    Rrf,         // Reciprocal Rank Fusion - intelligent hybrid ranking
}

/// Pagination configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PaginationConfig {
    #[serde(default = "default_page")]
    pub page: u32,
    #[serde(default = "default_limit")]
    pub limit: u32,
}

impl Default for PaginationConfig {
    fn default() -> Self {
        Self { page: 1, limit: 20 }
    }
}

fn default_page() -> u32 { 1 }
fn default_limit() -> u32 { 20 }

/// Sort configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SortConfig {
    pub field: String,
    #[serde(default = "default_order")]
    pub order: String,
}

fn default_order() -> String { "asc".to_string() }

/// Different types of searches
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum SearchType {
    /// Exact match on specific fields
    Exact {
        fields: Vec<String>,
        value: Value,
    },

    /// Full-text search
    Text {
        fields: Vec<String>,
        query: String,
        #[serde(default)]
        options: TextSearchOptions,
    },

    /// Vector similarity search
    Vector {
        field: String,
        /// Either provide vector directly or text to embed
        #[serde(flatten)]
        query: VectorQuery,
        #[serde(default = "default_k")]
        k: usize,
        #[serde(default = "default_threshold")]
        threshold: f32,
    },

    /// Semantic search (text to vector)
    Semantic {
        text: String,
        field: String,
        #[serde(default = "default_k")]
        k: usize,
    },

    /// SQL WHERE clause
    Sql {
        where_clause: String,
    },

    /// Range query
    Range {
        field: String,
        min: Option<Value>,
        max: Option<Value>,
    },

    /// Geospatial query
    Geo {
        field: String,
        latitude: f64,
        longitude: f64,
        radius: f64,
        #[serde(default = "default_unit")]
        unit: String,
    },

    /// Time series query
    TimeSeries {
        field: String,
        from: Option<String>,
        to: Option<String>,
        interval: Option<String>,
    },
}

fn default_k() -> usize { 10 }
fn default_threshold() -> f32 { 0.0 }
fn default_unit() -> String { "km".to_string() }

/// Vector query can be either raw vector or text to embed
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(untagged)]
pub enum VectorQuery {
    Vector { vector: Vec<f32> },
    Text { text: String },
}

/// Text search options
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TextSearchOptions {
    #[serde(default)]
    pub fuzzy: bool,
    #[serde(default = "default_operator")]
    pub operator: String, // "AND" or "OR"
    #[serde(default)]
    pub min_score: f32,
    #[serde(default)]
    pub boost: HashMap<String, f32>,
}

impl Default for TextSearchOptions {
    fn default() -> Self {
        Self {
            fuzzy: false,
            operator: "OR".to_string(),
            min_score: 0.0,
            boost: HashMap::new(),
        }
    }
}

fn default_operator() -> String { "OR".to_string() }

/// Search response
#[derive(Debug, Serialize)]
pub struct SearchResponse {
    pub results: Vec<Value>,
    pub total: usize,
    pub took_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aggregations: Option<HashMap<String, Value>>,
}

/// Search handler
pub async fn handle_search(
    schema: Arc<TableSchema>,
    engine: Arc<crate::engine::MiracleEngine>,
    Json(request): Json<SearchRequest>,
) -> Result<Json<SearchResponse>, (StatusCode, Json<ErrorResponse>)> {
    let start = std::time::Instant::now();

    // Execute each search type
    let mut result_sets: Vec<Vec<String>> = Vec::new();

    for search_type in &request.search_types {
        let ids = execute_search_type(search_type, &schema, engine.clone()).await
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: "Search execution failed".to_string(),
                        details: Some(e.to_string()),
                    }),
                )
            })?;

        result_sets.push(ids);
    }

    // Combine results based on strategy
    let final_ids = match request.combine {
        CombineStrategy::Union => combine_union(result_sets),
        CombineStrategy::Intersection => combine_intersection(result_sets),
        CombineStrategy::Rrf => combine_rrf(result_sets),
    };

    // Fetch actual records
    let results = fetch_records(&schema.table_name, &final_ids, engine).await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "Failed to fetch records".to_string(),
                    details: Some(e.to_string()),
                }),
            )
        })?;

    // Apply pagination
    let total = results.len();
    let page = request.pagination.page.max(1);
    let limit = request.pagination.limit.min(100).max(1);
    let offset = ((page - 1) * limit) as usize;
    let end = (offset + limit as usize).min(total);

    let paginated_results = results[offset..end].to_vec();

    let took_ms = start.elapsed().as_millis() as u64;

    Ok(Json(SearchResponse {
        results: paginated_results,
        total,
        took_ms,
        aggregations: None,
    }))
}

/// Execute a single search type
async fn execute_search_type(
    search_type: &SearchType,
    schema: &TableSchema,
    engine: Arc<crate::engine::MiracleEngine>,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    match search_type {
        SearchType::Exact { fields, value } => {
            execute_exact_search(&schema.table_name, fields, value, engine).await
        }
        SearchType::Text { fields, query, options } => {
            execute_text_search(&schema.table_name, fields, query, options, engine).await
        }
        SearchType::Vector { field, query, k, threshold } => {
            execute_vector_search(&schema.table_name, field, query, *k, *threshold, engine).await
        }
        SearchType::Semantic { text, field, k } => {
            execute_semantic_search(&schema.table_name, field, text, *k, engine).await
        }
        SearchType::Sql { where_clause } => {
            execute_sql_search(&schema.table_name, where_clause, engine).await
        }
        SearchType::Range { field, min, max } => {
            execute_range_search(&schema.table_name, field, min.as_ref(), max.as_ref(), engine).await
        }
        SearchType::Geo { field, latitude, longitude, radius, unit } => {
            execute_geo_search(&schema.table_name, field, *latitude, *longitude, *radius, unit, engine).await
        }
        SearchType::TimeSeries { field, from, to, interval } => {
            execute_timeseries_search(&schema.table_name, field, from.as_ref(), to.as_ref(), engine).await
        }
    }
}

// Search execution functions (implementations to be added)

async fn execute_exact_search(
    table: &str,
    fields: &[String],
    value: &Value,
    engine: Arc<crate::engine::MiracleEngine>,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    // Build WHERE clauses for each field with OR logic
    let formatted_value = match value {
        Value::String(s) => format!("'{}'", s.replace("'", "''")),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => "NULL".to_string(),
        _ => format!("'{}'", value.to_string().replace("'", "''")),
    };

    let where_conditions = fields.iter()
        .map(|field| format!("{} = {}", field, formatted_value))
        .collect::<Vec<_>>()
        .join(" OR ");

    let sql = format!("SELECT id FROM {} WHERE {}", table, where_conditions);

    tracing::debug!("Exact search query: {}", sql);

    // Execute query
    let result = engine.query(&sql).await?;
    let batches = result.collect().await?;

    // Extract IDs
    let mut ids = Vec::new();
    for batch in batches {
        if batch.num_rows() > 0 {
            for row_idx in 0..batch.num_rows() {
                if let Some(id_col) = batch.column_by_name("id") {
                    let id_str = format!("{:?}", id_col);
                    ids.push(id_str);
                }
            }
        }
    }

    Ok(ids)
}

async fn execute_text_search(
    table: &str,
    fields: &[String],
    query: &str,
    options: &TextSearchOptions,
    _engine: Arc<crate::engine::MiracleEngine>,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    use crate::fulltext::{FullTextIndexManager, TextSearchOptions as FTSearchOptions};

    tracing::debug!(
        "Full-text search on table '{}', fields: {:?}, query: '{}'",
        table,
        fields,
        query
    );

    // Create full-text index manager
    let ft_mgr = FullTextIndexManager::new("./data/fulltext");

    // Check if index exists for this table
    // For now, we'll try to search and let it fail if index doesn't exist
    // Future: add a method to check if index exists

    // Convert API options to fulltext module options
    let ft_options = FTSearchOptions {
        fuzzy: options.fuzzy,
        operator: options.operator.clone(),
        min_score: options.min_score,
        boost: options.boost.clone(),
    };

    // Execute the search
    let results = ft_mgr.search(
        table,
        query,
        100, // Default limit, can be made configurable
        &ft_options,
    ).map_err(|e| {
        format!(
            "Full-text search failed on table '{}': {}. \
            Ensure the full-text index exists and is properly configured.",
            table, e
        )
    })?;

    tracing::info!(
        "Full-text search on '{}' returned {} results for query: '{}'",
        table,
        results.len(),
        query
    );

    // Extract IDs from results
    let ids = results.into_iter()
        .map(|r| r.id)
        .collect();

    Ok(ids)
}

async fn execute_vector_search(
    table: &str,
    field: &str,
    query: &VectorQuery,
    k: usize,
    threshold: f32,
    engine: Arc<crate::engine::MiracleEngine>,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    // Get the vector to search with
    let search_vector = match query {
        VectorQuery::Vector { vector } => vector.clone(),
        VectorQuery::Text { text } => {
            // Convert text to embedding using Candle ML engine
            #[cfg(feature = "nlp")]
            {
                tracing::debug!("Converting text to embedding: '{}'", text);

                // Use default model name for now
                // TODO: Make model name configurable via API or config
                let model_name = "default";

                match engine.candle_engine.generate_embedding(model_name, text) {
                    Ok(embedding) => {
                        tracing::info!("Generated embedding with {} dimensions", embedding.len());
                        embedding
                    }
                    Err(e) => {
                        tracing::warn!("Failed to generate embedding: {}. Ensure a model is loaded.", e);
                        return Err(format!(
                            "Failed to generate embedding for text '{}': {}. \
                            Please load a model first using the Candle API. \
                            Example: engine.candle_engine.download_model_from_hf(...)",
                            text, e
                        ).into());
                    }
                }
            }

            #[cfg(not(feature = "nlp"))]
            {
                return Err(format!(
                    "Text-to-vector embedding requires 'nlp' feature. \
                    Please compile with --features nlp or provide raw vectors. \
                    Text query was: '{}'",
                    text
                ).into());
            }
        }
    };

    // Create vector index manager
    // TODO: Make base_path configurable, for now use "./data/vectors"
    let vector_mgr = crate::vector::VectorIndexManager::new("./data/vectors");

    // Check if dataset exists
    if !vector_mgr.dataset_exists(table).await {
        return Err(format!(
            "Vector dataset for table '{}' not found. \
            Please ensure Lance dataset exists at ./data/vectors/{}",
            table, table
        ).into());
    }

    // Perform vector search
    let threshold_opt = if threshold > 0.0 { Some(threshold) } else { None };

    let results = vector_mgr.search(
        table,
        field,
        &search_vector,
        k,
        threshold_opt,
    ).await.map_err(|e| {
        format!("Vector search failed on {}.{}: {}", table, field, e)
    })?;

    tracing::info!(
        "Vector search on {}.{} returned {} results (k={}, threshold={:?})",
        table,
        field,
        results.len(),
        k,
        threshold_opt
    );

    // Extract IDs from results
    let ids = results.into_iter()
        .map(|r| r.id)
        .collect();

    Ok(ids)
}

async fn execute_semantic_search(
    table: &str,
    field: &str,
    text: &str,
    k: usize,
    engine: Arc<crate::engine::MiracleEngine>,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    // Semantic search: Convert text to embedding, then do vector search
    tracing::debug!(
        "Semantic search on {}.{} for text: '{}'",
        table, field, text
    );

    #[cfg(feature = "nlp")]
    {
        // Generate embedding using Candle
        let model_name = "default";
        let embedding = engine.candle_engine.generate_embedding(model_name, text)
            .map_err(|e| {
                format!(
                    "Failed to generate embedding for semantic search: {}. \
                    Please load a model first using the Candle API.",
                    e
                )
            })?;

        tracing::info!("Generated embedding with {} dimensions for semantic search", embedding.len());

        // Perform vector search with the embedding
        let vector_mgr = crate::vector::VectorIndexManager::new("./data/vectors");

        if !vector_mgr.dataset_exists(table).await {
            return Err(format!(
                "Vector dataset for table '{}' not found. \
                Please ensure Lance dataset exists at ./data/vectors/{}",
                table, table
            ).into());
        }

        let results = vector_mgr.search(
            table,
            field,
            &embedding,
            k,
            None, // No threshold for semantic search
        ).await.map_err(|e| {
            format!("Semantic search failed on {}.{}: {}", table, field, e)
        })?;

        tracing::info!(
            "Semantic search on {}.{} returned {} results",
            table, field, results.len()
        );

        Ok(results.into_iter().map(|r| r.id).collect())
    }

    #[cfg(not(feature = "nlp"))]
    {
        Err(format!(
            "Semantic search requires 'nlp' feature. \
            Please compile with --features nlp. \
            Text query: '{}'",
            text
        ).into())
    }
}

async fn execute_sql_search(
    table: &str,
    where_clause: &str,
    engine: Arc<crate::engine::MiracleEngine>,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    // Execute SQL query with custom WHERE clause
    let sql = format!("SELECT id FROM {} WHERE {}", table, where_clause);

    tracing::debug!("SQL search query: {}", sql);

    let result = engine.query(&sql).await?;
    let batches = result.collect().await?;

    // Extract IDs from result
    let mut ids = Vec::new();
    for batch in batches {
        if batch.num_rows() > 0 {
            for row_idx in 0..batch.num_rows() {
                if let Some(id_col) = batch.column_by_name("id") {
                    let id_str = format!("{:?}", id_col);
                    ids.push(id_str);
                }
            }
        }
    }

    Ok(ids)
}

async fn execute_range_search(
    table: &str,
    field: &str,
    min: Option<&Value>,
    max: Option<&Value>,
    engine: Arc<crate::engine::MiracleEngine>,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    // Build range query WHERE clause
    let mut conditions = Vec::new();

    if let Some(min_val) = min {
        let formatted_min = match min_val {
            Value::String(s) => format!("'{}'", s.replace("'", "''")),
            Value::Number(n) => n.to_string(),
            _ => format!("'{}'", min_val.to_string().replace("'", "''")),
        };
        conditions.push(format!("{} >= {}", field, formatted_min));
    }

    if let Some(max_val) = max {
        let formatted_max = match max_val {
            Value::String(s) => format!("'{}'", s.replace("'", "''")),
            Value::Number(n) => n.to_string(),
            _ => format!("'{}'", max_val.to_string().replace("'", "''")),
        };
        conditions.push(format!("{} <= {}", field, formatted_max));
    }

    if conditions.is_empty() {
        return Ok(vec![]);
    }

    let where_clause = conditions.join(" AND ");
    let sql = format!("SELECT id FROM {} WHERE {}", table, where_clause);

    tracing::debug!("Range search query: {}", sql);

    let result = engine.query(&sql).await?;
    let batches = result.collect().await?;

    // Extract IDs
    let mut ids = Vec::new();
    for batch in batches {
        if batch.num_rows() > 0 {
            for row_idx in 0..batch.num_rows() {
                if let Some(id_col) = batch.column_by_name("id") {
                    let id_str = format!("{:?}", id_col);
                    ids.push(id_str);
                }
            }
        }
    }

    Ok(ids)
}

async fn execute_geo_search(
    table: &str,
    field: &str,
    lat: f64,
    lon: f64,
    radius: f64,
    unit: &str,
    engine: Arc<crate::engine::MiracleEngine>,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    // Convert radius to degrees (approximate)
    // 1 degree of latitude ≈ 111 km
    // 1 degree of longitude varies by latitude, using approximation: 111 * cos(lat)
    let radius_degrees = match unit {
        "km" => radius / 111.0,
        "miles" | "mi" => radius / 69.0, // 1 mile ≈ 1.6 km, 111/1.6 ≈ 69
        "m" | "meters" => radius / 111000.0,
        _ => radius / 111.0, // Default to km
    };

    // Calculate bounding box
    let lat_radians = lat.to_radians();
    let lon_delta = radius_degrees / lat_radians.cos().abs().max(0.1); // Avoid division by zero near poles

    let min_lat = lat - radius_degrees;
    let max_lat = lat + radius_degrees;
    let min_lon = lon - lon_delta;
    let max_lon = lon + lon_delta;

    // Try to find a spatial index for this table
    // Look for common geospatial column pairs
    let column_pairs = vec![
        ("latitude", "longitude"),
        ("lat", "lon"),
        ("lat", "lng"),
        ("y", "x"),
    ];

    for (lat_col, lon_col) in column_pairs {
        // Check if spatial index exists
        if let Ok(_metadata) = engine
            .spatial_index_manager
            .get_metadata(table, lon_col, lat_col)
        {
            tracing::info!(
                "Using spatial index for table '{}' on ({}, {})",
                table,
                lon_col,
                lat_col
            );

            // Use R-tree bounding box search
            match engine.spatial_index_manager.search_box(
                table,
                lon_col,
                lat_col,
                min_lon,
                min_lat,
                max_lon,
                max_lat,
            ) {
                Ok(row_ids) => {
                    // Convert row_ids to string IDs
                    let ids: Vec<String> = row_ids.iter().map(|id| id.to_string()).collect();
                    tracing::info!(
                        "Spatial index search found {} candidates for table '{}'",
                        ids.len(),
                        table
                    );
                    return Ok(ids);
                }
                Err(e) => {
                    tracing::warn!("Spatial index search failed: {}, falling back to SQL", e);
                }
            }
        }
    }

    // Fallback: Use SQL with ST_Distance UDF
    tracing::info!("No spatial index found, using SQL fallback for table '{}'", table);

    // Build SQL query using ST_Distance UDF
    let sql = format!(
        "SELECT id FROM {} WHERE ST_Distance({}, {}, {}, {}) < {}",
        table,
        field.split('.').nth(0).unwrap_or("latitude"),
        field.split('.').nth(1).unwrap_or("longitude"),
        lat,
        lon,
        radius_degrees
    );

    // Execute query
    match engine.query(&sql).await {
        Ok(df) => {
            // Extract IDs from result
            let batches = df.collect().await?;
            let mut ids = Vec::new();

            for batch in batches {
                if let Some(id_col) = batch.column_by_name("id") {
                    let id_str = format!("{:?}", id_col);
                    ids.push(id_str);
                }
            }

            Ok(ids)
        }
        Err(e) => {
            tracing::error!("Geospatial query failed: {}", e);
            Err(format!("Geospatial query failed: {}", e).into())
        }
    }
}

async fn execute_timeseries_search(
    table: &str,
    field: &str,
    from: Option<&String>,
    to: Option<&String>,
    engine: Arc<crate::engine::MiracleEngine>,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    // Build time range query
    let mut conditions = Vec::new();

    if let Some(from_time) = from {
        conditions.push(format!("{} >= '{}'", field, from_time.replace("'", "''")));
    }

    if let Some(to_time) = to {
        conditions.push(format!("{} <= '{}'", field, to_time.replace("'", "''")));
    }

    if conditions.is_empty() {
        return Ok(vec![]);
    }

    let where_clause = conditions.join(" AND ");
    let sql = format!("SELECT id FROM {} WHERE {}", table, where_clause);

    tracing::debug!("Time series search query: {}", sql);

    let result = engine.query(&sql).await?;
    let batches = result.collect().await?;

    // Extract IDs
    let mut ids = Vec::new();
    for batch in batches {
        if batch.num_rows() > 0 {
            for row_idx in 0..batch.num_rows() {
                if let Some(id_col) = batch.column_by_name("id") {
                    let id_str = format!("{:?}", id_col);
                    ids.push(id_str);
                }
            }
        }
    }

    Ok(ids)
}

// Result combination functions

fn combine_union(result_sets: Vec<Vec<String>>) -> Vec<String> {
    let mut all_ids = std::collections::HashSet::new();
    for ids in result_sets {
        for id in ids {
            all_ids.insert(id);
        }
    }
    all_ids.into_iter().collect()
}

fn combine_intersection(result_sets: Vec<Vec<String>>) -> Vec<String> {
    if result_sets.is_empty() {
        return vec![];
    }

    let mut result: std::collections::HashSet<String> =
        result_sets[0].iter().cloned().collect();

    for ids in &result_sets[1..] {
        let set: std::collections::HashSet<String> = ids.iter().cloned().collect();
        result = result.intersection(&set).cloned().collect();
    }

    result.into_iter().collect()
}

/// Combine results using Reciprocal Rank Fusion (RRF)
/// This is the "killer feature" that intelligently ranks results from multiple sources
fn combine_rrf(result_sets: Vec<Vec<String>>) -> Vec<String> {
    use crate::search::rrf::{RRFEngine, SearchResult as RRFSearchResult};

    if result_sets.is_empty() {
        return vec![];
    }

    let rrf_engine = RRFEngine::new();

    // Convert Vec<Vec<String>> to format RRF expects
    let rrf_input: Vec<(String, Vec<RRFSearchResult>)> = result_sets
        .into_iter()
        .enumerate()
        .map(|(source_idx, ids)| {
            let source_name = format!("source_{}", source_idx);
            let results: Vec<RRFSearchResult> = ids
                .into_iter()
                .enumerate()
                .map(|(rank, id)| RRFSearchResult {
                    id: id.clone(),
                    score: 0.0,
                    rank,
                    original_score: None,
                    source: source_name.clone(),
                    data: serde_json::json!({"id": id}),
                })
                .collect();
            (source_name, results)
        })
        .collect();

    // Apply RRF
    let rrf_results = rrf_engine.combine(rrf_input);

    // Extract IDs in RRF-ranked order
    rrf_results.into_iter().map(|r| r.id).collect()
}

async fn fetch_records(
    table: &str,
    ids: &[String],
    engine: Arc<crate::engine::MiracleEngine>,
) -> Result<Vec<Value>, Box<dyn std::error::Error>> {
    if ids.is_empty() {
        return Ok(vec![]);
    }

    // Build WHERE id IN (...) clause
    let id_list = ids.iter()
        .map(|id| format!("'{}'", id.replace("'", "''")))
        .collect::<Vec<_>>()
        .join(", ");

    let sql = format!("SELECT * FROM {} WHERE id IN ({})", table, id_list);

    tracing::debug!("Fetching records: {}", sql);

    let result = engine.query(&sql).await?;
    let batches = result.collect().await?;

    // Convert Arrow batches to JSON
    let mut rows = Vec::new();
    for batch in &batches {
        let schema = batch.schema();
        for row_idx in 0..batch.num_rows() {
            let mut row = serde_json::Map::new();
            for col_idx in 0..batch.num_columns() {
                let col_name = schema.field(col_idx).name().clone();
                let val_str = format!("{:?}", batch.column(col_idx));
                row.insert(col_name, Value::String(val_str));
            }
            rows.push(Value::Object(row));
        }
    }

    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_combine_union() {
        let result_sets = vec![
            vec!["1".to_string(), "2".to_string()],
            vec!["2".to_string(), "3".to_string()],
        ];
        let result = combine_union(result_sets);
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_combine_intersection() {
        let result_sets = vec![
            vec!["1".to_string(), "2".to_string(), "3".to_string()],
            vec!["2".to_string(), "3".to_string(), "4".to_string()],
        ];
        let result = combine_intersection(result_sets);
        assert_eq!(result.len(), 2);
        assert!(result.contains(&"2".to_string()));
        assert!(result.contains(&"3".to_string()));
    }
}
