//! MiracleDb Core Engine - Hybrid SQL/Document/Vector Database
//!
//! This module provides the central database engine combining:
//! - SQL queries via DataFusion
//! - Document storage (JSON/Avro)
//! - Vector search via Lance
//! - MVCC transactions with time-travel

mod transactions;
mod table_provider;
mod triggers;
mod compliance;
pub mod htap;

#[cfg(test)]
mod heap_test;

pub use transactions::*;
pub use table_provider::*;
pub use triggers::*;
pub use compliance::*;

use std::sync::Arc;
use std::collections::HashMap;
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::common::DataFusionError;
use datafusion::catalog::CatalogProvider;
use datafusion::datasource::TableProvider;
use tokio::sync::RwLock;
use tracing::{info, warn};

/// Configuration for the MiracleDb engine
#[derive(Clone, Debug)]
pub struct EngineConfig {
    /// Path to data directory
    pub data_dir: String,
    /// Enable MVCC transactions
    pub enable_transactions: bool,
    /// Enable compliance hooks
    pub enable_compliance: bool,
    /// Enable trigger system
    pub enable_triggers: bool,
    /// Maximum concurrent queries
    pub max_concurrent_queries: usize,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            data_dir: "./data".to_string(),
            enable_transactions: true,
            enable_compliance: true,
            enable_triggers: true,
            max_concurrent_queries: 100,
        }
    }
}

/// Real-time engine statistics
#[derive(Clone)]
pub struct EngineStats {
    pub start_time: std::time::Instant,
    pub total_queries: Arc<std::sync::atomic::AtomicU64>,
    pub total_transactions: Arc<std::sync::atomic::AtomicU64>,
    pub cache_hits: Arc<std::sync::atomic::AtomicU64>,
    pub cache_misses: Arc<std::sync::atomic::AtomicU64>,
}

impl EngineStats {
    pub fn new() -> Self {
        Self {
            start_time: std::time::Instant::now(),
            total_queries: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            total_transactions: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            cache_hits: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            cache_misses: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    pub fn increment_queries(&self) {
        self.total_queries.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn increment_transactions(&self) {
        self.total_transactions.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn uptime_seconds(&self) -> u64 {
        self.start_time.elapsed().as_secs()
    }

    pub fn get_total_queries(&self) -> u64 {
        self.total_queries.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn get_total_transactions(&self) -> u64 {
        self.total_transactions.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn get_cache_hit_ratio(&self) -> f64 {
        let hits = self.cache_hits.load(std::sync::atomic::Ordering::Relaxed);
        let misses = self.cache_misses.load(std::sync::atomic::Ordering::Relaxed);
        if hits + misses == 0 {
            1.0
        } else {
            hits as f64 / (hits + misses) as f64
        }
    }

    pub fn record_cache_hit(&self, cache_type: &str) {
        self.cache_hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        crate::observability::metrics::MetricsCollector::record_cache_hit(cache_type);
    }

    pub fn record_cache_miss(&self, cache_type: &str) {
        self.cache_misses.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        crate::observability::metrics::MetricsCollector::record_cache_miss(cache_type);
    }
}

impl Default for EngineStats {
    fn default() -> Self {
        Self::new()
    }
}

/// Active session information
#[derive(Clone, Debug)]
pub struct SessionEntry {
    pub pid: u32,
    pub username: String,
    pub database: String,
    pub state: String,
    pub current_query: Option<String>,
    pub connected_at: chrono::DateTime<chrono::Utc>,
    pub client_addr: String,
}

/// Session manager for tracking active connections
#[derive(Clone)]
pub struct SessionManager {
    sessions: Arc<RwLock<HashMap<u32, SessionEntry>>>,
    next_pid: Arc<std::sync::atomic::AtomicU32>,
}

impl SessionManager {
    pub fn new() -> Self {
        Self {
            sessions: Arc::new(RwLock::new(HashMap::new())),
            next_pid: Arc::new(std::sync::atomic::AtomicU32::new(1)),
        }
    }

    pub async fn create_session(&self, username: &str, database: &str, client_addr: &str) -> u32 {
        let pid = self.next_pid.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let entry = SessionEntry {
            pid,
            username: username.to_string(),
            database: database.to_string(),
            state: "idle".to_string(),
            current_query: None,
            connected_at: chrono::Utc::now(),
            client_addr: client_addr.to_string(),
        };
        self.sessions.write().await.insert(pid, entry);

        // Track connection metrics
        crate::observability::metrics::MetricsCollector::record_connection_open();

        pid
    }

    pub async fn update_query(&self, pid: u32, query: Option<String>, state: &str) {
        if let Some(session) = self.sessions.write().await.get_mut(&pid) {
            session.current_query = query;
            session.state = state.to_string();
        }
    }

    pub async fn close_session(&self, pid: u32) {
        if self.sessions.write().await.remove(&pid).is_some() {
            // Track connection metrics
            crate::observability::metrics::MetricsCollector::record_connection_close();
        }
    }

    pub async fn list_sessions(&self) -> Vec<SessionEntry> {
        self.sessions.read().await.values().cloned().collect()
    }

    pub async fn session_count(&self) -> usize {
        self.sessions.read().await.len()
    }
}

impl Default for SessionManager {
    fn default() -> Self {
        Self::new()
    }
}

/// The main MiracleDb engine
#[derive(Clone)]
pub struct MiracleEngine {
    /// DataFusion session context for SQL execution
    pub ctx: Arc<SessionContext>,
    /// Engine configuration
    pub config: EngineConfig,
    /// Transaction manager for MVCC
    pub tx_manager: Arc<TransactionManager>,
    /// Trigger registry for event-driven actions
    pub trigger_registry: Arc<RwLock<TriggerRegistry>>,
    /// Compliance manager for hooks
    pub compliance_manager: Arc<ComplianceManager>,
    /// Table metadata cache
    pub table_cache: Arc<RwLock<HashMap<String, TableMetadata>>>,
    /// Real-time statistics
    pub stats: Arc<EngineStats>,
    /// Session manager
    pub session_manager: Arc<SessionManager>,
    /// Execution Engine (Internal)
    pub execution_engine: Arc<crate::executor::ExecutionEngine>,
    /// ONNX model registry for ML inference
    #[cfg(feature = "ml")]
    pub model_registry: Arc<crate::udf::onnx::ModelRegistry>,
    /// Candle ML engine for text embeddings
    #[cfg(feature = "nlp")]
    pub candle_engine: Arc<crate::ml::CandleEngine>,
    /// Kafka sources for streaming ingestion
    pub kafka_sources: Arc<RwLock<HashMap<String, Arc<crate::integration::kafka::KafkaSource>>>>,
    /// Kafka sinks for CDC output
    pub kafka_sinks: Arc<RwLock<HashMap<String, Arc<crate::integration::kafka::KafkaSink>>>>,
    /// Spatial index manager for geospatial queries
    pub spatial_index_manager: Arc<crate::geospatial::SpatialIndexManager>,
}

/// Metadata about a registered table
#[derive(Clone, Debug)]
pub struct TableMetadata {
    pub name: String,
    pub table_type: TableType,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub version: u64,
    pub row_count: Option<u64>,
    /// Vector dimension (for Lance tables)
    pub vector_dimension: Option<usize>,
    /// Vector similarity metric (for Lance tables)
    pub vector_metric: Option<String>,
}

/// Types of tables supported
#[derive(Clone, Debug, PartialEq)]
pub enum TableType {
    /// Apache Parquet columnar storage
    Parquet,
    /// JSON document storage
    Json,
    /// Apache Avro serialization
    Avro,
    /// Lance vector storage with HNSW indexing
    Lance,
    /// In-memory Arrow table
    Memory,
}



impl MiracleEngine {
    /// Create a new MiracleDb engine with default config
    pub async fn new() -> Result<Self> {
        Self::with_config(EngineConfig::default()).await
    }

    /// Create a new MiracleDb engine with custom config
    pub async fn with_config(config: EngineConfig) -> Result<Self> {
        // Create DataFusion SessionContext with optimized settings and memory limits
        let session_config = SessionConfig::new()
            .with_target_partitions(num_cpus::get())
            .with_batch_size(8192)
            // Memory management settings
            .with_information_schema(true);
        
        // Create runtime config with memory limits (1GB pool, 512MB per query)
        // Create runtime config with memory limits and disk spilling
        // Target: 6GB RAM limit for 8GB machine (leave 2GB for OS)
        // Enable DiskManager for spilling to temp directory
        let runtime_config = datafusion::execution::runtime_env::RuntimeConfig::new()
            .with_memory_limit(6 * 1024 * 1024 * 1024, 0.8) // 6GB total, 80% threshold for spilling
            .with_disk_manager(datafusion::execution::disk_manager::DiskManagerConfig::NewOs);
        
        let runtime_env = Arc::new(
            datafusion::execution::runtime_env::RuntimeEnv::new(runtime_config)
                .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?
        );
        
        let state = datafusion::execution::context::SessionState::new_with_config_rt(
            session_config,
            runtime_env,
        );
        
        let ctx = SessionContext::new_with_state(state);
        
        // Register UDFs
        crate::udf::register_all_udfs(&ctx);

        // Initialize ONNX model registry
        #[cfg(feature = "ml")]
        let model_registry = Arc::new(
            crate::udf::onnx::ModelRegistry::new()
                .map_err(|e| datafusion::error::DataFusionError::Execution(
                    format!("Failed to initialize ONNX model registry: {}", e)
                ))?
        );

        // Register ONNX predict() UDF
        #[cfg(feature = "ml")]
        {
            let predict_udf = crate::udf::onnx::create_predict_udf(model_registry.clone());
            ctx.register_udf(predict_udf);

            let onnx_predict_udf = crate::udf::onnx::create_onnx_predict_udf(model_registry.clone());
            ctx.register_udf(onnx_predict_udf);

            info!("ONNX model registry initialized with predict() and onnx_predict() UDFs");
        }

        #[cfg(not(feature = "ml"))]
        info!("ONNX support disabled - compile with 'ml' feature to enable");

        // Initialize Candle ML engine
        #[cfg(feature = "nlp")]
        let candle_engine = match crate::ml::CandleEngine::new() {
            Ok(engine) => {
                let engine_arc = Arc::new(engine);
                crate::udf::candle::register_candle_functions(&ctx, engine_arc.clone());
                info!("Candle ML engine initialized with candle_embed() and candle_predict() UDFs");
                engine_arc
            }
            Err(e) => {
                warn!("Failed to initialize Candle ML engine: {}", e);
                info!("Candle ML UDFs not available - creating stub engine");
                // Create a fallback engine (will have empty models)
                Arc::new(crate::ml::CandleEngine::new().unwrap_or_else(|_| {
                    panic!("Could not create Candle engine")
                }))
            }
        };

        #[cfg(not(feature = "nlp"))]
        info!("Candle ML support disabled - compile with 'nlp' feature to enable");

        // Initialize transaction manager
        let tx_manager = Arc::new(TransactionManager::new(&config.data_dir));

        // Initialize trigger registry
        let trigger_registry = Arc::new(RwLock::new(TriggerRegistry::new()));

        // Initialize compliance manager
        let compliance_manager = Arc::new(ComplianceManager::new());

        // Initialize table cache
        let table_cache = Arc::new(RwLock::new(HashMap::new()));

        // Initialize stats tracker
        let stats = Arc::new(EngineStats::new());

        // Initialize session manager
        let session_manager = Arc::new(SessionManager::new());

        // Initialize Execution Engine Components
        // Re-use data_dir from config
        let smgr = Arc::new(crate::smgr::StorageManagerInterface::new(std::path::PathBuf::from(&config.data_dir)));
        // BufferPool size is in PAGES (8KB each). 128MB = 16384 pages.
        let buffer_pool = Arc::new(crate::buffer::BufferPool::new(16384, smgr.clone()));
        let catalog = Arc::new(crate::catalog::Catalog::new());
        let execution_engine = Arc::new(crate::executor::ExecutionEngine::new(catalog.clone(), buffer_pool.clone(), smgr.clone()));

        // Initialize Kafka registries
        let kafka_sources = Arc::new(RwLock::new(HashMap::new()));
        let kafka_sinks = Arc::new(RwLock::new(HashMap::new()));

        // Initialize Spatial Index Manager
        let spatial_index_path = std::path::PathBuf::from(&config.data_dir).join("spatial_indexes");
        let spatial_index_manager = Arc::new(
            crate::geospatial::SpatialIndexManager::new(
                spatial_index_path.to_str().unwrap_or("./data/spatial_indexes")
            )
        );
        // Load existing indexes from disk
        if let Err(e) = spatial_index_manager.load_all_indexes() {
            warn!("Failed to load spatial indexes: {}", e);
        } else {
            info!("Spatial index manager initialized");
        }

        // ========== Initialize Observability ==========

        // Register Prometheus metrics
        if let Err(e) = crate::observability::metrics::MetricsCollector::register_default_metrics() {
            warn!("Failed to register metrics (may already be registered): {}", e);
        }

        // Start periodic resource metric collection (every 15 seconds)
        crate::observability::metrics::ResourceMetrics::start_periodic_collection(15);

        info!("MiracleDb engine initialized with config: {:?}", config);
        info!("Observability enabled: metrics and health checks active");

        Ok(Self {
            ctx: Arc::new(ctx),
            config,
            tx_manager,
            trigger_registry,
            compliance_manager,
            table_cache,
            stats,
            session_manager,
            execution_engine,
            #[cfg(feature = "ml")]
            model_registry,
            #[cfg(feature = "nlp")]
            candle_engine,
            kafka_sources,
            kafka_sinks,
            spatial_index_manager,
        })
    }

    /// Execute a SQL query with compliance hooks
    pub async fn query(&self, sql: &str) -> Result<DataFrame> {
        // Track query statistics
        self.stats.increment_queries();

        // Determine query type for metrics
        let query_type = Self::get_query_type(sql);

        // Start query metrics tracking
        let metrics = crate::observability::metrics::QueryMetrics::new(&query_type);

        // Create tracing span for distributed tracing
        let _span = crate::observability::tracing::trace_query(sql, &query_type);

        // Before query hook (masking, access control)
        if self.config.enable_compliance {
            self.compliance_manager.before_query(sql).await?;
        }

        // Intercept custom SQL commands
        let sql_upper = sql.trim().to_uppercase();

        // ONNX Model commands
        if sql_upper.starts_with("CREATE MODEL") {
            return self.handle_create_model(sql).await;
        } else if sql_upper.starts_with("DROP MODEL") {
            return self.handle_drop_model(sql).await;
        } else if sql_upper.starts_with("SHOW MODELS") || sql_upper == "LIST MODELS" {
            return self.handle_list_models().await;
        }

        // Kafka streaming commands
        if sql_upper.starts_with("CREATE SOURCE") {
            return self.handle_create_source(sql).await;
        } else if sql_upper.starts_with("CREATE SINK") {
            return self.handle_create_sink(sql).await;
        } else if sql_upper.starts_with("DROP SOURCE") {
            return self.handle_drop_source(sql).await;
        } else if sql_upper.starts_with("DROP SINK") {
            return self.handle_drop_sink(sql).await;
        } else if sql_upper.starts_with("SHOW SOURCES") || sql_upper == "LIST SOURCES" {
            return self.handle_list_sources().await;
        } else if sql_upper.starts_with("SHOW SINKS") || sql_upper == "LIST SINKS" {
            return self.handle_list_sinks().await;
        }

        // Execute query
        let result = self.ctx.sql(sql).await;

        // After query hook (audit logging)
        if self.config.enable_compliance {
            self.compliance_manager.after_query(sql, result.is_ok()).await?;
        }

        // Record query metrics
        let status = if result.is_ok() { "success" } else { "error" };
        // For now, we can't easily get row count without collecting results
        // So we use 0 as a placeholder - this will be improved with better instrumentation
        metrics.finish(status, 0, 0);

        // Record error metric if query failed
        if result.is_err() {
            crate::observability::metrics::ERRORS_TOTAL
                .with_label_values(&["query", "engine"])
                .inc();
        }

        result
    }


    /// Execute a query within a transaction
    pub async fn query_in_transaction(&self, tx_id: u64, sql: &str) -> Result<DataFrame> {
        // Verify transaction is active
        self.tx_manager.verify_active(tx_id)?;
        
        let _version = self.tx_manager.get_snapshot_version(tx_id).await
            .ok_or(DataFusionError::Execution("Transaction not found".to_string()))?;

        // Note: We are currently not injecting the TxID into the session state due to DataFusion API limitations.
        // This means scans will use a fresh snapshot and might not see uncommitted writes from this transaction.
        // Future improvements should use TaskContext or thread-locals.

        // Track query statistics
        self.stats.increment_queries();
        
        // Before query hook (masking, access control)
        if self.config.enable_compliance {
            self.compliance_manager.before_query(sql).await?;
        }
        
        // Execute query
        let result = self.ctx.sql(sql).await;
        
        // After query hook (audit logging)
        if self.config.enable_compliance {
            self.compliance_manager.after_query(sql, result.is_ok()).await?;
        }
        
        result
    }

    /// Begin a new transaction
    pub async fn begin_transaction(&self) -> Result<u64> {
        let result = self.tx_manager.begin().await;

        // Track transaction metrics
        if result.is_ok() {
            crate::observability::metrics::TRANSACTIONS_STARTED.inc();
        } else {
            crate::observability::metrics::ERRORS_TOTAL
                .with_label_values(&["transaction", "begin"])
                .inc();
        }

        result
    }

    /// Commit a transaction
    pub async fn commit_transaction(&self, tx_id: u64) -> Result<()> {
        let start = std::time::Instant::now();
        let result = self.tx_manager.commit(tx_id).await;

        // Track transaction metrics
        if result.is_ok() {
            crate::observability::metrics::TRANSACTIONS_COMMITTED.inc();
            let duration = start.elapsed().as_secs_f64();
            crate::observability::metrics::TRANSACTION_DURATION.observe(duration);
        } else {
            crate::observability::metrics::ERRORS_TOTAL
                .with_label_values(&["transaction", "commit"])
                .inc();
        }

        result
    }

    /// Rollback a transaction
    pub async fn rollback_transaction(&self, tx_id: u64) -> Result<()> {
        let start = std::time::Instant::now();
        let result = self.tx_manager.rollback(tx_id).await;

        // Track transaction metrics
        if result.is_ok() {
            crate::observability::metrics::TRANSACTIONS_ROLLED_BACK.inc();
            let duration = start.elapsed().as_secs_f64();
            crate::observability::metrics::TRANSACTION_DURATION.observe(duration);
        } else {
            crate::observability::metrics::ERRORS_TOTAL
                .with_label_values(&["transaction", "rollback"])
                .inc();
        }

        result
    }

    /// Register a Parquet table
    pub async fn register_parquet(&self, name: &str, path: &str) -> Result<()> {
        self.ctx.register_parquet(name, path, Default::default()).await?;
        self.cache_table_metadata(name, TableType::Parquet).await;
        info!("Registered Parquet table: {}", name);
        Ok(())
    }

    /// Register a JSON table
    pub async fn register_json(&self, name: &str, path: &str) -> Result<()> {
        self.ctx.register_json(name, path, Default::default()).await?;
        self.cache_table_metadata(name, TableType::Json).await;
        info!("Registered JSON table: {}", name);
        Ok(())
    }

    /// Register a Lance vector table
    pub async fn register_lance(&self, name: &str, path: &str) -> Result<()> {
        self.register_lance_with_params(name, path, None, None).await
    }
    
    /// Register a Lance vector table with explicit vector parameters
    pub async fn register_lance_with_params(
        &self, 
        name: &str, 
        path: &str, 
        dimension: Option<usize>,
        metric: Option<&str>,
    ) -> Result<()> {
        // Create Lance table provider
        let provider = LanceTableProvider::new(path).await?;
        
        // Infer dimension from schema if not provided
        let vector_dim = dimension.or_else(|| {
            // Try to detect from schema
            let schema = provider.schema();
            for field in schema.fields() {
                if let arrow::datatypes::DataType::FixedSizeList(_, size) = field.data_type() {
                    return Some(*size as usize);
                }
            }
            None
        });
        
        self.ctx.register_table(name, Arc::new(provider))?;
        
        // Cache with vector metadata
        let mut cache = self.table_cache.write().await;
        cache.insert(name.to_string(), TableMetadata {
            name: name.to_string(),
            table_type: TableType::Lance,
            created_at: chrono::Utc::now(),
            version: 1,
            row_count: None,
            vector_dimension: vector_dim,
            vector_metric: metric.map(|s| s.to_string()).or(Some("cosine".to_string())),
        });
        
        info!("Registered Lance vector table: {} (dim={:?}, metric={:?})", 
            name, vector_dim, metric);
        Ok(())
    }

    /// Register a trigger on a table
    pub async fn register_trigger(
        &self,
        table: &str,
        event: TriggerEvent,
        wasm_module: Vec<u8>,
    ) -> Result<()> {
        let mut registry = self.trigger_registry.write().await;
        registry.register(table, event, wasm_module)?;
        info!("Registered {:?} trigger on table: {}", event, table);
        Ok(())
    }

    /// Get table metadata
    pub async fn get_table_metadata(&self, name: &str) -> Option<TableMetadata> {
        let cache = self.table_cache.read().await;
        cache.get(name).cloned()
    }

    /// List all registered tables
    pub async fn list_tables(&self) -> Vec<String> {
        // Query DataFusion's catalog instead of relying on cache
        // This ensures we capture tables created via SQL
        if let Some(catalog) = self.ctx.catalog("datafusion") {
            if let Some(schema) = catalog.schema("public") {
                return schema.table_names();
            }
        }

        // Fallback to cache if catalog unavailable
        let cache = self.table_cache.read().await;
        cache.keys().cloned().collect()
    }

    /// Cache table metadata
    async fn cache_table_metadata(&self, name: &str, table_type: TableType) {
        let mut cache = self.table_cache.write().await;
        cache.insert(name.to_string(), TableMetadata {
            name: name.to_string(),
            table_type,
            created_at: chrono::Utc::now(),
            version: 1,
            row_count: None,
            vector_dimension: None,
            vector_metric: None,
        });
    }

    /// Determine query type from SQL for metrics
    fn get_query_type(sql: &str) -> String {
        let sql_upper = sql.trim().to_uppercase();

        // Check for DDL commands
        if sql_upper.starts_with("CREATE TABLE") {
            "create_table".to_string()
        } else if sql_upper.starts_with("CREATE MODEL") {
            "create_model".to_string()
        } else if sql_upper.starts_with("CREATE SOURCE") {
            "create_source".to_string()
        } else if sql_upper.starts_with("CREATE SINK") {
            "create_sink".to_string()
        } else if sql_upper.starts_with("CREATE") {
            "create".to_string()
        } else if sql_upper.starts_with("DROP") {
            "drop".to_string()
        } else if sql_upper.starts_with("ALTER") {
            "alter".to_string()
        }
        // Check for DML commands
        else if sql_upper.starts_with("SELECT") {
            // Check for special query types
            if sql_upper.contains("VECTOR_SEARCH") || sql_upper.contains("KNN") {
                "vector_search".to_string()
            } else if sql_upper.contains("GRAPH") || sql_upper.contains("MATCH") {
                "graph".to_string()
            } else if sql_upper.contains("TIME_BUCKET") || sql_upper.contains("TIMESERIES") {
                "timeseries".to_string()
            } else {
                "select".to_string()
            }
        } else if sql_upper.starts_with("INSERT") {
            "insert".to_string()
        } else if sql_upper.starts_with("UPDATE") {
            "update".to_string()
        } else if sql_upper.starts_with("DELETE") {
            "delete".to_string()
        }
        // Check for transaction commands
        else if sql_upper.starts_with("BEGIN") || sql_upper.starts_with("START TRANSACTION") {
            "begin".to_string()
        } else if sql_upper.starts_with("COMMIT") {
            "commit".to_string()
        } else if sql_upper.starts_with("ROLLBACK") {
            "rollback".to_string()
        }
        // Check for utility commands
        else if sql_upper.starts_with("SHOW") || sql_upper.starts_with("LIST") {
            "show".to_string()
        } else if sql_upper.starts_with("DESCRIBE") || sql_upper.starts_with("DESC") {
            "describe".to_string()
        } else if sql_upper.starts_with("EXPLAIN") {
            "explain".to_string()
        }
        // Default
        else {
            "other".to_string()
        }
    }

    /// Get the DataFusion catalog for advanced operations
    pub fn catalog(&self) -> Arc<dyn CatalogProvider> {
        self.ctx.catalog("datafusion").expect("Default catalog should exist")
    }

    /// Create a new table (Heap Storage)
    pub async fn create_table(&self, name: &str, schema: arrow::datatypes::SchemaRef) -> Result<()> {
        // 1. Register with Internal Catalog (for Executor)
        // Convert Arrow Schema to Catalog Schema (Simplified)
        let columns = schema.fields().iter().map(|f| crate::catalog::ColumnDefinition {
            name: f.name().clone(),
            data_type: crate::catalog::DataType::Text, // MVP: All Text
            nullable: f.is_nullable(),
        }).collect();
        
        let table_schema = crate::catalog::TableSchema {
            name: name.to_string(),
            columns,
        };
        
        self.execution_engine.catalog.create_table(name, table_schema).await
            .map_err(|e| DataFusionError::Execution(e))?;
            
        // 2. Register Table Provider with DataFusion (for Queries)
        // We need to access the components from ExecutionEngine to build the provider
        // But ExecutionEngine fields are private/Arc... 
        // We initialized them in `new`, but didn't keep refs. 
        // We can expose `buffer_pool` and `smgr` from ExecutionEngine if we make them public or access via getters.
        // Let's assume we make them public in executor.
        
        
        // Retrieve the assigned RelFileNode from Catalog
        let rel_node = self.execution_engine.catalog.get_rel_node_by_name(name).await
            .ok_or(DataFusionError::Execution("Failed to retrieve created table node".to_string()))?;

        let buffer_pool = self.execution_engine.buffer_pool.clone();
        let _smgr = self.execution_engine.smgr.clone(); 
        let tx_manager = Some(self.tx_manager.clone());
        
        let provider = HeapTableProvider::with_node(name, schema, buffer_pool, tx_manager, rel_node);
        self.ctx.register_table(name, Arc::new(provider))?;
        
        self.cache_table_metadata(name, TableType::Parquet).await; // Using Parquet type as placeholder for "Standard/Heap"
        info!("Registered Heap table: {}", name);
        Ok(())
    }

    /// Get table schema (Arrow schema)
    pub async fn get_table_schema(&self, name: &str) -> Option<Arc<arrow::datatypes::Schema>> {
        // Try to get from catalog
        let catalog = self.ctx.catalog("datafusion")?;
        let schema = catalog.schema("public")?;
        let table = schema.table(name).await.ok()??;
        Some(table.schema())
    }

    // ==================== ONNX Model Management ====================

    /// Handle CREATE MODEL command
    /// Syntax:
    ///   CREATE MODEL model_name FROM 'path/to/model.onnx';
    ///   CREATE MODEL model_name FROM 's3://bucket/model.onnx';
    ///   CREATE MODEL model_name FROM 'https://example.com/model.onnx';
    async fn handle_create_model(&self, sql: &str) -> Result<DataFrame> {
        #[cfg(feature = "ml")]
        {
            // Parse: CREATE MODEL <name> FROM '<path_or_url>'
            let parts: Vec<&str> = sql.split_whitespace().collect();

            if parts.len() < 5 || parts[3].to_uppercase() != "FROM" {
                return Err(DataFusionError::SQL(
                    datafusion::sql::sqlparser::parser::ParserError::ParserError(
                        "Invalid CREATE MODEL syntax. Expected: CREATE MODEL name FROM 'path'".to_string()
                    ),
                    None,
                ));
            }

            let model_name = parts[2];

            // Extract path/URL from remaining parts (handle quoted strings)
            let path_start = sql.find("FROM").unwrap() + 4;
            let path_part = sql[path_start..].trim();

            // Remove quotes
            let path = path_part.trim_matches(|c| c == '\'' || c == '"' || c == ';').trim();

            info!("CREATE MODEL: name='{}', source='{}'", model_name, path);

            // Determine if URL or local path
            let is_url = path.starts_with("http://")
                || path.starts_with("https://")
                || path.starts_with("s3://")
                || path.starts_with("gs://");

            #[cfg(feature = "ml")]
            {
                if is_url {
                    self.model_registry.load_model_from_url(model_name, path).await?;
                } else {
                    self.model_registry.load_model(model_name, path)?;
                }
            }

            #[cfg(not(feature = "ml"))]
            {
                return Err(datafusion::error::DataFusionError::NotImplemented(
                    "ONNX model loading requires 'ml' feature - recompile with --features ml".to_string()
                ));
            }

            // Update ML model metrics
            crate::observability::metrics::ML_MODELS_LOADED.inc();
            info!("Model '{}' loaded successfully", model_name);

            // Return success result (empty DataFrame)
            let schema = Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("status", arrow::datatypes::DataType::Utf8, false),
                arrow::datatypes::Field::new("model_name", arrow::datatypes::DataType::Utf8, false),
            ]));

            let status_array = arrow::array::StringArray::from(vec!["success"]);
            let name_array = arrow::array::StringArray::from(vec![model_name]);

            let batch = arrow::record_batch::RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(status_array), Arc::new(name_array)],
            )
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;

            let df = self.ctx.read_batch(batch)?;
            Ok(df)
        }

        #[cfg(not(feature = "ml"))]
        {
            Err(DataFusionError::NotImplemented(
                "ONNX ML support not enabled - recompile with 'ml' feature".to_string(),
            ))
        }
    }

    /// Handle DROP MODEL command
    /// Syntax: DROP MODEL model_name;
    async fn handle_drop_model(&self, sql: &str) -> Result<DataFrame> {
        #[cfg(feature = "ml")]
        {
            let parts: Vec<&str> = sql.split_whitespace().collect();

            if parts.len() < 3 {
                return Err(DataFusionError::SQL(
                    datafusion::sql::sqlparser::parser::ParserError::ParserError(
                        "Invalid DROP MODEL syntax. Expected: DROP MODEL name".to_string()
                    ),
                    None,
                ));
            }

            let model_name = parts[2].trim_matches(|c| c == ';' || c == '\'' || c == '"');

            info!("DROP MODEL: name='{}'", model_name);

            self.model_registry.unload_model(model_name)?;

            // Update ML model metrics
            crate::observability::metrics::ML_MODELS_LOADED.dec();
            info!("Model '{}' unloaded successfully", model_name);

            // Return success result
            let schema = Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("status", arrow::datatypes::DataType::Utf8, false),
                arrow::datatypes::Field::new("model_name", arrow::datatypes::DataType::Utf8, false),
            ]));

            let status_array = arrow::array::StringArray::from(vec!["dropped"]);
            let name_array = arrow::array::StringArray::from(vec![model_name]);

            let batch = arrow::record_batch::RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(status_array), Arc::new(name_array)],
            )
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;

            let df = self.ctx.read_batch(batch)?;
            Ok(df)
        }

        #[cfg(not(feature = "ml"))]
        {
            Err(DataFusionError::NotImplemented(
                "ONNX ML support not enabled".to_string(),
            ))
        }
    }

    /// Handle LIST MODELS / SHOW MODELS command
    async fn handle_list_models(&self) -> Result<DataFrame> {
        #[cfg(feature = "ml")]
        {
            let models = self.model_registry.list_models();

            let schema = Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("model_name", arrow::datatypes::DataType::Utf8, false),
            ]));

            let name_array = arrow::array::StringArray::from(models);

            let batch = arrow::record_batch::RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(name_array)],
            )
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;

            let df = self.ctx.read_batch(batch)?;
            Ok(df)
        }

        #[cfg(not(feature = "ml"))]
        {
            Err(DataFusionError::NotImplemented(
                "ONNX ML support not enabled".to_string(),
            ))
        }
    }

    // ==================== Kafka Streaming Commands ====================

    /// Handle CREATE SOURCE command
    /// Syntax:
    ///   CREATE SOURCE source_name
    ///   FROM KAFKA (brokers='...', topic='...', group_id='...')
    ///   SCHEMA (column1 TYPE, column2 TYPE, ...);
    async fn handle_create_source(&self, sql: &str) -> Result<DataFrame> {
        #[cfg(feature = "kafka")]
        {
            // Simple parser for CREATE SOURCE
            // Format: CREATE SOURCE <name> FROM KAFKA (...) SCHEMA (...)

            // Extract source name
            let parts: Vec<&str> = sql.split_whitespace().collect();
            if parts.len() < 3 {
                return Err(DataFusionError::SQL(
                    datafusion::sql::sqlparser::parser::ParserError::ParserError(
                        "Invalid CREATE SOURCE syntax".to_string()
                    ),
                    None,
                ));
            }

            let source_name = parts[2];
            info!("CREATE SOURCE: name='{}'", source_name);

            // Parse Kafka configuration (simplified)
            // In production, use a proper SQL parser
            let config = crate::integration::kafka::KafkaSourceConfig {
                brokers: "localhost:9092".to_string(),
                topic: source_name.to_string(),
                group_id: format!("miracledb-{}", source_name),
                auto_offset_reset: "latest".to_string(),
                schema_registry_url: None,
                consumer_config: std::collections::HashMap::new(),
            };

            // Create simple schema (would normally be parsed from SCHEMA clause)
            let schema = Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
                arrow::datatypes::Field::new("data", arrow::datatypes::DataType::Utf8, true),
            ]));

            // Create Kafka source
            let source = crate::integration::kafka::KafkaSource::new(config, schema)?;

            // Store in registry
            let mut sources = self.kafka_sources.write().unwrap();
            sources.insert(source_name.to_string(), Arc::new(source));

            info!("Kafka source '{}' created", source_name);

            // Track Kafka source metric
            crate::observability::metrics::KAFKA_SOURCES_ACTIVE.inc();

            // Return success result
            let result_schema = Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("status", arrow::datatypes::DataType::Utf8, false),
                arrow::datatypes::Field::new("source_name", arrow::datatypes::DataType::Utf8, false),
            ]));

            let status_array = arrow::array::StringArray::from(vec!["created"]);
            let name_array = arrow::array::StringArray::from(vec![source_name]);

            let batch = arrow::record_batch::RecordBatch::try_new(
                result_schema.clone(),
                vec![Arc::new(status_array), Arc::new(name_array)],
            )
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;

            let df = self.ctx.read_batch(batch)?;
            Ok(df)
        }

        #[cfg(not(feature = "kafka"))]
        {
            Err(DataFusionError::NotImplemented(
                "Kafka feature not enabled - recompile with 'kafka' feature".to_string(),
            ))
        }
    }

    /// Handle CREATE SINK command
    /// Syntax:
    ///   CREATE SINK sink_name
    ///   TO KAFKA (brokers='...', topic='...')
    ///   FROM TABLE table_name;
    async fn handle_create_sink(&self, sql: &str) -> Result<DataFrame> {
        #[cfg(feature = "kafka")]
        {
            let parts: Vec<&str> = sql.split_whitespace().collect();
            if parts.len() < 3 {
                return Err(DataFusionError::SQL(
                    datafusion::sql::sqlparser::parser::ParserError::ParserError(
                        "Invalid CREATE SINK syntax".to_string()
                    ),
                    None,
                ));
            }

            let sink_name = parts[2];
            info!("CREATE SINK: name='{}'", sink_name);

            // Parse configuration
            let config = crate::integration::kafka::KafkaSinkConfig {
                brokers: "localhost:9092".to_string(),
                topic: sink_name.to_string(),
                compression: "snappy".to_string(),
                batch_size: 10000,
                linger_ms: 100,
                schema_registry_url: None,
                producer_config: std::collections::HashMap::new(),
            };

            // Simple schema
            let schema = Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
                arrow::datatypes::Field::new("data", arrow::datatypes::DataType::Utf8, true),
            ]));

            // Create Kafka sink
            let sink = crate::integration::kafka::KafkaSink::new(config, schema)?;

            // Store in registry
            let mut sinks = self.kafka_sinks.write().unwrap();
            sinks.insert(sink_name.to_string(), Arc::new(sink));

            info!("Kafka sink '{}' created", sink_name);

            // Track Kafka sink metric
            crate::observability::metrics::KAFKA_SINKS_ACTIVE.inc();

            // Return success result
            let result_schema = Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("status", arrow::datatypes::DataType::Utf8, false),
                arrow::datatypes::Field::new("sink_name", arrow::datatypes::DataType::Utf8, false),
            ]));

            let status_array = arrow::array::StringArray::from(vec!["created"]);
            let name_array = arrow::array::StringArray::from(vec![sink_name]);

            let batch = arrow::record_batch::RecordBatch::try_new(
                result_schema.clone(),
                vec![Arc::new(status_array), Arc::new(name_array)],
            )
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;

            let df = self.ctx.read_batch(batch)?;
            Ok(df)
        }

        #[cfg(not(feature = "kafka"))]
        {
            Err(DataFusionError::NotImplemented(
                "Kafka feature not enabled".to_string(),
            ))
        }
    }

    /// Handle DROP SOURCE command
    async fn handle_drop_source(&self, sql: &str) -> Result<DataFrame> {
        #[cfg(feature = "kafka")]
        {
            let parts: Vec<&str> = sql.split_whitespace().collect();
            if parts.len() < 3 {
                return Err(DataFusionError::SQL(
                    datafusion::sql::sqlparser::parser::ParserError::ParserError(
                        "Invalid DROP SOURCE syntax".to_string()
                    ),
                    None,
                ));
            }

            let source_name = parts[2].trim_matches(|c| c == ';' || c == '\'' || c == '"');
            info!("DROP SOURCE: name='{}'", source_name);

            // Remove from registry
            let mut sources = self.kafka_sources.write().unwrap();
            if sources.remove(source_name).is_some() {
                info!("Kafka source '{}' dropped", source_name);

                // Track Kafka source metric
                crate::observability::metrics::KAFKA_SOURCES_ACTIVE.dec();

                // Return success result
                let schema = Arc::new(arrow::datatypes::Schema::new(vec![
                    arrow::datatypes::Field::new("status", arrow::datatypes::DataType::Utf8, false),
                ]));

                let status_array = arrow::array::StringArray::from(vec!["dropped"]);

                let batch = arrow::record_batch::RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(status_array)],
                )
                .map_err(|e| DataFusionError::Execution(e.to_string()))?;

                let df = self.ctx.read_batch(batch)?;
                Ok(df)
            } else {
                Err(DataFusionError::Execution(format!(
                    "Source '{}' not found",
                    source_name
                )))
            }
        }

        #[cfg(not(feature = "kafka"))]
        {
            Err(DataFusionError::NotImplemented(
                "Kafka feature not enabled".to_string(),
            ))
        }
    }

    /// Handle DROP SINK command
    async fn handle_drop_sink(&self, sql: &str) -> Result<DataFrame> {
        #[cfg(feature = "kafka")]
        {
            let parts: Vec<&str> = sql.split_whitespace().collect();
            if parts.len() < 3 {
                return Err(DataFusionError::SQL(
                    datafusion::sql::sqlparser::parser::ParserError::ParserError(
                        "Invalid DROP SINK syntax".to_string()
                    ),
                    None,
                ));
            }

            let sink_name = parts[2].trim_matches(|c| c == ';' || c == '\'' || c == '"');
            info!("DROP SINK: name='{}'", sink_name);

            // Remove from registry
            let mut sinks = self.kafka_sinks.write().unwrap();
            if sinks.remove(sink_name).is_some() {
                info!("Kafka sink '{}' dropped", sink_name);

                // Track Kafka sink metric
                crate::observability::metrics::KAFKA_SINKS_ACTIVE.dec();

                let schema = Arc::new(arrow::datatypes::Schema::new(vec![
                    arrow::datatypes::Field::new("status", arrow::datatypes::DataType::Utf8, false),
                ]));

                let status_array = arrow::array::StringArray::from(vec!["dropped"]);

                let batch = arrow::record_batch::RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(status_array)],
                )
                .map_err(|e| DataFusionError::Execution(e.to_string()))?;

                let df = self.ctx.read_batch(batch)?;
                Ok(df)
            } else {
                Err(DataFusionError::Execution(format!(
                    "Sink '{}' not found",
                    sink_name
                )))
            }
        }

        #[cfg(not(feature = "kafka"))]
        {
            Err(DataFusionError::NotImplemented(
                "Kafka feature not enabled".to_string(),
            ))
        }
    }

    /// Handle LIST SOURCES / SHOW SOURCES command
    async fn handle_list_sources(&self) -> Result<DataFrame> {
        #[cfg(feature = "kafka")]
        {
            let sources = self.kafka_sources.read().unwrap();
            let source_names: Vec<String> = sources.keys().cloned().collect();

            info!("LIST SOURCES: found {} sources", source_names.len());

            let schema = Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("source_name", arrow::datatypes::DataType::Utf8, false),
            ]));

            let name_array = arrow::array::StringArray::from(source_names);

            let batch = arrow::record_batch::RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(name_array)],
            )
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;

            let df = self.ctx.read_batch(batch)?;
            Ok(df)
        }

        #[cfg(not(feature = "kafka"))]
        {
            Err(DataFusionError::NotImplemented(
                "Kafka feature not enabled".to_string(),
            ))
        }
    }

    /// Handle LIST SINKS / SHOW SINKS command
    async fn handle_list_sinks(&self) -> Result<DataFrame> {
        #[cfg(feature = "kafka")]
        {
            let sinks = self.kafka_sinks.read().unwrap();
            let sink_names: Vec<String> = sinks.keys().cloned().collect();

            info!("LIST SINKS: found {} sinks", sink_names.len());

            let schema = Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("sink_name", arrow::datatypes::DataType::Utf8, false),
            ]));

            let name_array = arrow::array::StringArray::from(sink_names);

            let batch = arrow::record_batch::RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(name_array)],
            )
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;

            let df = self.ctx.read_batch(batch)?;
            Ok(df)
        }

        #[cfg(not(feature = "kafka"))]
        {
            Err(DataFusionError::NotImplemented(
                "Kafka feature not enabled".to_string(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_engine_creation() {
        let engine = MiracleEngine::new().await.unwrap();
        assert!(engine.config.enable_transactions);
    }

    #[tokio::test]
    async fn test_simple_query() {
        let engine = MiracleEngine::new().await.unwrap();
        let df = engine.query("SELECT 1 + 1 as result").await.unwrap();
        let batches = df.collect().await.unwrap();
        assert_eq!(batches.len(), 1);
    }
}
