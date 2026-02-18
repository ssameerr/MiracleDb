use tokio::net::TcpListener;
use std::path::PathBuf;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;
use clap::{Parser, Subcommand};


pub mod engine;
pub mod embedded;
pub mod api;
pub mod udf;
pub mod security;
pub mod cluster;
// pub mod udf; // Removed duplicate
pub mod rlm;
pub mod common; // Shared types

// MiracleAuth - Post-Quantum Security Suite
pub mod auth;
pub mod ratchet;
pub mod mpc;
pub mod signing;

// Advanced feature modules
pub mod audit;
pub mod optimizer;
pub mod blob;
pub mod chat;
pub mod realtime;
pub mod vector;
pub mod fulltext;
pub mod geospatial;
pub mod timeseries;
pub mod graph;
pub mod store;
pub mod logs;
// pub mod chat; // Duplicate
pub mod extensions;
pub mod nucleus;

// Enterprise modules
pub mod observability;
pub mod backup;
pub mod cache;
pub mod governance;
pub mod federation;
pub mod integration;
pub mod analytics;
pub mod quality;
pub mod financial;
pub mod healthcare;
pub mod iot;
pub mod protocol;

// Time Travel & Data Versioning
pub mod version;

// Specialized modules
pub mod nlp;
pub mod ml;
pub mod blockchain;
pub mod scheduler;
pub mod replication;
pub mod search;
pub mod plugin;
pub mod session;
pub mod ratelimit;
pub mod pool;
pub mod migration;
pub mod health;
pub mod profiler;
pub mod config;
pub mod compression;
pub mod crypto;
pub mod streaming;
pub mod events;
pub mod metrics;
pub mod locking;
pub mod snapshot;
pub mod plan;
pub mod schema;
pub mod index;
pub mod partition;
pub mod notification;
pub mod workflow;
pub mod stats;
pub mod lineage;
pub mod validation;
pub mod testing;
pub mod format;
pub mod retry;
pub mod cli;
pub mod builder;
pub mod sampling;
pub mod diff;
pub mod pipeline;
pub mod expr;
pub mod trace;
pub mod permission;
pub mod cursor;
pub mod docs;
pub mod alert;
pub mod quota;
pub mod template;
pub mod hook;
pub mod transform;
pub mod namespace;
pub mod export;
pub mod import;
pub mod changelog;
pub mod policy;
pub mod aggregate;
pub mod sequence;
pub mod constraint;
pub mod comment;
pub mod label;
pub mod dependency;
pub mod privilege;
pub mod variable;
pub mod monitor;
pub mod catalog;
pub mod tablespace;
pub mod vacuum;
pub mod analyze;
pub mod prepared;
pub mod explain;
pub mod routine;
pub mod rule;
pub mod domain;
pub mod collation;
pub mod operator;
pub mod cast;
pub mod view;
pub mod matview;
pub mod publication;
pub mod subscription;
pub mod fdw;
pub mod slot;
pub mod wal;
pub mod checkpoint;
pub mod recovery;
pub mod buffer;
pub mod storage;
pub mod heap;
pub mod toast;
pub mod fsm;
pub mod vm;
pub mod xact;
pub mod clog;
pub mod multixact;
pub mod smgr;
pub mod relcache;
pub mod syscache;
pub mod catcache;
pub mod planner;
pub mod executor;
pub mod parser;
pub mod rewrite;
pub mod portal;
pub mod tcop;
pub mod bootstrap;
pub mod postmaster;








































// Removed global_allocator (moved to bin)

// Core modules
// ... (modules remain)

// ...

#[derive(Parser)]
#[command(name = "miracledb")]
#[command(about = "MiracleDb - The Ultimate Database", long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Commands>,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Start the database server (default)
    Start {
        /// Port to listen on
        #[arg(short, long, default_value = "8080")]
        port: u16,
    },
    /// Start the interactive CLI client
    Cli {
        /// Host to connect to
        #[arg(long, default_value = "localhost:8080")]
        host: String,
    },
    /// Initialize a new configuration file
    Init {
        /// Output path for config file
        #[arg(short, long, default_value = "miracledb.toml")]
        output: String,
    },
    /// Check server status
    Status {
        /// Host to connect to
        #[arg(short, long, default_value = "localhost:8080")]
        host: String,
    },
    /// Administrative commands
    Admin {
        #[command(subcommand)]
        command: AdminCommands,
    },
    /// Authentication commands
    Auth {
        #[command(subcommand)]
        command: AuthCommands,
    },
    /// Generate a JWT token for API authentication
    Token {
        /// Username/subject for the token
        #[arg(short, long, default_value = "admin")]
        user: String,
        /// Comma-separated roles (e.g., "admin,user")
        #[arg(short, long, default_value = "admin")]
        roles: String,
        /// Token expiry in days
        #[arg(short, long, default_value = "365")]
        expiry_days: u64,
    },
    /// ML model management commands
    #[cfg(feature = "nlp")]
    Ml {
        #[command(subcommand)]
        command: MlCommands,
    },
}

#[derive(Subcommand)]
pub enum AdminCommands {
    /// Create a new user
    CreateUser {
        /// Username
        username: String,
    },
}

#[derive(Subcommand)]
pub enum AuthCommands {
    /// Login to the server
    Login {
        /// Username
        #[arg(short, long)]
        username: String,
        /// Password
        #[arg(short, long)]
        password: String,
    },
}

#[cfg(feature = "nlp")]
#[derive(Subcommand)]
pub enum MlCommands {
    /// Download a model from HuggingFace
    Download {
        /// HuggingFace model ID (e.g., "sentence-transformers/all-MiniLM-L6-v2")
        #[arg(long)]
        model_id: String,
        /// Local name for the model
        #[arg(long)]
        name: String,
        /// Cache directory (default: ./data/models)
        #[arg(long, default_value = "./data/models")]
        cache_dir: String,
    },
    /// List all loaded models
    List,
    /// Get information about a model
    Info {
        /// Model name
        name: String,
    },
    /// Unload a model
    Unload {
        /// Model name
        name: String,
    },
    /// Test embedding generation
    Test {
        /// Model name
        #[arg(long)]
        model: String,
        /// Text to embed
        #[arg(long)]
        text: String,
    },
}

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    // Initialize Logging/Tracing
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .expect("setting default subscriber failed");

    match cli.command {
        Some(Commands::Cli { host }) => {
            cli::run_cli(host).await?;
        }
        Some(Commands::Start { port }) => {
            start_server(port).await?;
        }
        Some(Commands::Init { output }) => {
            cli::run_init(output).await?;
        }
        Some(Commands::Status { host }) => {
            cli::run_status(host).await?;
        }
        Some(Commands::Admin { command }) => {
            cli::run_admin(command).await?;
        }
        Some(Commands::Auth { command }) => {
            cli::run_auth(command).await?;
        }
        Some(Commands::Token { user, roles, expiry_days }) => {
            cli::generate_token(&user, &roles, expiry_days);
        }
        #[cfg(feature = "nlp")]
        Some(Commands::Ml { command }) => {
            cli::run_ml(command).await?;
        }
        None => {
            // Default to starting server on 8080 if no command provided
            start_server(8080).await?;
        }
    }

    Ok(())
}

/// Initialize test tables for demonstration if no tables exist
async fn initialize_test_tables(engine: &engine::MiracleEngine) -> Result<(), Box<dyn std::error::Error>> {
    // Check if tables already exist
    let existing_tables = engine.list_tables().await;
    if !existing_tables.is_empty() {
        info!("Tables already exist, skipping test data initialization");
        return Ok(());
    }

    info!("No tables found, creating test tables for demonstration...");

    // Create users table
    engine.query("CREATE TABLE users (id INTEGER, name VARCHAR(100), email VARCHAR(100), age INTEGER)").await?;
    info!("Created 'users' table");

    // Create products table
    engine.query("CREATE TABLE products (id INTEGER, name VARCHAR(200), description VARCHAR(500), price DOUBLE, stock INTEGER, category VARCHAR(50))").await?;
    info!("Created 'products' table");

    // Create orders table
    engine.query("CREATE TABLE orders (id INTEGER, user_id INTEGER, product_id INTEGER, quantity INTEGER, total_price DOUBLE, status VARCHAR(20))").await?;
    info!("Created 'orders' table");

    // Insert sample data - users
    engine.query("INSERT INTO users (id, name, email, age) VALUES (1, 'Alice Johnson', 'alice@example.com', 28)").await?;
    engine.query("INSERT INTO users (id, name, email, age) VALUES (2, 'Bob Smith', 'bob@example.com', 34)").await?;
    engine.query("INSERT INTO users (id, name, email, age) VALUES (3, 'Carol Williams', 'carol@example.com', 42)").await?;

    // Insert sample data - products
    engine.query("INSERT INTO products (id, name, description, price, stock, category) VALUES (1, 'Laptop Pro', 'High-performance laptop', 1299.99, 50, 'Electronics')").await?;
    engine.query("INSERT INTO products (id, name, description, price, stock, category) VALUES (2, 'Wireless Mouse', 'Ergonomic wireless mouse', 29.99, 200, 'Accessories')").await?;
    engine.query("INSERT INTO products (id, name, description, price, stock, category) VALUES (3, 'USB-C Cable', '6ft USB-C charging cable', 15.99, 500, 'Accessories')").await?;

    // Insert sample data - orders
    engine.query("INSERT INTO orders (id, user_id, product_id, quantity, total_price, status) VALUES (1, 1, 1, 1, 1299.99, 'completed')").await?;
    engine.query("INSERT INTO orders (id, user_id, product_id, quantity, total_price, status) VALUES (2, 2, 2, 2, 59.98, 'shipped')").await?;
    engine.query("INSERT INTO orders (id, user_id, product_id, quantity, total_price, status) VALUES (3, 1, 3, 3, 47.97, 'pending')").await?;

    info!("Test tables and data created successfully (3 tables, 9 total rows)");

    Ok(())
}

async fn start_server(port: u16) -> Result<(), Box<dyn std::error::Error>> {
    info!("Starting MiracleDb - The Ultimate Database...");
    info!("Features: SQL + Document + Vector + Graph + TimeSeries + Realtime");

    // 2. Initialize Core Engine
    let engine = engine::MiracleEngine::new().await?;
    info!("Core Engine initialized.");

    // 2.5 Initialize test tables for demo (if none exist)
    initialize_test_tables(&engine).await?;

    // 3. Initialize Security Manager
    let security_mgr = security::SecurityManager::new();
    security_mgr.rbac.init_default_roles().await;
    info!("Security Manager initialized with default roles.");

    // 4. Initialize Advanced Modules
    let nucleus_system = std::sync::Arc::new(nucleus::NucleusSystem::new());
    let graph_db = std::sync::Arc::new(graph::GraphDb::new());
    let rlm_agent = std::sync::Arc::new(rlm::RecursiveAgent::new(nucleus_system.clone(), graph_db.clone()));
    
    // Initialize Log Engine for dashboard integration
    let log_engine: Option<std::sync::Arc<logs::LogEngine>> = match logs::LogEngine::new("./data/logs") {
        Ok(engine) => {
            info!("Log Engine initialized.");
            Some(std::sync::Arc::new(engine))
        }
        Err(e) => {
            info!("Log Engine not initialized (optional): {}", e);
            None
        }
    };
    
    info!("Advanced modules initialized (Nucleus + Graph + RLM).");

    // 4.5 Initialize Backup & Recovery System
    let backup_dir = PathBuf::from("./backups");
    let data_dir = "./data";

    let backup_manager = std::sync::Arc::new(backup::BackupManager::new(
        backup_dir.to_str().unwrap()
    ));
    let recovery_manager = std::sync::Arc::new(recovery::RecoveryManager::new());
    let backup_scheduler = std::sync::Arc::new(backup::scheduler::BackupScheduler::new(
        backup_manager.clone(),
        data_dir.to_string(),
    ));

    // Start backup scheduler in background
    let scheduler_clone = backup_scheduler.clone();
    tokio::spawn(async move {
        scheduler_clone.start().await;
    });

    let backup_api_state = api::backup::BackupApiState {
        backup_manager,
        recovery_manager,
        backup_scheduler,
        data_dir: data_dir.to_string(),
    };

    info!("Backup & Recovery System initialized.");

    // 4.6 Initialize Vector Index Manager
    let vector_manager = std::sync::Arc::new(vector::VectorIndexManager::new(data_dir));
    info!("Vector Index Manager initialized.");

    // 4.7 Initialize Full-Text Index Manager
    let fulltext_manager = std::sync::Arc::new(fulltext::FullTextIndexManager::new("./data/fulltext"));
    info!("Full-Text Index Manager initialized.");

    // 4.8 Initialize Notification Manager
    let (notification_manager, _notification_rx) = notification::NotificationManager::new();
    let notification_manager = std::sync::Arc::new(notification_manager);
    info!("Notification Manager initialized.");

    // Register Prometheus metrics
    if let Err(e) = observability::metrics::MetricsCollector::register_default_metrics() {
        info!("Metrics already registered: {}", e);
    }

    // 5. Initialize API Server
    let sse_manager = std::sync::Arc::new(api::sse::SSEManager::default());
    let app = api::router(
        std::sync::Arc::new(engine),
        sse_manager,
        nucleus_system,
        graph_db,
        rlm_agent,
        std::sync::Arc::new(security_mgr),
        log_engine,
        Some(backup_api_state),
        vector_manager,
        fulltext_manager,
        notification_manager,
    );
    
    // 5. Start Server with TLS (if configured)
    // Check if SSL keys exist, otherwise fall back to HTTP (or generate self-signed for dev)
    let cert_path = PathBuf::from("cert.pem");
    let key_path = PathBuf::from("key.pem");

    let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));
    info!("MiracleDb listening on {}", addr);
    
    info!("API Endpoints:");
    info!("  - REST: https://{}/api/v1/", addr);
    info!("  - GraphQL: https://{}/graphql", addr);
    info!("  - WebSocket CDC: wss://{}/ws/events", addr);
    info!("  - SSE: https://{}/events/stream", addr);
    info!("  - MCP: https://{}/mcp/", addr);
    info!("  - Health: https://{}/health", addr);
    info!("  - Metrics: https://{}/metrics", addr);

    if cert_path.exists() && key_path.exists() {
        info!("SSL/TLS Enabled using cert.pem and key.pem");
        let config = axum_server::tls_rustls::RustlsConfig::from_pem_file(
            cert_path,
            key_path,
        ).await?;

        axum_server::bind_rustls(addr, config)
            .serve(app.into_make_service())
            .await?;
    } else {
        info!("WARNING: SSL keys not found (cert.pem, key.pem). Starting in HTTP mode (INSECURE).");
        info!("To enable TLS, place cert.pem and key.pem in the working directory.");
        let listener = TcpListener::bind(&addr).await?;
        axum::serve(listener, app).await?;
    }

    Ok(())
}
