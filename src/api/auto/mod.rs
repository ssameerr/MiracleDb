//! Auto-Generated API Module
//!
//! Automatically generates REST APIs for database tables with:
//! - Full CRUD operations
//! - Multiple search types (text, vector, semantic, SQL, range, geo)
//! - Automatic Swagger documentation
//! - Field-level permissions
//! - Relationship support

pub mod registry;
pub mod crud;
pub mod search;
pub mod schema;

pub use registry::ApiRegistry;
pub use crud::CrudHandlers;
pub use search::{SearchRequest, SearchResponse, SearchType,
    BatchVectorSearchRequest, BatchVectorSearchResponse, SearchHit};
pub use schema::{TableSchema, ColumnInfo, IndexInfo};
