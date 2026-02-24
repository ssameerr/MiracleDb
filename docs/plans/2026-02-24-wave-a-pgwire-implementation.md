# Wave A: pgwire PostgreSQL Protocol Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace the 244-line manual PostgreSQL wire protocol in `src/protocol/postgres.rs` with pgwire 0.38, adding extended query protocol (Parse/Bind/Execute/Describe/Sync) and config-gated SSL/TLS.

**Architecture:** A single `MiracleDbHandler` struct holds `Arc<MiracleEngine>` and implements pgwire's `SimpleQueryHandler` and `ExtendedQueryHandler` traits. pgwire handles message framing, protocol state, auth, and TLS. We implement only business logic (SQL dispatch, type encoding, parameter substitution).

**Tech Stack:** `pgwire 0.38`, `dashmap 6.1` (already in Cargo.toml), `postgres-types` (re-exported by pgwire), Arrow column encoding via text format.

---

## Quick Reference

**Engine API:** `engine.query(sql: &str) -> Result<DataFrame>` → call `.collect().await?` for `Vec<RecordBatch>`

**Existing tests to preserve:** `test_startup_auth_ok`, `test_simple_query_response`, `test_ready_for_query_after_startup` (these test TCP-level protocol — they will pass with pgwire since pgwire implements the same wire protocol)

**`cargo test` baseline:** 344 tests must pass after every task.

---

### Task 1: Add pgwire dependency + Arrow→PostgreSQL type mapping

**Files:**
- Modify: `Cargo.toml`
- Create: `src/protocol/type_map.rs`
- Modify: `src/protocol/mod.rs`

**Step 1: Write the failing tests in a temporary location**

Create `src/protocol/type_map.rs` with tests only (no implementation yet):

```rust
// src/protocol/type_map.rs
use arrow::datatypes::DataType;

/// Map an Arrow DataType to the corresponding PostgreSQL OID (text format).
pub fn arrow_to_pg_oid(dt: &DataType) -> i32 {
    todo!()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;

    #[test]
    fn test_int32_maps_to_int4() {
        assert_eq!(arrow_to_pg_oid(&DataType::Int32), 23);
    }

    #[test]
    fn test_int8_maps_to_int4() {
        assert_eq!(arrow_to_pg_oid(&DataType::Int8), 23);
    }

    #[test]
    fn test_int64_maps_to_int8() {
        assert_eq!(arrow_to_pg_oid(&DataType::Int64), 20);
    }

    #[test]
    fn test_float32_maps_to_float4() {
        assert_eq!(arrow_to_pg_oid(&DataType::Float32), 700);
    }

    #[test]
    fn test_float64_maps_to_float8() {
        assert_eq!(arrow_to_pg_oid(&DataType::Float64), 701);
    }

    #[test]
    fn test_utf8_maps_to_text() {
        assert_eq!(arrow_to_pg_oid(&DataType::Utf8), 25);
    }

    #[test]
    fn test_bool_maps_to_bool() {
        assert_eq!(arrow_to_pg_oid(&DataType::Boolean), 16);
    }

    #[test]
    fn test_unknown_maps_to_text_fallback() {
        // List is not in the mapping — should fall back to TEXT
        assert_eq!(arrow_to_pg_oid(&DataType::List(
            std::sync::Arc::new(arrow::datatypes::Field::new("item", DataType::Int32, true))
        )), 25);
    }
}
```

**Step 2: Add `pub mod type_map;` to `src/protocol/mod.rs`**

`src/protocol/mod.rs` currently reads:
```rust
pub mod postgres;
```

Change it to:
```rust
pub mod postgres;
pub mod type_map;
```

**Step 3: Run tests to verify they fail with `todo!()`**

```bash
cargo test --lib protocol::type_map 2>&1 | head -30
```

Expected: tests compile but panic with `not yet implemented`.

**Step 4: Implement `arrow_to_pg_oid`**

Replace the `todo!()` body:

```rust
pub fn arrow_to_pg_oid(dt: &DataType) -> i32 {
    match dt {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::UInt8
        | DataType::UInt16 => 23,  // INT4
        DataType::Int64 | DataType::UInt32 | DataType::UInt64 => 20, // INT8
        DataType::Float32 => 700,  // FLOAT4
        DataType::Float64 => 701,  // FLOAT8
        DataType::Utf8 | DataType::LargeUtf8 => 25,  // TEXT
        DataType::Boolean => 16,   // BOOL
        DataType::Date32 => 1082,  // DATE
        DataType::Timestamp(_, _) => 1114, // TIMESTAMP
        DataType::Binary | DataType::LargeBinary => 17, // BYTEA
        _ => 25, // TEXT fallback (safe for everything else)
    }
}
```

**Step 5: Add pgwire to `Cargo.toml`**

In the `[dependencies]` section, after `postgres-protocol = "0.6"`:

```toml
pgwire = { version = "0.38", features = ["server-api-async"] }
```

**Step 6: Run tests**

```bash
cargo test --lib protocol::type_map 2>&1 | tail -15
```

Expected: `test result: ok. 8 passed; 0 failed`

**Step 7: Run full test suite to confirm nothing broke**

```bash
cargo test --lib 2>&1 | tail -5
```

Expected: 344+ tests, 0 failures.

**Step 8: Commit**

```bash
git add Cargo.toml src/protocol/type_map.rs src/protocol/mod.rs
git commit -m "feat(protocol): add pgwire dep and Arrow→PostgreSQL type mapping"
```

---

### Task 2: MiracleDbHandler with SimpleQueryHandler

**Files:**
- Modify: `src/protocol/postgres.rs` (add handler alongside existing code)

**Context:** pgwire 0.38 provides the `SimpleQueryHandler` trait. We implement it on `MiracleDbHandler`. The engine returns `DataFrame` from DataFusion; we collect it into `Vec<RecordBatch>`, convert to `FieldInfo` descriptors, then encode each row as text via `DataRowEncoder`.

The key pgwire types to import:
```rust
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::query::{SimpleQueryHandler, StatementOrPortal};
use pgwire::api::results::{DataRowEncoder, FieldInfo, FieldFormat, QueryResponse, Response, Tag};
use pgwire::api::{ClientInfo, PgWireServerHandlers, Type};
use pgwire::error::{PgWireError, PgWireResult};
```

> **Note:** Check the exact import paths after adding the dependency with `cargo doc --open` or looking at pgwire examples. The imports above reflect the typical pgwire 0.38 API surface but may need minor adjustment.

**Step 1: Write failing unit tests for SimpleQueryHandler**

Add a new `#[cfg(test)]` block at the bottom of `src/protocol/postgres.rs` (keep existing tests, add new block):

```rust
#[cfg(test)]
mod handler_tests {
    use super::*;

    /// Test that an Arrow Int32 column is encoded as a decimal string
    #[tokio::test]
    async fn test_encode_int32_as_text() {
        use arrow::array::Int32Array;
        use arrow::datatypes::{Field, Schema, DataType};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
        ]));
        let id_array = Arc::new(Int32Array::from(vec![42]));
        let batch = RecordBatch::try_new(schema, vec![id_array]).unwrap();

        // Encode value at row 0 of the "id" column
        let text = arrow_col_to_text(batch.column(0), 0);
        assert_eq!(text, Some("42".to_string()));
    }

    /// Test that a null value encodes as None
    #[tokio::test]
    async fn test_encode_null_as_none() {
        use arrow::array::Int32Array;
        use arrow::datatypes::{Field, Schema, DataType};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
        ]));
        let id_array = Arc::new(Int32Array::from(vec![None]));
        let batch = RecordBatch::try_new(schema, vec![id_array]).unwrap();

        let text = arrow_col_to_text(batch.column(0), 0);
        assert_eq!(text, None);
    }

    /// Test that a Float64 value encodes as decimal string
    #[tokio::test]
    async fn test_encode_float64_as_text() {
        use arrow::array::Float64Array;
        use arrow::datatypes::{Field, Schema, DataType};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![
            Field::new("price", DataType::Float64, false),
        ]));
        let arr = Arc::new(Float64Array::from(vec![3.14]));
        let batch = RecordBatch::try_new(schema, vec![arr]).unwrap();

        let text = arrow_col_to_text(batch.column(0), 0);
        assert!(text.is_some());
        assert!(text.unwrap().contains("3.14"));
    }

    /// Test that a Utf8 column encodes the string value
    #[tokio::test]
    async fn test_encode_utf8_as_text() {
        use arrow::array::StringArray;
        use arrow::datatypes::{Field, Schema, DataType};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
        ]));
        let arr = Arc::new(StringArray::from(vec!["hello"]));
        let batch = RecordBatch::try_new(schema, vec![arr]).unwrap();

        let text = arrow_col_to_text(batch.column(0), 0);
        assert_eq!(text, Some("hello".to_string()));
    }
}
```

**Step 2: Run tests to verify they fail**

```bash
cargo test --lib protocol::postgres::handler_tests 2>&1 | head -20
```

Expected: compile error — `arrow_col_to_text` not defined.

**Step 3: Implement `arrow_col_to_text` helper and `MiracleDbHandler`**

Add to `src/protocol/postgres.rs` (before the existing `PgServer` struct):

```rust
use std::sync::Arc;
use arrow::array::{Array, ArrayRef};
use arrow::datatypes::DataType;
use crate::engine::MiracleEngine;
use crate::protocol::type_map::arrow_to_pg_oid;

// pgwire imports — verify exact paths with `cargo doc` for pgwire 0.38
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::query::SimpleQueryHandler;
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::{ClientInfo, Type};
use pgwire::error::{PgWireError, PgWireResult};

/// Convert a single Arrow array cell at `row_idx` to its text representation.
/// Returns `None` for null values.
pub fn arrow_col_to_text(col: &ArrayRef, row_idx: usize) -> Option<String> {
    if col.is_null(row_idx) {
        return None;
    }
    use arrow::array::*;
    let s = match col.data_type() {
        DataType::Boolean => {
            let a = col.as_any().downcast_ref::<BooleanArray>().unwrap();
            a.value(row_idx).to_string()
        }
        DataType::Int8 => col.as_any().downcast_ref::<Int8Array>().unwrap().value(row_idx).to_string(),
        DataType::Int16 => col.as_any().downcast_ref::<Int16Array>().unwrap().value(row_idx).to_string(),
        DataType::Int32 => col.as_any().downcast_ref::<Int32Array>().unwrap().value(row_idx).to_string(),
        DataType::Int64 => col.as_any().downcast_ref::<Int64Array>().unwrap().value(row_idx).to_string(),
        DataType::UInt8 => col.as_any().downcast_ref::<UInt8Array>().unwrap().value(row_idx).to_string(),
        DataType::UInt16 => col.as_any().downcast_ref::<UInt16Array>().unwrap().value(row_idx).to_string(),
        DataType::UInt32 => col.as_any().downcast_ref::<UInt32Array>().unwrap().value(row_idx).to_string(),
        DataType::UInt64 => col.as_any().downcast_ref::<UInt64Array>().unwrap().value(row_idx).to_string(),
        DataType::Float32 => col.as_any().downcast_ref::<Float32Array>().unwrap().value(row_idx).to_string(),
        DataType::Float64 => col.as_any().downcast_ref::<Float64Array>().unwrap().value(row_idx).to_string(),
        DataType::Utf8 => col.as_any().downcast_ref::<StringArray>().unwrap().value(row_idx).to_string(),
        DataType::LargeUtf8 => col.as_any().downcast_ref::<LargeStringArray>().unwrap().value(row_idx).to_string(),
        DataType::Date32 => col.as_any().downcast_ref::<Date32Array>().unwrap().value(row_idx).to_string(),
        _ => {
            // Safe fallback: use Arrow's display formatting
            arrow::util::display::array_value_to_string(col, row_idx).unwrap_or_default()
        }
    };
    Some(s)
}

/// Build pgwire FieldInfo descriptors from an Arrow schema.
fn schema_to_fields(schema: &arrow::datatypes::Schema) -> Vec<FieldInfo> {
    schema.fields().iter().map(|f| {
        let oid = arrow_to_pg_oid(f.data_type()) as u32;
        // Type::from_oid may return None for unknown OIDs; fall back to TEXT
        let pg_type = Type::from_oid(oid).unwrap_or(Type::TEXT);
        FieldInfo::new(f.name().clone(), None, None, pg_type, FieldFormat::Text)
    }).collect()
}

/// Central handler: holds the engine and implements pgwire handler traits.
pub struct MiracleDbHandler {
    engine: Arc<MiracleEngine>,
}

impl MiracleDbHandler {
    pub fn new(engine: Arc<MiracleEngine>) -> Self {
        Self { engine }
    }
}

#[async_trait::async_trait]
impl SimpleQueryHandler for MiracleDbHandler {
    async fn do_query<'a, C>(&self, _client: &mut C, query: &'a str) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let sql = query.trim_end_matches(';').trim();

        // Execute SQL via DataFusion engine
        let df = self.engine.query(sql).await
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        let batches = df.collect().await
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

        if batches.is_empty() || batches[0].num_rows() == 0 {
            // Non-SELECT or empty result
            let tag = if sql.to_uppercase().starts_with("SELECT") {
                Tag::new("SELECT").with_rows(0)
            } else {
                Tag::new(sql.split_whitespace().next().unwrap_or("OK"))
            };
            return Ok(vec![Response::Execution(tag)]);
        }

        // Build schema descriptors
        let schema = batches[0].schema();
        let fields = Arc::new(schema_to_fields(&schema));

        // Encode all rows as owned DataRow values
        let mut rows: Vec<PgWireResult<pgwire::api::results::DataRow>> = Vec::new();
        let mut total_rows = 0usize;
        for batch in &batches {
            for row_idx in 0..batch.num_rows() {
                total_rows += 1;
                let mut encoder = DataRowEncoder::new(fields.clone());
                for col_idx in 0..batch.num_columns() {
                    let text = arrow_col_to_text(batch.column(col_idx), row_idx);
                    encoder.encode_field(&text.as_deref())
                        .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
                }
                rows.push(encoder.finish());
            }
        }

        let row_stream = futures::stream::iter(rows);
        let query_response = QueryResponse::new(fields, row_stream);

        Ok(vec![Response::Query(query_response)])
    }
}
```

> **Note on `encoder.encode_field`:** The exact method signature may vary. In pgwire 0.38, encoding a `Option<&str>` in text format typically works via `encoder.encode_field(&opt_str_ref)`. If the compiler rejects this, try `encoder.encode_field_with_type_oid(value_bytes_opt, oid)` or check the `DataRowEncoder` docs. The goal is: `None` → SQL NULL, `Some(str)` → UTF-8 bytes.

**Step 4: Run handler tests**

```bash
cargo test --lib protocol::postgres::handler_tests 2>&1 | tail -15
```

Expected: 4 tests pass.

**Step 5: Run full test suite**

```bash
cargo test --lib 2>&1 | tail -5
```

Expected: 344+ tests, 0 failures.

**Step 6: Commit**

```bash
git add src/protocol/postgres.rs
git commit -m "feat(protocol): implement MiracleDbHandler SimpleQueryHandler with Arrow text encoding"
```

---

### Task 3: Parameter substitution + ExtendedQueryHandler

**Files:**
- Modify: `src/protocol/postgres.rs`

**Context:** Extended query protocol: client sends `Parse` (store prepared statement), `Bind` (bind parameters to portal), `Execute` (run portal), `Describe` (describe statement/portal without executing), `Sync` (flush). We store statements in a `DashMap`. Parameter substitution replaces `$1`, `$2`, … with literal string values before passing SQL to DataFusion.

The `dashmap` crate is already in `Cargo.toml`.

**Step 1: Write unit tests for parameter substitution**

Add to the `handler_tests` module:

```rust
    #[test]
    fn test_substitute_no_params() {
        let sql = "SELECT * FROM users";
        let params: Vec<String> = vec![];
        assert_eq!(substitute_params(sql, &params), "SELECT * FROM users");
    }

    #[test]
    fn test_substitute_single_param_integer() {
        let sql = "SELECT * FROM users WHERE id = $1";
        let params = vec!["42".to_string()];
        assert_eq!(
            substitute_params(sql, &params),
            "SELECT * FROM users WHERE id = 42"
        );
    }

    #[test]
    fn test_substitute_multiple_params() {
        let sql = "SELECT * FROM orders WHERE user_id = $1 AND status = $2";
        let params = vec!["1".to_string(), "completed".to_string()];
        // String params are quoted
        assert_eq!(
            substitute_params(sql, &params),
            "SELECT * FROM orders WHERE user_id = 1 AND status = 'completed'"
        );
    }

    #[test]
    fn test_substitute_param_out_of_range_leaves_placeholder() {
        // $2 with only one param → leave $2 as-is (DataFusion will error, not us)
        let sql = "SELECT $1, $2";
        let params = vec!["hello".to_string()];
        let result = substitute_params(sql, &params);
        assert!(result.contains("$2"), "unresolved placeholder should remain");
    }
```

**Step 2: Run to verify they fail**

```bash
cargo test --lib protocol::postgres::handler_tests::test_substitute 2>&1 | head -10
```

Expected: compile error — `substitute_params` not defined.

**Step 3: Implement `substitute_params`**

Add to `src/protocol/postgres.rs` (before `MiracleDbHandler`):

```rust
/// Replace `$1`, `$2`, … placeholders with bound parameter values.
/// Non-numeric-looking values are single-quoted; numeric values are unquoted.
pub fn substitute_params(sql: &str, params: &[String]) -> String {
    let mut result = sql.to_string();
    // Process in reverse so $10 is replaced before $1
    for (i, value) in params.iter().enumerate().rev() {
        let placeholder = format!("${}", i + 1);
        // Heuristic: if value parses as a number, use unquoted; else single-quote it
        let replacement = if value.parse::<f64>().is_ok() {
            value.clone()
        } else {
            format!("'{}'", value.replace('\'', "''"))  // escape single quotes
        };
        result = result.replace(&placeholder, &replacement);
    }
    result
}
```

**Step 4: Run substitution tests**

```bash
cargo test --lib protocol::postgres::handler_tests::test_substitute 2>&1 | tail -10
```

Expected: 4 substitution tests pass.

**Step 5: Implement `ExtendedQueryHandler`**

The extended query handler stores parsed statements and portals. In pgwire 0.38, you typically implement `ExtendedQueryHandler` by providing associated types for `Statement` and implementing `do_query` for portals.

Add to `src/protocol/postgres.rs`:

```rust
use dashmap::DashMap;
use pgwire::api::query::ExtendedQueryHandler;
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::portal::Portal;

/// A stored prepared statement (just the SQL string for now).
#[derive(Debug, Clone)]
pub struct MiracleStatement {
    pub sql: String,
}

#[async_trait::async_trait]
impl ExtendedQueryHandler for MiracleDbHandler {
    type Statement = MiracleStatement;
    type QueryExecutor = Self;

    fn query_executor(&self) -> &Self {
        self
    }
}

// If pgwire requires a QueryExecutor trait impl on Self, implement it:
// (Check pgwire 0.38 docs — the trait structure may combine these differently)
// The key method to implement is `do_query` for portals, which substitutes
// bound params and then delegates to the same logic as SimpleQueryHandler.
```

> **Important implementation note:** pgwire 0.38's `ExtendedQueryHandler` trait shape changed significantly across versions. After adding the dependency:
> 1. Run `cargo doc -p pgwire --open` to see the exact trait definitions
> 2. Check `pgwire/examples/` in the crate source for a working implementation
> 3. The key operations are: Parse (store SQL in DashMap), Bind (store portal with params), Execute (call `substitute_params` + `SimpleQueryHandler::do_query` logic), Describe (return schema metadata without executing), Sync (flush, no-op)
> 4. If the trait is complex, implement the simplest version that handles `Execute` correctly — Parse/Bind/Describe can return empty/default responses initially.

**Minimal working extended query handler (adapt to actual trait):**

The core logic for `Execute`:
```rust
async fn execute_portal(&self, portal: &Portal<MiracleStatement>) -> PgWireResult<Vec<Response<'_>>> {
    let sql = &portal.statement().statement.sql;
    let params: Vec<String> = portal.parameters().iter()
        .map(|p| String::from_utf8_lossy(p).to_string())
        .collect();
    let substituted = substitute_params(sql, &params);
    // Reuse SimpleQueryHandler logic by calling the engine directly
    self.execute_sql(&substituted).await
}
```

Extract common execution logic into `execute_sql`:
```rust
async fn execute_sql<'a>(&self, sql: &str) -> PgWireResult<Vec<Response<'a>>> {
    // Same as do_query body — extract the logic here and call from both handlers
}
```

**Step 6: Add describe tests**

Add to `handler_tests`:

```rust
    #[test]
    fn test_param_substitution_with_quoted_string() {
        let sql = "SELECT * FROM products WHERE name = $1";
        let params = vec!["Laptop Pro".to_string()];
        assert_eq!(
            substitute_params(sql, &params),
            "SELECT * FROM products WHERE name = 'Laptop Pro'"
        );
    }

    #[test]
    fn test_param_substitution_escapes_single_quote() {
        let sql = "SELECT $1";
        let params = vec!["it's".to_string()];
        let result = substitute_params(sql, &params);
        assert_eq!(result, "SELECT 'it''s'");
    }
```

**Step 7: Run all protocol tests**

```bash
cargo test --lib protocol:: 2>&1 | tail -15
```

Expected: all type_map tests + handler_tests pass.

**Step 8: Run full suite**

```bash
cargo test --lib 2>&1 | tail -5
```

Expected: 344+ tests, 0 failures.

**Step 9: Commit**

```bash
git add src/protocol/postgres.rs
git commit -m "feat(protocol): add ExtendedQueryHandler and parameter substitution"
```

---

### Task 4: Replace PgServer with pgwire process_socket server loop

**Files:**
- Modify: `src/protocol/postgres.rs`

**Context:** Replace the manual `PgServer::start()` / `handle_connection()` implementation with pgwire's `process_socket`. pgwire provides `pgwire::tokio::process_socket(socket, tls_acceptor, handler)` which runs the full protocol state machine. We keep `PgServer` as a public struct but change its internals.

**Step 1: Write the integration test (keeps existing tests passing)**

The 3 existing tests (`test_startup_auth_ok`, `test_simple_query_response`, `test_ready_for_query_after_startup`) connect via raw TCP and test the wire protocol. They should continue to pass with pgwire since pgwire implements the same protocol. Keep them as-is.

Add a new test that exercises the pgwire-based server:

```rust
    #[tokio::test]
    async fn test_pgwire_server_starts_and_accepts() {
        // Start a pgwire-based server on an ephemeral port
        let engine = Arc::new(crate::engine::MiracleEngine::new().await.unwrap());
        let handler = Arc::new(MiracleDbHandler::new(engine));

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let handler_clone = handler.clone();
        tokio::spawn(async move {
            if let Ok((socket, _)) = listener.accept().await {
                let _ = pgwire::tokio::process_socket(
                    socket,
                    None, // no TLS
                    Arc::new(NoopStartupHandler),
                    handler_clone.clone(),
                    handler_clone,
                ).await;
            }
        });

        // Connect and send a startup message
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let user_param = b"user\0test\0\0";
        let len = (4u32 + 4 + user_param.len() as u32).to_be_bytes();
        let protocol = 196608u32.to_be_bytes(); // 3.0
        stream.write_all(&len).await.unwrap();
        stream.write_all(&protocol).await.unwrap();
        stream.write_all(user_param).await.unwrap();

        let mut buf = [0u8; 64];
        let n = stream.read(&mut buf).await.unwrap();
        assert!(n > 0, "server should respond to startup");
        assert_eq!(buf[0], b'R', "expected AuthOK ('R')");
    }
```

> **Note:** The exact `process_socket` call signature may differ. Check pgwire 0.38 docs. It might be:
> ```rust
> pgwire::tokio::process_socket(socket, tls_acceptor_opt, startup_handler, simple_handler, extended_handler)
> ```
> Or the handler might be a single struct implementing `PgWireServerHandlers` combining all three.

**Step 2: Replace `PgServer::start()` and remove `handle_connection`**

Rewrite `PgServer` to use pgwire:

```rust
/// PostgreSQL wire protocol server powered by pgwire.
pub struct PgServer {
    addr: String,
    engine: Arc<MiracleEngine>,
    tls_config: Option<std::sync::Arc<tokio_rustls::TlsAcceptor>>,
}

impl PgServer {
    pub fn new(addr: &str, engine: Arc<MiracleEngine>) -> Self {
        Self {
            addr: addr.to_string(),
            engine,
            tls_config: None,
        }
    }

    /// Optionally enable TLS. Call before `start()`.
    pub fn with_tls(mut self, acceptor: tokio_rustls::TlsAcceptor) -> Self {
        self.tls_config = Some(std::sync::Arc::new(acceptor));
        self
    }

    pub async fn start(&self) -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(&self.addr).await?;
        println!("PostgreSQL server (pgwire) listening on {}", self.addr);

        loop {
            let (socket, _peer_addr) = listener.accept().await?;
            let handler = Arc::new(MiracleDbHandler::new(self.engine.clone()));
            let tls = self.tls_config.clone();

            tokio::spawn(async move {
                let result = pgwire::tokio::process_socket(
                    socket,
                    tls.as_deref(),          // Option<&TlsAcceptor>
                    Arc::new(NoopStartupHandler),
                    handler.clone(),          // simple query handler
                    handler,                  // extended query handler
                ).await;
                if let Err(e) = result {
                    eprintln!("PG connection error: {e}");
                }
            });
        }
    }
}
```

> **Important:** Delete the old `handle_connection` function entirely. It is replaced by `process_socket`.

> **Existing 3 tests:** They call `handle_connection` directly. Update `start_test_server()` in those tests to use `process_socket` instead. The test structure stays the same (bind, spawn, connect via TCP), but the server side now uses pgwire.

**Step 3: Update `start_test_server` in the old tests**

```rust
    async fn start_test_server() -> String {
        let engine = Arc::new(crate::engine::MiracleEngine::new().await.unwrap());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            if let Ok((socket, _)) = listener.accept().await {
                let handler = Arc::new(MiracleDbHandler::new(engine));
                let _ = pgwire::tokio::process_socket(
                    socket, None,
                    Arc::new(NoopStartupHandler),
                    handler.clone(), handler,
                ).await;
            }
        });
        addr
    }
```

**Step 4: Compile and run all protocol tests**

```bash
cargo test --lib protocol:: 2>&1 | tail -20
```

Expected: all tests pass (original 3 + new tests).

**Step 5: Run full test suite**

```bash
cargo test --lib 2>&1 | tail -5
```

Expected: 344+ tests, 0 failures.

**Step 6: Commit**

```bash
git add src/protocol/postgres.rs
git commit -m "feat(protocol): replace manual wire protocol with pgwire process_socket"
```

---

### Task 5: SSL/TLS config-gated support

**Files:**
- Modify: `src/protocol/postgres.rs` (TLS acceptor construction)
- Modify: `miracledb.toml` (optional — add `[tls]` section documentation)

**Context:** TLS is optional. When `miracledb.toml` has `[tls]` with `cert_file` and `key_file`, the server wraps connections with `tokio-rustls`. When absent, plain TCP is used (no breaking change). pgwire handles SSLRequest negotiation automatically when a `TlsAcceptor` is provided.

**Step 1: Write a TLS config struct test**

Add to `handler_tests`:

```rust
    #[test]
    fn test_tls_config_none_when_no_cert_files() {
        // When cert and key paths don't exist, TLS should be None
        let result = build_tls_acceptor(None::<&str>, None::<&str>);
        assert!(result.is_none());
    }

    #[test]
    fn test_tls_config_none_for_nonexistent_paths() {
        let result = build_tls_acceptor(
            Some("/nonexistent/cert.pem"),
            Some("/nonexistent/key.pem"),
        );
        assert!(result.is_none());
    }
```

**Step 2: Implement `build_tls_acceptor`**

Add to `src/protocol/postgres.rs`:

```rust
/// Build a TLS acceptor from cert/key PEM files.
/// Returns `None` if paths are absent or files cannot be read.
pub fn build_tls_acceptor(
    cert_path: Option<impl AsRef<std::path::Path>>,
    key_path: Option<impl AsRef<std::path::Path>>,
) -> Option<tokio_rustls::TlsAcceptor> {
    let cert_path = cert_path?;
    let key_path = key_path?;

    if !cert_path.as_ref().exists() || !key_path.as_ref().exists() {
        return None;
    }

    // Load cert chain
    let cert_pem = std::fs::read(cert_path.as_ref()).ok()?;
    let key_pem = std::fs::read(key_path.as_ref()).ok()?;

    let certs = rustls_pemfile::certs(&mut cert_pem.as_slice())
        .collect::<Result<Vec<_>, _>>().ok()?;
    let private_key = rustls_pemfile::private_key(&mut key_pem.as_slice()).ok()??;

    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, private_key)
        .ok()?;

    Some(tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(config)))
}
```

Add `rustls-pemfile` to Cargo.toml if not already present:
```toml
rustls-pemfile = "2.1"
tokio-rustls = "0.26"
```

> **Note:** Check if `tokio-rustls` and `rustls-pemfile` are already pulled in transitively by existing deps (`axum-server` with `tls-rustls`). Run `cargo tree | grep rustls` to see. If they're already available, you may not need to add them explicitly.

**Step 3: Wire `build_tls_acceptor` into `PgServer`**

Update `start_server` in `src/lib.rs` (or wherever `PgServer` is constructed) to optionally pass TLS:

```rust
// In src/protocol/postgres.rs PgServer::start() or wherever the server is started:
// Read from config:
let tls_acceptor = build_tls_acceptor(
    config.tls.as_ref().map(|t| &t.cert_file),
    config.tls.as_ref().map(|t| &t.key_file),
);
let mut pg_server = PgServer::new(&pg_addr, engine.clone());
if let Some(acceptor) = tls_acceptor {
    pg_server = pg_server.with_tls(acceptor);
}
pg_server.start().await?;
```

> **Note:** The PostgreSQL server is not yet wired into `src/lib.rs::start_server()`. That is fine — the goal is that `PgServer::new(addr, engine).start()` works. The TLS API is ready to use once wired.

**Step 4: Run TLS tests**

```bash
cargo test --lib protocol::postgres::handler_tests::test_tls 2>&1 | tail -10
```

Expected: 2 TLS config tests pass.

**Step 5: Run full suite**

```bash
cargo test --lib 2>&1 | tail -5
```

Expected: 344+ tests, 0 failures.

**Step 6: Commit**

```bash
git add src/protocol/postgres.rs Cargo.toml
git commit -m "feat(protocol): add config-gated SSL/TLS support via build_tls_acceptor"
```

---

### Task 6: Final verification + ROADMAP update

**Files:**
- Modify: `ROADMAP.md` (PostgreSQL Wire Protocol: 60% → 90%)

**Step 1: Run the complete test suite**

```bash
cargo test --lib 2>&1 | tail -10
```

Expected output (must contain):
```
test result: ok. N passed; 0 failed; 0 ignored
```
Where N ≥ 344.

If any test fails, fix it before proceeding.

**Step 2: Verify pgwire handler compiles cleanly**

```bash
cargo check 2>&1 | grep -E "(error|warning: unused)" | head -20
```

Fix any `error` lines. Unused import warnings are acceptable but should be cleaned up.

**Step 3: Run protocol-specific tests**

```bash
cargo test --lib protocol:: -v 2>&1 | tail -30
```

Expected: all type_map tests, handler_tests, and socket tests pass.

**Step 4: Update ROADMAP.md**

Find the PostgreSQL Wire Protocol line in `ROADMAP.md` and change `60%` to `90%`. Also update the progress notes to reflect extended query protocol support.

If `ROADMAP.md` doesn't exist at the root, check for `ROADMAP_TO_100_PERCENT.md` or similar. Update the relevant entry:

```markdown
| PostgreSQL Wire Protocol | 90% | Extended query (P/B/E/D/S) via pgwire 0.38, SSL/TLS, psql compatible |
```

**Step 5: Final commit**

```bash
git add ROADMAP.md  # or ROADMAP_TO_100_PERCENT.md
git commit -m "docs: update PostgreSQL Wire Protocol from 60% to 90% after Wave A"
```

**Step 6: Final full test run**

```bash
cargo test --lib 2>&1 | tail -5
```

Expected: all tests pass, 0 failures.

---

## Summary

| Task | What it builds | Tests added |
|------|---------------|-------------|
| 1 | `type_map.rs` + pgwire dep | ~8 unit tests |
| 2 | `MiracleDbHandler` + `SimpleQueryHandler` | ~4 unit tests |
| 3 | `ExtendedQueryHandler` + param substitution | ~6 unit tests |
| 4 | Replace `PgServer` with pgwire `process_socket` | ~1 integration-style test |
| 5 | `build_tls_acceptor`, config-gated TLS | ~2 unit tests |
| 6 | ROADMAP update, final verification | — |

**Total new tests: ~21**
**End state: 365+ tests passing, PostgreSQL Wire Protocol at 90%**
