use tokio::net::{TcpListener, TcpStream};
use std::sync::Arc;

// ─── pgwire-based handler (Task 2 + Task 3) ──────────────────────────────────

use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use pgwire::api::ClientInfo;
use pgwire::api::portal::Portal;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DataRowEncoder, DescribePortalResponse, DescribeResponse, DescribeStatementResponse,
    FieldFormat, FieldInfo, QueryResponse, Response, Tag,
};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::error::{PgWireError, PgWireResult};
use crate::engine::MiracleEngine;
use crate::protocol::type_map::arrow_to_pg_oid;

/// Convert a single Arrow array cell to its PostgreSQL text representation.
/// Returns None for null values.
pub fn arrow_col_to_text(col: &ArrayRef, row_idx: usize) -> Option<String> {
    if col.is_null(row_idx) {
        return None;
    }
    use arrow::array::*;
    let s = match col.data_type() {
        DataType::Boolean => col
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(row_idx)
            .to_string(),
        DataType::Int8 => col
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap()
            .value(row_idx)
            .to_string(),
        DataType::Int16 => col
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap()
            .value(row_idx)
            .to_string(),
        DataType::Int32 => col
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(row_idx)
            .to_string(),
        DataType::Int64 => col
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(row_idx)
            .to_string(),
        DataType::UInt8 => col
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap()
            .value(row_idx)
            .to_string(),
        DataType::UInt16 => col
            .as_any()
            .downcast_ref::<UInt16Array>()
            .unwrap()
            .value(row_idx)
            .to_string(),
        DataType::UInt32 => col
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap()
            .value(row_idx)
            .to_string(),
        DataType::UInt64 => col
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(row_idx)
            .to_string(),
        DataType::Float32 => col
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .value(row_idx)
            .to_string(),
        DataType::Float64 => col
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(row_idx)
            .to_string(),
        DataType::Utf8 => col
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(row_idx)
            .to_string(),
        DataType::LargeUtf8 => col
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap()
            .value(row_idx)
            .to_string(),
        DataType::Date32 | DataType::Date64 => {
            arrow::util::display::array_value_to_string(col, row_idx).unwrap_or_default()
        }
        DataType::Timestamp(_, _) => {
            arrow::util::display::array_value_to_string(col, row_idx).unwrap_or_default()
        }
        _ => arrow::util::display::array_value_to_string(col, row_idx).unwrap_or_default(),
    };
    Some(s)
}

/// Build pgwire FieldInfo descriptors from an Arrow schema.
fn schema_to_fields(schema: &arrow::datatypes::Schema) -> Vec<FieldInfo> {
    schema
        .fields()
        .iter()
        .map(|f| {
            let oid = arrow_to_pg_oid(f.data_type());
            let pg_type =
                pgwire::api::Type::from_oid(oid).unwrap_or(pgwire::api::Type::TEXT);
            FieldInfo::new(
                f.name().clone(),
                None,
                None,
                pg_type,
                FieldFormat::Text,
            )
        })
        .collect()
}

#[derive(Clone)]
pub struct MiracleDbHandler {
    engine: Arc<MiracleEngine>,
}

impl MiracleDbHandler {
    pub fn new(engine: Arc<MiracleEngine>) -> Self {
        Self { engine }
    }

    /// Shared execution logic used by both SimpleQueryHandler and ExtendedQueryHandler.
    async fn execute_sql(&self, sql: &str) -> PgWireResult<Response> {
        let trimmed = sql.trim_end_matches(';').trim();

        let df = self
            .engine
            .query(trimmed)
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        let batches = df
            .collect()
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

        if batches.is_empty() {
            let verb = trimmed
                .split_whitespace()
                .next()
                .unwrap_or("OK")
                .to_uppercase();
            return Ok(Response::Execution(Tag::new(&verb)));
        }

        let schema = batches[0].schema();
        let fields = Arc::new(schema_to_fields(&schema));

        if batches.iter().all(|b| b.num_rows() == 0) {
            let row_stream = futures::stream::iter(vec![]);
            return Ok(Response::Query(QueryResponse::new(fields, row_stream)));
        }

        let mut rows: Vec<PgWireResult<pgwire::messages::data::DataRow>> = Vec::new();
        for batch in &batches {
            for row_idx in 0..batch.num_rows() {
                let mut encoder = DataRowEncoder::new(fields.clone());
                for col_idx in 0..batch.num_columns() {
                    let text = arrow_col_to_text(batch.column(col_idx), row_idx);
                    encoder
                        .encode_field(&text.as_deref())
                        .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
                }
                rows.push(Ok(encoder.take_row()));
            }
        }

        let row_stream = futures::stream::iter(rows);
        Ok(Response::Query(QueryResponse::new(fields, row_stream)))
    }
}

#[async_trait::async_trait]
impl SimpleQueryHandler for MiracleDbHandler {
    async fn do_query<C>(
        &self,
        _client: &mut C,
        query: &str,
    ) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo
            + pgwire::api::ClientPortalStore
            + futures::Sink<pgwire::messages::PgWireBackendMessage>
            + Unpin
            + Send
            + Sync,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as futures::Sink<pgwire::messages::PgWireBackendMessage>>::Error>,
    {
        Ok(vec![self.execute_sql(query).await?])
    }
}

// ─── Task 3: substitute_params + ExtendedQueryHandler ────────────────────────

/// Replace `$1`, `$2`, … placeholders with bound parameter values.
///
/// NULL parameters (None) become the SQL keyword `NULL`.
/// Numeric-looking values (parseable as f64) are substituted unquoted;
/// everything else is single-quoted with interior `'` escaped as `''`.
/// Replacements are applied in **reverse index order** and use a capturing
/// regex that matches `$N` only when not immediately followed by another
/// digit, preventing `$1` from corrupting `$10`, `$11`, etc.
pub fn substitute_params(sql: &str, params: &[Option<String>]) -> String {
    let mut result = sql.to_string();
    for (i, value) in params.iter().enumerate().rev() {
        // Match `$N` followed by a non-digit or end of string.
        // Capture the trailing non-digit character (group 1) so we can
        // re-emit it in the replacement.
        let placeholder = format!(r"\${}(\D|$)", i + 1);
        let re = regex::Regex::new(&placeholder).unwrap();
        let replacement = match value {
            None => "NULL".to_string(),
            Some(v) if {
                let lower = v.trim().to_lowercase();
                lower != "inf" && lower != "-inf" && lower != "nan"
                && lower != "infinity" && lower != "-infinity"
                && v.parse::<f64>().is_ok()
            } => v.clone(),
            Some(v) => format!("'{}'", v.replace('\'', "''")),
        };
        // Use a closure so the replacement string is never interpreted for
        // backreferences — prevents `$` in param values (e.g., "$100") from
        // being mangled by the regex engine.
        result = re.replace_all(&result, |caps: &regex::Captures| {
            let suffix = caps.get(1).map_or("", |m| m.as_str());
            format!("{}{}", replacement, suffix)
        }).to_string();
    }
    result
}

#[async_trait::async_trait]
impl ExtendedQueryHandler for MiracleDbHandler {
    /// Use the raw SQL string as the stored statement — `NoopQueryParser`
    /// just echoes the SQL back, which is all we need.
    type Statement = String;
    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        Arc::new(NoopQueryParser::new())
    }

    /// Execute the portal: extract parameters from the raw bytes, substitute
    /// them into the SQL (preserving NULL as SQL NULL), then run via the engine.
    async fn do_query<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo
            + pgwire::api::ClientPortalStore
            + futures::Sink<pgwire::messages::PgWireBackendMessage>
            + Unpin
            + Send
            + Sync,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as futures::Sink<pgwire::messages::PgWireBackendMessage>>::Error>,
    {
        let raw_sql = portal.statement.statement.as_str();
        let params: Vec<Option<String>> = portal
            .parameters
            .iter()
            .map(|opt| {
                opt.as_ref()
                    .map(|b| String::from_utf8_lossy(b).into_owned())
                // None stays None — will become SQL NULL
            })
            .collect();
        let sql = substitute_params(raw_sql, &params);
        self.execute_sql(&sql).await
    }

    /// Describe a prepared statement: we cannot resolve types without executing,
    /// so return no-data (psql and JDBC still function correctly without this).
    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        _stmt: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        Ok(<DescribeStatementResponse as DescribeResponse>::no_data())
    }

    /// Describe a portal: same reasoning — return no-data.
    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        _portal: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        Ok(<DescribePortalResponse as DescribeResponse>::no_data())
    }
}

// ─── end Task 3 ──────────────────────────────────────────────────────────────

// ─── Task 4: PgWireServerHandlers wrapper ────────────────────────────────────

/// A pgwire `PgWireServerHandlers` implementation that routes startup through
/// the built-in `NoopHandler` (accept all connections, no password check) and
/// dispatches queries to `MiracleDbHandler`.
struct MiracleHandlers {
    query: Arc<MiracleDbHandler>,
}

impl MiracleHandlers {
    fn new(engine: Arc<MiracleEngine>) -> Self {
        Self {
            query: Arc::new(MiracleDbHandler::new(engine)),
        }
    }
}

impl pgwire::api::PgWireServerHandlers for MiracleHandlers {
    fn startup_handler(&self) -> Arc<impl pgwire::api::auth::StartupHandler> {
        Arc::new(pgwire::api::NoopHandler)
    }

    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.query.clone()
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        self.query.clone()
    }
}

// ─── end Task 4 helpers ──────────────────────────────────────────────────────

/// PostgreSQL wire protocol server powered by pgwire 0.38.
pub struct PgServer {
    addr: String,
    engine: Arc<MiracleEngine>,
}

impl PgServer {
    pub fn new(addr: &str, engine: Arc<MiracleEngine>) -> Self {
        Self { addr: addr.to_string(), engine }
    }

    pub async fn start(&self) -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(&self.addr).await?;
        println!("PostgreSQL server (pgwire) listening on {}", self.addr);

        loop {
            let (socket, _) = listener.accept().await?;
            let handlers = MiracleHandlers::new(self.engine.clone());
            tokio::spawn(async move {
                let result = pgwire::tokio::process_socket(
                    socket,
                    None, // no TLS — Task 5 adds this
                    handlers,
                )
                .await;
                if let Err(e) = result {
                    eprintln!("PG connection error: {e}");
                }
            });
        }
    }
}

#[cfg(test)]
mod handler_tests {
    use super::*;
    use arrow::array::{Float64Array, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    #[test]
    fn test_encode_int32_as_text() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let arr = Arc::new(Int32Array::from(vec![42]));
        let batch = RecordBatch::try_new(schema, vec![arr]).unwrap();
        let text = arrow_col_to_text(batch.column(0), 0);
        assert_eq!(text, Some("42".to_string()));
    }

    #[test]
    fn test_encode_null_as_none() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
        let arr = Arc::new(Int32Array::from(vec![None::<i32>]));
        let batch = RecordBatch::try_new(schema, vec![arr]).unwrap();
        let text = arrow_col_to_text(batch.column(0), 0);
        assert_eq!(text, None);
    }

    #[test]
    fn test_encode_float64_as_text() {
        let schema =
            Arc::new(Schema::new(vec![Field::new("price", DataType::Float64, false)]));
        let arr = Arc::new(Float64Array::from(vec![3.14]));
        let batch = RecordBatch::try_new(schema, vec![arr]).unwrap();
        let text = arrow_col_to_text(batch.column(0), 0);
        assert!(text.is_some());
        assert!(text.unwrap().contains("3.14"));
    }

    #[test]
    fn test_encode_utf8_as_text() {
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
        let arr = Arc::new(StringArray::from(vec!["hello"]));
        let batch = RecordBatch::try_new(schema, vec![arr]).unwrap();
        let text = arrow_col_to_text(batch.column(0), 0);
        assert_eq!(text, Some("hello".to_string()));
    }

    // ── substitute_params tests ──────────────────────────────────────────────

    #[test]
    fn test_substitute_no_params() {
        let sql = "SELECT * FROM users";
        let params: Vec<Option<String>> = vec![];
        assert_eq!(substitute_params(sql, &params), "SELECT * FROM users");
    }

    #[test]
    fn test_substitute_single_param_integer() {
        let sql = "SELECT * FROM users WHERE id = $1";
        let params = vec![Some("42".to_string())];
        assert_eq!(
            substitute_params(sql, &params),
            "SELECT * FROM users WHERE id = 42"
        );
    }

    #[test]
    fn test_substitute_multiple_params() {
        let sql = "SELECT * FROM orders WHERE user_id = $1 AND status = $2";
        let params = vec![Some("1".to_string()), Some("completed".to_string())];
        assert_eq!(
            substitute_params(sql, &params),
            "SELECT * FROM orders WHERE user_id = 1 AND status = 'completed'"
        );
    }

    #[test]
    fn test_substitute_param_out_of_range_leaves_placeholder() {
        let sql = "SELECT $1, $2";
        let params = vec![Some("hello".to_string())];
        let result = substitute_params(sql, &params);
        assert!(result.contains("$2"), "unresolved placeholder should remain");
    }

    #[test]
    fn test_substitute_with_quoted_string() {
        let sql = "SELECT * FROM products WHERE name = $1";
        let params = vec![Some("Laptop Pro".to_string())];
        assert_eq!(
            substitute_params(sql, &params),
            "SELECT * FROM products WHERE name = 'Laptop Pro'"
        );
    }

    #[test]
    fn test_substitute_escapes_single_quote() {
        let sql = "SELECT $1";
        let params = vec![Some("it's".to_string())];
        let result = substitute_params(sql, &params);
        assert_eq!(result, "SELECT 'it''s'");
    }

    #[test]
    fn test_substitute_no_corruption_of_higher_numbered_params() {
        let sql = "SELECT $1, $10";
        let params: Vec<Option<String>> = (1..=10).map(|i| Some(i.to_string())).collect();
        let result = substitute_params(sql, &params);
        assert_eq!(result, "SELECT 1, 10");
    }

    #[test]
    fn test_null_param_becomes_sql_null() {
        let sql = "SELECT * FROM users WHERE name = $1";
        let params = vec![None];
        let result = substitute_params(sql, &params);
        assert_eq!(result, "SELECT * FROM users WHERE name = NULL");
    }

    #[test]
    fn test_substitute_param_value_containing_dollar() {
        let sql = "SELECT * FROM prices WHERE amount = $1";
        let params = vec![Some("$100".to_string())];
        let result = substitute_params(sql, &params);
        assert_eq!(result, "SELECT * FROM prices WHERE amount = '$100'");
    }

    #[test]
    fn test_substitute_inf_is_quoted_not_numeric() {
        let sql = "SELECT $1";
        let params = vec![Some("inf".to_string())];
        let result = substitute_params(sql, &params);
        assert_eq!(result, "SELECT 'inf'");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    async fn start_test_server() -> String {
        let engine = Arc::new(crate::engine::MiracleEngine::new().await.unwrap());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            if let Ok((socket, _)) = listener.accept().await {
                let handlers = MiracleHandlers::new(engine);
                let _ = pgwire::tokio::process_socket(
                    socket,
                    None,
                    handlers,
                )
                .await;
            }
        });
        addr
    }

    /// Send a PostgreSQL 3.0 startup message.
    fn build_startup_msg() -> Vec<u8> {
        let user_param = b"user\0test\0\0";
        let len = (4u32 + 4 + user_param.len() as u32).to_be_bytes();
        let protocol = 196608u32.to_be_bytes(); // 3.0
        let mut msg = Vec::new();
        msg.extend_from_slice(&len);
        msg.extend_from_slice(&protocol);
        msg.extend_from_slice(user_param);
        msg
    }

    /// Drain the startup response from a pgwire server, accumulating bytes into
    /// a Vec until a `ReadyForQuery` (`Z`) message is seen. Returns all bytes
    /// received. Reads in chunks to handle the large ParameterStatus burst.
    async fn drain_startup(stream: &mut TcpStream) -> Vec<u8> {
        let mut all = Vec::new();
        let mut chunk = [0u8; 1024];
        loop {
            let n = stream.read(&mut chunk).await.unwrap();
            if n == 0 {
                break;
            }
            all.extend_from_slice(&chunk[..n]);
            // Stop once we have seen the ReadyForQuery 'Z' byte somewhere.
            // In practice pgwire sends it as the very last message of startup.
            if all.contains(&b'Z') {
                break;
            }
        }
        all
    }

    #[tokio::test]
    async fn test_startup_auth_ok() {
        let addr = start_test_server().await;
        let mut stream = TcpStream::connect(&addr).await.unwrap();

        // Send startup message
        stream.write_all(&build_startup_msg()).await.unwrap();

        // Read the first chunk — AuthOK ('R') must be the very first byte.
        let mut buf = [0u8; 64];
        let n = stream.read(&mut buf).await.unwrap();
        assert!(n >= 9, "expected at least AuthOK bytes, got {}", n);
        assert_eq!(buf[0], b'R', "expected AuthOK ('R'), got '{}'", buf[0] as char);
    }

    #[tokio::test]
    async fn test_simple_query_response() {
        let addr = start_test_server().await;
        let mut stream = TcpStream::connect(&addr).await.unwrap();

        // Startup — fully drain until ReadyForQuery so no leftover bytes.
        stream.write_all(&build_startup_msg()).await.unwrap();
        drain_startup(&mut stream).await;

        // Send Query: 'Q' + len(4) + "SELECT 1\0"
        let sql = b"SELECT 1\0";
        let qlen = (4u32 + sql.len() as u32).to_be_bytes();
        let mut qmsg = vec![b'Q'];
        qmsg.extend_from_slice(&qlen);
        qmsg.extend_from_slice(sql);
        stream.write_all(&qmsg).await.unwrap();

        // Response should start with 'T' (RowDescription), 'D' (DataRow), or 'C' (CommandComplete).
        let mut rbuf = [0u8; 512];
        let n = stream.read(&mut rbuf).await.unwrap();
        assert!(n > 0, "expected response to SELECT 1");
        assert!(
            rbuf[0] == b'T' || rbuf[0] == b'C' || rbuf[0] == b'D',
            "unexpected first byte: {} ({})", rbuf[0] as char, rbuf[0]
        );
    }

    #[tokio::test]
    async fn test_ready_for_query_after_startup() {
        let addr = start_test_server().await;
        let mut stream = TcpStream::connect(&addr).await.unwrap();
        stream.write_all(&build_startup_msg()).await.unwrap();

        // Drain the full startup response; it must contain a ReadyForQuery 'Z'.
        let all = drain_startup(&mut stream).await;
        assert!(all.contains(&b'Z'), "ReadyForQuery ('Z') not found in startup response");
    }

    #[tokio::test]
    async fn test_pgwire_server_startup_response() {
        // Create a minimal pgwire-based server using MiracleDbHandler via
        // MiracleHandlers, start it on an ephemeral port, connect via raw TCP,
        // and verify it sends AuthOK ('R') in the startup response.
        let engine = Arc::new(crate::engine::MiracleEngine::new().await.unwrap());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            if let Ok((socket, _)) = listener.accept().await {
                let handlers = MiracleHandlers::new(engine);
                let _ = pgwire::tokio::process_socket(
                    socket,
                    None,
                    handlers,
                )
                .await;
            }
        });

        // Connect and send a PostgreSQL startup message
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let user_param = b"user\0test\0\0";
        let len = (4u32 + 4 + user_param.len() as u32).to_be_bytes();
        let protocol = 196608u32.to_be_bytes(); // protocol version 3.0
        stream.write_all(&len).await.unwrap();
        stream.write_all(&protocol).await.unwrap();
        stream.write_all(user_param).await.unwrap();

        let mut buf = [0u8; 64];
        let n = stream.read(&mut buf).await.unwrap();
        assert!(n > 0, "expected response from pgwire server");
        assert_eq!(buf[0], b'R', "expected AuthOK ('R'), got '{}'", buf[0] as char);
    }
}
