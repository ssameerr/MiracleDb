use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use bytes::{BytesMut, BufMut};
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
            Some(v) if v.parse::<f64>().is_ok() => v.clone(),
            Some(v) => format!("'{}'", v.replace('\'', "''")),
        };
        // $1 in the replacement string refers to the captured trailing character.
        let repl_with_suffix = format!("{}$1", replacement);
        result = re.replace_all(&result, repl_with_suffix.as_str()).to_string();
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

pub struct PgServer {
    addr: String,
}

impl PgServer {
    pub fn new(addr: &str) -> Self {
        Self { addr: addr.to_string() }
    }

    pub async fn start(&self) -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(&self.addr).await?;
        println!("Postgres Emulation Server listening on {}", self.addr);

        loop {
            let (socket, _) = listener.accept().await?;
            tokio::spawn(async move {
                if let Err(e) = handle_connection(socket).await {
                    eprintln!("PG Connection error: {}", e);
                }
            });
        }
    }
}

async fn handle_connection(mut socket: TcpStream) -> Result<(), Box<dyn std::error::Error>> {
    use bytes::BufMut;

    // --- Startup phase ---
    let mut len_buf = [0u8; 4];
    socket.read_exact(&mut len_buf).await?;
    let msg_len = u32::from_be_bytes(len_buf) as usize;
    let mut startup_buf = vec![0u8; msg_len.saturating_sub(4)];
    if !startup_buf.is_empty() {
        socket.read_exact(&mut startup_buf).await?;
    }

    // Send: AuthOK + ParameterStatus messages + BackendKeyData + ReadyForQuery
    let mut response = BytesMut::new();

    // AuthOK: 'R' | len=8 | int32=0
    response.put_u8(b'R');
    response.put_i32(8);
    response.put_i32(0);

    // ParameterStatus messages (inlined to avoid borrow checker issues with closure)
    let ps1_body = b"server_version\x0014.0 (MiracleDB)\x00";
    response.put_u8(b'S');
    response.put_i32((4 + ps1_body.len()) as i32);
    response.extend_from_slice(ps1_body);

    let ps2_body = b"client_encoding\x00UTF8\x00";
    response.put_u8(b'S');
    response.put_i32((4 + ps2_body.len()) as i32);
    response.extend_from_slice(ps2_body);

    let ps3_body = b"DateStyle\x00ISO, MDY\x00";
    response.put_u8(b'S');
    response.put_i32((4 + ps3_body.len()) as i32);
    response.extend_from_slice(ps3_body);

    // BackendKeyData: 'K' | len=12 | pid | secret
    response.put_u8(b'K');
    response.put_i32(12);
    response.put_i32(std::process::id() as i32);
    response.put_i32(12345);

    // ReadyForQuery: 'Z' | len=5 | 'I'
    response.put_u8(b'Z');
    response.put_i32(5);
    response.put_u8(b'I');

    socket.write_all(&response).await?;

    // --- Query loop ---
    let mut buf = BytesMut::with_capacity(4096);
    loop {
        buf.clear();
        let mut type_buf = [0u8; 1];
        if socket.read_exact(&mut type_buf).await.is_err() {
            break;
        }
        let msg_type = type_buf[0];

        let mut len_buf = [0u8; 4];
        if socket.read_exact(&mut len_buf).await.is_err() {
            break;
        }
        let body_len = (u32::from_be_bytes(len_buf) as usize).saturating_sub(4);
        let mut body = vec![0u8; body_len];
        if body_len > 0 && socket.read_exact(&mut body).await.is_err() {
            break;
        }

        match msg_type {
            b'Q' => {
                let _sql = String::from_utf8_lossy(&body).trim_end_matches('\0').to_string();
                let mut resp = BytesMut::new();

                // RowDescription: 'T' | len | field_count(2) | field_name\0 + 18 bytes
                let field_name = b"result\0";
                resp.put_u8(b'T');
                resp.put_i32((4 + 2 + field_name.len() + 18) as i32);
                resp.put_i16(1);
                resp.extend_from_slice(field_name);
                resp.put_i32(0);   // table OID
                resp.put_i16(0);   // column attr num
                resp.put_i32(25);  // type OID (text)
                resp.put_i16(-1);  // type size
                resp.put_i32(-1);  // type modifier
                resp.put_i16(0);   // format (text)

                // DataRow: 'D' | len | field_count(2) | field_len(4) + data
                let val = b"1";
                resp.put_u8(b'D');
                resp.put_i32((4 + 2 + 4 + val.len()) as i32);
                resp.put_i16(1);
                resp.put_i32(val.len() as i32);
                resp.extend_from_slice(val);

                // CommandComplete
                let tag = b"SELECT 1\0";
                resp.put_u8(b'C');
                resp.put_i32((4 + tag.len()) as i32);
                resp.extend_from_slice(tag);

                // ReadyForQuery
                resp.put_u8(b'Z');
                resp.put_i32(5);
                resp.put_u8(b'I');

                socket.write_all(&resp).await?;
            }
            b'X' => break, // Terminate
            _ => {
                let mut resp = BytesMut::new();
                let err = b"Sunknown message\0";
                resp.put_u8(b'E');
                resp.put_i32((4 + err.len()) as i32);
                resp.extend_from_slice(err);
                resp.put_u8(b'Z');
                resp.put_i32(5);
                resp.put_u8(b'I');
                socket.write_all(&resp).await?;
            }
        }
    }
    Ok(())
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    async fn start_test_server() -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            handle_connection(socket).await.ok();
        });
        addr
    }

    #[tokio::test]
    async fn test_startup_auth_ok() {
        let addr = start_test_server().await;
        let mut stream = TcpStream::connect(&addr).await.unwrap();

        // Send startup message: length(4) + protocol(4) + "user\0test\0\0"
        let user_param = b"user\0test\0\0";
        let len = (4 + 4 + user_param.len()) as u32;
        let mut msg = vec![];
        msg.extend_from_slice(&len.to_be_bytes());
        msg.extend_from_slice(&196608u32.to_be_bytes()); // protocol 3.0
        msg.extend_from_slice(user_param);
        stream.write_all(&msg).await.unwrap();

        // Read response — expect 'R' (AuthOK)
        let mut buf = [0u8; 64];
        let n = stream.read(&mut buf).await.unwrap();
        assert!(n >= 9, "expected at least AuthOK + ReadyForQuery, got {} bytes", n);
        assert_eq!(buf[0], b'R', "expected AuthOK ('R'), got '{}'", buf[0] as char);
    }

    #[tokio::test]
    async fn test_simple_query_response() {
        let addr = start_test_server().await;
        let mut stream = TcpStream::connect(&addr).await.unwrap();

        // Startup
        let user_param = b"user\0test\0\0";
        let len = (4 + 4 + user_param.len()) as u32;
        let mut msg = vec![];
        msg.extend_from_slice(&len.to_be_bytes());
        msg.extend_from_slice(&196608u32.to_be_bytes());
        msg.extend_from_slice(user_param);
        stream.write_all(&msg).await.unwrap();
        let mut buf = [0u8; 256];
        stream.read(&mut buf).await.unwrap();

        // Send Query: 'Q' + len(4) + "SELECT 1\0"
        let sql = b"SELECT 1\0";
        let qlen = (4 + sql.len()) as u32;
        let mut qmsg = vec![b'Q'];
        qmsg.extend_from_slice(&qlen.to_be_bytes());
        qmsg.extend_from_slice(sql);
        stream.write_all(&qmsg).await.unwrap();

        // Response should contain 'T', 'D', or 'C'
        let mut rbuf = [0u8; 256];
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
        let user_param = b"user\0test\0\0";
        let len = (4 + 4 + user_param.len()) as u32;
        let mut msg = vec![];
        msg.extend_from_slice(&len.to_be_bytes());
        msg.extend_from_slice(&196608u32.to_be_bytes());
        msg.extend_from_slice(user_param);
        stream.write_all(&msg).await.unwrap();

        let mut buf = [0u8; 128];
        let n = stream.read(&mut buf).await.unwrap();
        // Find 'Z' byte (ReadyForQuery) in response
        assert!(buf[..n].contains(&b'Z'), "ReadyForQuery ('Z') not found in startup response");
    }
}
