use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use bytes::{BytesMut, BufMut};
use std::sync::Arc;

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
