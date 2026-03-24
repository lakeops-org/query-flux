//! MySQL wire protocol frontend.
//!
//! Accepts connections from any MySQL-compatible client (StarRocks clients,
//! DBeaver, mysql CLI, JDBC, etc.) and dispatches queries through the normal
//! QueryFlux routing/dispatch pipeline.
//!
//! All backends (DuckDB, StarRocks, Trino) are supported. Results are streamed
//! as Arrow RecordBatches and serialised to MySQL text protocol on the fly.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use arrow::array::Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::UnboundedSender;
use tracing::{debug, info, warn};

use queryflux_auth::Credentials;
use queryflux_core::{
    error::{QueryFluxError, Result},
    query::{FrontendProtocol, QueryStats},
    session::SessionContext,
};

use crate::dispatch::{execute_to_sink, ResultSink};
use crate::state::AppState;
use crate::FrontendListenerTrait;

// ── MySQL command bytes ───────────────────────────────────────────────────────

const COM_QUIT: u8 = 0x01;
const COM_INIT_DB: u8 = 0x02;
const COM_QUERY: u8 = 0x03;
const COM_FIELD_LIST: u8 = 0x04;
const COM_PING: u8 = 0x0e;

// ── MySQL column type bytes ───────────────────────────────────────────────────

const MYSQL_TYPE_DECIMAL: u8 = 0;
const MYSQL_TYPE_TINY: u8 = 1;
const MYSQL_TYPE_SHORT: u8 = 2;
const MYSQL_TYPE_LONG: u8 = 3;
const MYSQL_TYPE_FLOAT: u8 = 4;
const MYSQL_TYPE_DOUBLE: u8 = 5;
const MYSQL_TYPE_TIMESTAMP: u8 = 7;
const MYSQL_TYPE_LONGLONG: u8 = 8;
const MYSQL_TYPE_DATE: u8 = 10;
const MYSQL_TYPE_DATETIME: u8 = 12;
const MYSQL_TYPE_BLOB: u8 = 252;
const MYSQL_TYPE_VAR_STRING: u8 = 253;

// ── Capability flags (sent in the server handshake) ───────────────────────────

const CLIENT_LONG_PASSWORD: u32 = 1;
const CLIENT_FOUND_ROWS: u32 = 2;
const CLIENT_LONG_FLAG: u32 = 4;
const CLIENT_CONNECT_WITH_DB: u32 = 8;
const CLIENT_NO_SCHEMA: u32 = 16;
const CLIENT_PROTOCOL_41: u32 = 512;
const CLIENT_TRANSACTIONS: u32 = 8192;
const CLIENT_SECURE_CONNECTION: u32 = 32768;
const CLIENT_PLUGIN_AUTH: u32 = 1 << 19;

static CONNECTION_ID: AtomicU32 = AtomicU32::new(1);

// ── Frontend ──────────────────────────────────────────────────────────────────

pub struct MysqlWireFrontend {
    state: Arc<AppState>,
    port: u16,
}

impl MysqlWireFrontend {
    pub fn new(state: Arc<AppState>, port: u16) -> Self {
        Self { state, port }
    }
}

#[async_trait::async_trait]
impl FrontendListenerTrait for MysqlWireFrontend {
    async fn listen(&self) -> Result<()> {
        let addr = format!("0.0.0.0:{}", self.port);
        info!("MySQL wire frontend listening on {addr}");
        let listener = TcpListener::bind(&addr)
            .await
            .map_err(|e| QueryFluxError::Other(e.into()))?;

        loop {
            let (stream, peer) = listener
                .accept()
                .await
                .map_err(|e| QueryFluxError::Other(e.into()))?;
            debug!(peer = %peer, "MySQL wire: new connection");
            let state = self.state.clone();
            let conn_id = CONNECTION_ID.fetch_add(1, Ordering::Relaxed);
            tokio::spawn(async move {
                if let Err(e) = handle_connection(stream, state, conn_id).await {
                    debug!(conn_id, "MySQL wire connection closed: {e}");
                }
            });
        }
    }
}

// ── Connection handler ────────────────────────────────────────────────────────

async fn handle_connection(
    stream: TcpStream,
    state: Arc<AppState>,
    connection_id: u32,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    stream.set_nodelay(true)?;
    let (mut reader, mut writer) = tokio::io::split(stream);

    // Send server handshake.
    write_packet(&mut writer, 0, &build_handshake(connection_id)).await?;

    // Read client HandshakeResponse.
    let (_, payload) = read_packet(&mut reader).await?;
    let (user, schema) = parse_handshake_response(&payload);

    // Accept any credentials — QueryFlux trusts the network here.
    write_packet(&mut writer, 2, &build_ok(0, 0)).await?;

    info!(
        user,
        schema = ?schema,
        conn_id = connection_id,
        "MySQL wire: client authenticated"
    );

    let mut session = SessionContext::MySqlWire {
        user: if user.is_empty() { None } else { Some(user) },
        schema,
        session_vars: HashMap::new(),
    };

    // Command loop.
    loop {
        let (seq, payload) = match read_packet(&mut reader).await {
            Ok(p) => p,
            Err(_) => break, // client disconnected
        };
        if payload.is_empty() {
            break;
        }

        let cmd = payload[0];
        let body = &payload[1..];

        match cmd {
            COM_QUIT => break,

            COM_PING => {
                write_packet(&mut writer, seq.wrapping_add(1), &build_ok(0, 0)).await?;
            }

            COM_INIT_DB => {
                let db = String::from_utf8_lossy(body)
                    .trim_end_matches('\0')
                    .to_string();
                if let SessionContext::MySqlWire {
                    user, session_vars, ..
                } = &session
                {
                    session = SessionContext::MySqlWire {
                        schema: if db.is_empty() { None } else { Some(db) },
                        user: user.clone(),
                        session_vars: session_vars.clone(),
                    };
                }
                write_packet(&mut writer, seq.wrapping_add(1), &build_ok(0, 0)).await?;
            }

            COM_QUERY => {
                let sql = String::from_utf8_lossy(body)
                    .trim_end_matches('\0')
                    .to_string();
                debug!(conn_id = connection_id, sql = %sql, "MySQL wire: query");
                handle_com_query(&mut writer, &state, &session, &sql, seq.wrapping_add(1)).await?;
            }

            COM_FIELD_LIST => {
                // Used by some clients for tab-completion; respond with empty EOF.
                write_packet(&mut writer, seq.wrapping_add(1), &build_eof()).await?;
            }

            _ => {
                warn!(
                    conn_id = connection_id,
                    cmd, "MySQL wire: unsupported command"
                );
                write_packet(
                    &mut writer,
                    seq.wrapping_add(1),
                    &build_err(1047, &format!("Unsupported command: {cmd:#04x}")),
                )
                .await?;
            }
        }
    }

    debug!(conn_id = connection_id, "MySQL wire: connection closed");
    Ok(())
}

// ── Query execution ───────────────────────────────────────────────────────────

async fn handle_com_query<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    state: &Arc<AppState>,
    session: &SessionContext,
    sql: &str,
    start_seq: u8,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let sql_lower = sql.trim().to_lowercase();

    // Fast-path: SET statements — acknowledge without dispatching.
    if sql_lower.starts_with("set ") || sql_lower.starts_with("set\t") {
        write_packet(writer, start_seq, &build_ok(0, 0)).await?;
        return Ok(());
    }

    // Fast-path: synthetic @@version queries sent by MySQL drivers on connect.
    if sql_lower.contains("@@version") {
        return write_string_result(writer, "@@version", "8.0.0-queryflux", start_seq).await;
    }

    let protocol = FrontendProtocol::MySqlWire;

    // Authenticate — derive AuthContext from session (Phase 1: NoneAuthProvider).
    let creds = Credentials {
        username: session.user().map(|s| s.to_string()),
        ..Default::default()
    };
    let auth_ctx = match state.auth_provider.authenticate(&creds).await {
        Ok(ctx) => ctx,
        Err(e) => {
            write_packet(writer, start_seq, &build_err(1045, &e.to_string())).await?;
            return Ok(());
        }
    };

    let routing_result = {
        let live = state.live.read().await;
        live.router_chain
            .route_with_trace(sql, session, &protocol, Some(&auth_ctx))
            .await
    };
    let (group, _trace) = match routing_result {
        Ok(r) => r,
        Err(e) => {
            write_packet(writer, start_seq, &build_err(1105, &e.to_string())).await?;
            return Ok(());
        }
    };

    // Channel: sink encodes MySQL packets and sends them; we write them to TCP.
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
    let mut sink = MysqlResultSink::new(tx, start_seq);

    let state2 = state.clone();
    let session2 = session.clone();
    let sql2 = sql.to_string();

    let exec_task = tokio::spawn(async move {
        execute_to_sink(
            &state2, sql2, session2, protocol, group, &mut sink, &auth_ctx,
        )
        .await
        // `sink` drops here — closes tx — rx.recv() will return None after last packet
    });

    // Forward encoded MySQL packets to the client as they arrive.
    while let Some(pkt) = rx.recv().await {
        writer.write_all(&pkt).await?;
        writer.flush().await?;
    }

    // Log any panic in the execution task; result errors go through on_error → ERR packet.
    if let Err(e) = exec_task.await {
        warn!("MySQL query task panicked: {e}");
    }

    Ok(())
}

/// Write a single-column single-row string result set directly to the writer.
/// Used for synthetic responses (@@version, etc.) that bypass `execute_to_sink`.
async fn write_string_result<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    col_name: &str,
    value: &str,
    start_seq: u8,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut seq = start_seq;

    let mut count = Vec::new();
    write_lenenc_int(&mut count, 1);
    write_packet(writer, seq, &count).await?;
    seq = seq.wrapping_add(1);

    write_packet(
        writer,
        seq,
        &build_column_def_named(col_name, MYSQL_TYPE_VAR_STRING),
    )
    .await?;
    seq = seq.wrapping_add(1);

    write_packet(writer, seq, &build_eof()).await?;
    seq = seq.wrapping_add(1);

    let mut row = Vec::new();
    write_lenenc_str(&mut row, value.as_bytes());
    write_packet(writer, seq, &row).await?;
    seq = seq.wrapping_add(1);

    write_packet(writer, seq, &build_eof()).await?;
    Ok(())
}

// ── MysqlResultSink ───────────────────────────────────────────────────────────

/// Streams Arrow RecordBatches as MySQL text-protocol result packets over a channel.
///
/// Each `on_*` call encodes one or more MySQL packets (4-byte header + payload)
/// and sends them to an `UnboundedSender`. The COM_QUERY handler drains the
/// receiver and writes them to the TCP stream, preserving O(1 batch) memory.
struct MysqlResultSink {
    tx: UnboundedSender<Vec<u8>>,
    seq: u8,
}

impl MysqlResultSink {
    fn new(tx: UnboundedSender<Vec<u8>>, start_seq: u8) -> Self {
        Self { tx, seq: start_seq }
    }

    /// Prepend a 4-byte MySQL packet header and send to the channel.
    fn encode_and_send(&mut self, payload: Vec<u8>) {
        let seq = self.seq;
        self.seq = self.seq.wrapping_add(1);
        let len = payload.len();
        let mut pkt = Vec::with_capacity(4 + len);
        pkt.push((len & 0xff) as u8);
        pkt.push(((len >> 8) & 0xff) as u8);
        pkt.push(((len >> 16) & 0xff) as u8);
        pkt.push(seq);
        pkt.extend_from_slice(&payload);
        let _ = self.tx.send(pkt); // ignore SendError: receiver gone = client disconnected
    }
}

#[async_trait]
impl ResultSink for MysqlResultSink {
    async fn on_schema(&mut self, schema: &Schema) -> Result<()> {
        // Column count packet.
        let mut count_pkt = Vec::new();
        write_lenenc_int(&mut count_pkt, schema.fields().len() as u64);
        self.encode_and_send(count_pkt);

        // One ColumnDefinition41 packet per field.
        for field in schema.fields() {
            self.encode_and_send(build_column_def_from_field(field));
        }

        // EOF separating column defs from rows.
        self.encode_and_send(build_eof());
        Ok(())
    }

    async fn on_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        for row in 0..batch.num_rows() {
            let mut row_pkt = Vec::new();
            for col in batch.columns() {
                match arrow_value_to_mysql_bytes(col.as_ref(), row) {
                    None => row_pkt.push(0xfb), // NULL marker
                    Some(bytes) => write_lenenc_str(&mut row_pkt, &bytes),
                }
            }
            self.encode_and_send(row_pkt);
        }
        Ok(())
    }

    async fn on_complete(&mut self, _stats: &QueryStats) -> Result<()> {
        self.encode_and_send(build_eof());
        Ok(())
    }

    async fn on_error(&mut self, message: &str) -> Result<()> {
        self.encode_and_send(build_err(1105, message));
        Ok(())
    }
}

// ── Arrow type helpers ────────────────────────────────────────────────────────

fn arrow_type_to_mysql_type(dt: &DataType) -> u8 {
    match dt {
        DataType::Boolean | DataType::Int8 => MYSQL_TYPE_TINY,
        DataType::Int16 | DataType::UInt8 => MYSQL_TYPE_SHORT,
        DataType::Int32 | DataType::UInt16 => MYSQL_TYPE_LONG,
        DataType::Int64 | DataType::UInt32 | DataType::UInt64 => MYSQL_TYPE_LONGLONG,
        DataType::Float16 | DataType::Float32 => MYSQL_TYPE_FLOAT,
        DataType::Float64 => MYSQL_TYPE_DOUBLE,
        DataType::Decimal128(..) | DataType::Decimal256(..) => MYSQL_TYPE_DECIMAL,
        DataType::Date32 | DataType::Date64 => MYSQL_TYPE_DATE,
        DataType::Timestamp(..) => MYSQL_TYPE_DATETIME,
        DataType::Time32(..) | DataType::Time64(..) => MYSQL_TYPE_TIMESTAMP,
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => MYSQL_TYPE_BLOB,
        _ => MYSQL_TYPE_VAR_STRING, // Utf8, LargeUtf8, List, Map, Struct, Duration, ...
    }
}

/// Build a MySQL ColumnDefinition41 packet from an Arrow field.
fn build_column_def_from_field(field: &Field) -> Vec<u8> {
    let mut pkt = Vec::new();
    write_lenenc_str(&mut pkt, b"def"); // catalog
    write_lenenc_str(&mut pkt, b""); // schema
    write_lenenc_str(&mut pkt, b""); // table
    write_lenenc_str(&mut pkt, b""); // org_table
    write_lenenc_str(&mut pkt, field.name().as_bytes()); // name
    write_lenenc_str(&mut pkt, field.name().as_bytes()); // org_name
    pkt.push(0x0c); // fixed-length block = 12
    pkt.extend_from_slice(&0x21u16.to_le_bytes()); // charset: utf8mb4 (33)
    pkt.extend_from_slice(&0xffffu32.to_le_bytes()); // max column length
    pkt.push(arrow_type_to_mysql_type(field.data_type())); // type byte
    let flags: u16 = if field.is_nullable() { 0 } else { 0x0001 }; // NOT_NULL_FLAG
    pkt.extend_from_slice(&flags.to_le_bytes());
    pkt.push(0); // decimals
    pkt.extend_from_slice(&[0u8; 2]); // filler
    pkt
}

/// Build a ColumnDefinition41 packet by name and type byte (for synthetic results).
fn build_column_def_named(name: &str, type_byte: u8) -> Vec<u8> {
    let mut pkt = Vec::new();
    write_lenenc_str(&mut pkt, b"def");
    write_lenenc_str(&mut pkt, b"");
    write_lenenc_str(&mut pkt, b"");
    write_lenenc_str(&mut pkt, b"");
    write_lenenc_str(&mut pkt, name.as_bytes());
    write_lenenc_str(&mut pkt, name.as_bytes());
    pkt.push(0x0c);
    pkt.extend_from_slice(&0x21u16.to_le_bytes());
    pkt.extend_from_slice(&0xffffu32.to_le_bytes());
    pkt.push(type_byte);
    pkt.extend_from_slice(&0u16.to_le_bytes()); // nullable
    pkt.push(0);
    pkt.extend_from_slice(&[0u8; 2]);
    pkt
}

/// Serialize a single Arrow array cell as MySQL text protocol bytes.
/// Returns `None` for SQL NULL.
fn arrow_value_to_mysql_bytes(col: &dyn Array, row: usize) -> Option<Vec<u8>> {
    if col.is_null(row) {
        return None;
    }
    use arrow::util::display::{ArrayFormatter, FormatOptions};
    let s = ArrayFormatter::try_new(col, &FormatOptions::default())
        .map(|fmt| fmt.value(row).to_string())
        .unwrap_or_default();
    Some(s.into_bytes())
}

// ── Packet I/O ────────────────────────────────────────────────────────────────

async fn read_packet<R: AsyncReadExt + Unpin>(
    reader: &mut R,
) -> std::result::Result<(u8, Vec<u8>), Box<dyn std::error::Error + Send + Sync>> {
    let mut header = [0u8; 4];
    reader.read_exact(&mut header).await?;
    let len = u32::from_le_bytes([header[0], header[1], header[2], 0]) as usize;
    let seq = header[3];
    let mut payload = vec![0u8; len];
    if len > 0 {
        reader.read_exact(&mut payload).await?;
    }
    Ok((seq, payload))
}

async fn write_packet<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    seq: u8,
    payload: &[u8],
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let len = payload.len();
    let header = [
        (len & 0xff) as u8,
        ((len >> 8) & 0xff) as u8,
        ((len >> 16) & 0xff) as u8,
        seq,
    ];
    writer.write_all(&header).await?;
    writer.write_all(payload).await?;
    writer.flush().await?;
    Ok(())
}

// ── Packet builders ───────────────────────────────────────────────────────────

fn build_handshake(connection_id: u32) -> Vec<u8> {
    let caps: u32 = CLIENT_LONG_PASSWORD
        | CLIENT_FOUND_ROWS
        | CLIENT_LONG_FLAG
        | CLIENT_CONNECT_WITH_DB
        | CLIENT_NO_SCHEMA
        | CLIENT_PROTOCOL_41
        | CLIENT_TRANSACTIONS
        | CLIENT_SECURE_CONNECTION
        | CLIENT_PLUGIN_AUTH;

    let mut pkt = Vec::new();
    pkt.push(0x0a); // protocol version 10
    pkt.extend_from_slice(b"8.0.0-queryflux\0"); // server version + NUL
    pkt.extend_from_slice(&connection_id.to_le_bytes());
    pkt.extend_from_slice(b"12345678"); // auth-plugin-data part 1 (8 bytes)
    pkt.push(0x00); // filler
    pkt.extend_from_slice(&(caps as u16).to_le_bytes()); // capabilities low
    pkt.push(0x21); // charset: utf8mb4 (33)
    pkt.extend_from_slice(&0u16.to_le_bytes()); // status flags
    pkt.extend_from_slice(&((caps >> 16) as u16).to_le_bytes()); // capabilities high
    pkt.push(21); // auth_plugin_data length (8 + 13)
    pkt.extend_from_slice(&[0u8; 10]); // reserved
    pkt.extend_from_slice(b"123456789012\0"); // auth-plugin-data part 2 (12 bytes + NUL)
    pkt.extend_from_slice(b"mysql_native_password\0"); // plugin name + NUL
    pkt
}

fn build_ok(affected_rows: u64, last_insert_id: u64) -> Vec<u8> {
    let mut pkt = Vec::new();
    pkt.push(0x00); // OK marker
    write_lenenc_int(&mut pkt, affected_rows);
    write_lenenc_int(&mut pkt, last_insert_id);
    pkt.extend_from_slice(&0x0002u16.to_le_bytes()); // status: SERVER_STATUS_AUTOCOMMIT
    pkt.extend_from_slice(&0u16.to_le_bytes()); // warning count
    pkt
}

fn build_err(error_code: u16, message: &str) -> Vec<u8> {
    let mut pkt = Vec::new();
    pkt.push(0xff); // ERR marker
    pkt.extend_from_slice(&error_code.to_le_bytes());
    pkt.push(b'#'); // SQL state marker
    pkt.extend_from_slice(b"HY000"); // generic SQL state
                                     // Truncate to 512 bytes to keep packets reasonable.
    pkt.extend_from_slice(message.as_bytes().get(..512).unwrap_or(message.as_bytes()));
    pkt
}

fn build_eof() -> Vec<u8> {
    let mut pkt = Vec::new();
    pkt.push(0xfe); // EOF marker
    pkt.extend_from_slice(&0u16.to_le_bytes()); // warning count
    pkt.extend_from_slice(&0x0002u16.to_le_bytes()); // status: SERVER_STATUS_AUTOCOMMIT
    pkt
}

// ── Length-encoded integers and strings ──────────────────────────────────────

fn write_lenenc_int(buf: &mut Vec<u8>, n: u64) {
    if n < 251 {
        buf.push(n as u8);
    } else if n < 65_536 {
        buf.push(0xfc);
        buf.extend_from_slice(&(n as u16).to_le_bytes());
    } else if n < 16_777_216 {
        buf.push(0xfd);
        buf.push((n & 0xff) as u8);
        buf.push(((n >> 8) & 0xff) as u8);
        buf.push(((n >> 16) & 0xff) as u8);
    } else {
        buf.push(0xfe);
        buf.extend_from_slice(&n.to_le_bytes());
    }
}

fn write_lenenc_str(buf: &mut Vec<u8>, s: &[u8]) {
    write_lenenc_int(buf, s.len() as u64);
    buf.extend_from_slice(s);
}

// ── HandshakeResponse parsing ─────────────────────────────────────────────────

/// Extracts (username, optional database) from a MySQL 4.1+ HandshakeResponse packet.
fn parse_handshake_response(payload: &[u8]) -> (String, Option<String>) {
    // Layout: capabilities(4) + max_packet_size(4) + charset(1) + reserved(23) = 32 bytes
    if payload.len() < 32 {
        return (String::new(), None);
    }
    let mut pos = 32usize;

    // Null-terminated username.
    let user = read_nul_str(payload, &mut pos);

    // Auth response: 1-byte length prefix (CLIENT_SECURE_CONNECTION).
    if pos < payload.len() {
        let auth_len = payload[pos] as usize;
        pos += 1 + auth_len;
    }

    // Optional null-terminated database (CLIENT_CONNECT_WITH_DB).
    let database = if pos < payload.len() {
        let db = read_nul_str(payload, &mut pos);
        if db.is_empty() {
            None
        } else {
            Some(db)
        }
    } else {
        None
    };

    (user, database)
}

fn read_nul_str(payload: &[u8], pos: &mut usize) -> String {
    let start = *pos;
    while *pos < payload.len() && payload[*pos] != 0 {
        *pos += 1;
    }
    let s = String::from_utf8_lossy(&payload[start..*pos]).to_string();
    if *pos < payload.len() {
        *pos += 1; // consume NUL terminator
    }
    s
}
