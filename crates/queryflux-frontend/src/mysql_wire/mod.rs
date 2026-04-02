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
    tags::{parse_query_tags, QueryTags},
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
const CLIENT_SSL: u32 = 1 << 11;

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

    // Read client HandshakeResponse or SSLRequest.
    let (_, payload) = read_packet(&mut reader).await?;

    if is_ssl_request(&payload) {
        warn!(
            conn_id = connection_id,
            "MySQL wire: client requested TLS (SSLRequest); TLS is not supported — closing"
        );
        write_packet(
            &mut writer,
            1,
            &build_err(
                1105,
                "QueryFlux MySQL wire does not support TLS. Disable SSL on the client \
                 (e.g. mysql --ssl-mode=DISABLED, JDBC useSSL=false).",
            ),
        )
        .await?;
        return Ok(());
    }

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
        tags: QueryTags::new(),
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
                    user,
                    session_vars,
                    tags,
                    ..
                } = &session
                {
                    session = SessionContext::MySqlWire {
                        schema: if db.is_empty() { None } else { Some(db) },
                        user: user.clone(),
                        session_vars: session_vars.clone(),
                        tags: tags.clone(),
                    };
                }
                write_packet(&mut writer, seq.wrapping_add(1), &build_ok(0, 0)).await?;
            }

            COM_QUERY => {
                let sql = String::from_utf8_lossy(body)
                    .trim_end_matches('\0')
                    .to_string();
                debug!(conn_id = connection_id, sql = %sql, "MySQL wire: query");
                handle_com_query(&mut writer, &state, &mut session, &sql, seq.wrapping_add(1))
                    .await?;
            }

            COM_FIELD_LIST => {
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
    session: &mut SessionContext,
    sql: &str,
    start_seq: u8,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Unwrap MySQL conditional comments: /*!40101 SET ... */ → SET ...
    let logical = strip_mysql_conditional_comment(sql);
    let sql_lower = logical.trim().to_lowercase();

    // Fast-path: SET query_tags / SET SESSION query_tags — update session tags and ACK.
    if let Some(new_tags) = try_parse_set_query_tags(logical) {
        if let SessionContext::MySqlWire { tags, .. } = session {
            *tags = new_tags;
        }
        write_packet(writer, start_seq, &build_ok(0, 0)).await?;
        return Ok(());
    }

    // Fast-path: all other SET statements — acknowledge without dispatching.
    if sql_lower.starts_with("set ") || sql_lower.starts_with("set\t") {
        write_packet(writer, start_seq, &build_ok(0, 0)).await?;
        return Ok(());
    }

    // Fast-path: USE db sent as COM_QUERY text (mysql CLI does this).
    if let Some(db) = try_parse_use(&sql_lower) {
        if let SessionContext::MySqlWire {
            user,
            session_vars,
            tags,
            ..
        } = session
        {
            *session = SessionContext::MySqlWire {
                schema: if db.is_empty() { None } else { Some(db) },
                user: user.clone(),
                session_vars: session_vars.clone(),
                tags: tags.clone(),
            };
        }
        write_packet(writer, start_seq, &build_ok(0, 0)).await?;
        return Ok(());
    }

    // Fast-path: synthetic @@version / @@version_comment (single value, no comma).
    if sql_lower.contains("@@version") && !sql_lower.contains(',') {
        let col = if sql_lower.contains("version_comment") {
            "@@version_comment"
        } else {
            "@@version"
        };
        return write_string_result(writer, col, "8.0.0-queryflux", start_seq).await;
    }

    // Fast-path: SELECT DATABASE()
    if is_select_database(&sql_lower) {
        return write_optional_string_result(writer, "DATABASE()", session.database(), start_seq)
            .await;
    }

    // Fast-path: SHOW WARNINGS (empty result).
    if sql_lower.starts_with("show warnings") {
        return write_empty_show_warnings(writer, start_seq).await;
    }

    // Fast-path: SHOW VARIABLES / SHOW SESSION VARIABLES / SHOW GLOBAL VARIABLES
    // MySQL clients (JDBC, DBeaver, etc.) probe these at startup. Return empty set
    // rather than forwarding to the backend where they'd fail with a parse error.
    if sql_lower.starts_with("show variables")
        || sql_lower.starts_with("show session variables")
        || sql_lower.starts_with("show global variables")
        || sql_lower.starts_with("show status")
        || sql_lower.starts_with("show session status")
        || sql_lower.starts_with("show global status")
    {
        return write_empty_show_variables(writer, start_seq).await;
    }

    // Fast-path: SELECT with only MySQL metadata expressions (@@vars, VERSION(),
    // CURRENT_SCHEMA(), etc.) and no real FROM clause. Covers both the pure
    // @@-variable init queries and the mixed VERSION()/@@/CURRENT_SCHEMA() probe
    // that mysql-connector-j sends right after the comment-prefixed init query.
    if let Some(col_vals) = try_parse_mysql_metadata_select(&sql_lower, session) {
        return write_synthetic_multi_column_row(writer, &col_vals, start_seq).await;
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

// ── Synthetic result helpers ──────────────────────────────────────────────────

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

/// Single string column; value may be SQL NULL (e.g. `SELECT DATABASE()` with no schema).
async fn write_optional_string_result<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    col_name: &str,
    value: Option<&str>,
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
    match value {
        None => row.push(0xfb), // NULL marker
        Some(v) => write_lenenc_str(&mut row, v.as_bytes()),
    }
    write_packet(writer, seq, &row).await?;
    seq = seq.wrapping_add(1);

    write_packet(writer, seq, &build_eof()).await?;
    Ok(())
}

/// Empty `SHOW WARNINGS` result (three columns, zero rows).
async fn write_empty_show_warnings<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    start_seq: u8,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut seq = start_seq;

    let mut count = Vec::new();
    write_lenenc_int(&mut count, 3);
    write_packet(writer, seq, &count).await?;
    seq = seq.wrapping_add(1);

    for (name, ty) in [
        ("Level", MYSQL_TYPE_VAR_STRING),
        ("Code", MYSQL_TYPE_LONGLONG),
        ("Message", MYSQL_TYPE_VAR_STRING),
    ] {
        write_packet(writer, seq, &build_column_def_named(name, ty)).await?;
        seq = seq.wrapping_add(1);
    }

    write_packet(writer, seq, &build_eof()).await?;
    seq = seq.wrapping_add(1);

    // Zero rows — straight to the closing EOF.
    write_packet(writer, seq, &build_eof()).await?;
    Ok(())
}

/// Empty two-column result set for `SHOW VARIABLES` / `SHOW STATUS` and similar
/// MySQL-specific commands that the backend doesn't understand.
async fn write_empty_show_variables<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    start_seq: u8,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut seq = start_seq;

    let mut count = Vec::new();
    write_lenenc_int(&mut count, 2);
    write_packet(writer, seq, &count).await?;
    seq = seq.wrapping_add(1);

    for name in ["Variable_name", "Value"] {
        write_packet(
            writer,
            seq,
            &build_column_def_named(name, MYSQL_TYPE_VAR_STRING),
        )
        .await?;
        seq = seq.wrapping_add(1);
    }

    write_packet(writer, seq, &build_eof()).await?;
    seq = seq.wrapping_add(1);

    // Zero rows.
    write_packet(writer, seq, &build_eof()).await?;
    Ok(())
}

/// Multi-column single-row result for synthetic MySQL metadata selects.
/// Each entry is `(column_label, value)` where `None` = SQL NULL.
async fn write_synthetic_multi_column_row<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    col_vals: &[(String, Option<String>)],
    start_seq: u8,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut seq = start_seq;

    let mut count = Vec::new();
    write_lenenc_int(&mut count, col_vals.len() as u64);
    write_packet(writer, seq, &count).await?;
    seq = seq.wrapping_add(1);

    for (label, _) in col_vals {
        write_packet(
            writer,
            seq,
            &build_column_def_named(label, MYSQL_TYPE_VAR_STRING),
        )
        .await?;
        seq = seq.wrapping_add(1);
    }

    write_packet(writer, seq, &build_eof()).await?;
    seq = seq.wrapping_add(1);

    let mut row = Vec::new();
    for (_, val) in col_vals {
        match val {
            None => row.push(0xfb), // SQL NULL
            Some(s) => write_lenenc_str(&mut row, s.as_bytes()),
        }
    }
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

// ── SQL classification helpers ────────────────────────────────────────────────

/// Strip any number of leading block comments (`/* ... */` and `/*!NN... */`) from
/// the query and return the first non-comment token onwards.
///
/// MySQL connectors prepend metadata comments to every query, e.g.:
///   `/* mysql-connector-j-9.5.0 (...) */ SELECT @@session.auto_increment_increment ...`
/// Without stripping these the query starts with `/*` instead of `SELECT`, so all
/// fast-path checks miss it and it is forwarded to the backend engine unchanged.
///
/// For `/*!NNNNNbody*/` conditional-execution comments the body is extracted and
/// returned (the mysql CLI uses this form for SET init statements).
fn strip_mysql_conditional_comment(sql: &str) -> &str {
    let mut t = sql.trim();
    loop {
        if !t.starts_with("/*") {
            return t;
        }
        // `/*!NNNNNbody*/` — conditional execution: return the body itself.
        if let Some(inner) = t.strip_prefix("/*!") {
            if let Some((before_close, after_close)) = inner.split_once("*/") {
                let skip = before_close
                    .char_indices()
                    .find(|(_, c)| !c.is_ascii_digit())
                    .map(|(i, _)| i)
                    .unwrap_or(before_close.len());
                let body = before_close[skip..].trim();
                if !body.is_empty() {
                    return body;
                }
                // Empty body (e.g. `/*!*/`) — skip and keep going.
                t = after_close.trim();
                continue;
            }
        }
        // Plain `/* ... */` comment — skip it entirely.
        if let Some((_, after_close)) = t.split_once("*/") {
            t = after_close.trim();
        } else {
            // Unterminated comment — return as-is.
            return t;
        }
    }
}

/// Parse `SET query_tags = '...'` / `SET SESSION query_tags = '...'` (and `query_tag` spelling).
/// Returns the parsed `QueryTags` on match, `None` otherwise.
/// Case-insensitive; tolerant of extra whitespace and a trailing semicolon.
fn try_parse_set_query_tags(sql: &str) -> Option<QueryTags> {
    let s = sql.trim().trim_end_matches(';').trim();
    // Must start with SET (case-insensitive).
    let rest = s.strip_prefix_ci("SET")?;
    let rest = rest.trim_start();
    // Optionally skip SESSION keyword.
    let rest = if rest.to_ascii_lowercase().starts_with("session") {
        rest["SESSION".len()..].trim_start()
    } else {
        rest
    };
    // Match query_tags or query_tag key.
    let rest = if rest.to_ascii_lowercase().starts_with("query_tags") {
        &rest["query_tags".len()..]
    } else if rest.to_ascii_lowercase().starts_with("query_tag") {
        &rest["query_tag".len()..]
    } else {
        return None;
    };
    let rest = rest.trim_start().strip_prefix('=')?.trim_start();
    // Strip surrounding single or double quotes.
    let value = rest
        .strip_prefix('\'')
        .and_then(|s| s.strip_suffix('\''))
        .or_else(|| rest.strip_prefix('"').and_then(|s| s.strip_suffix('"')))
        .unwrap_or(rest);
    let (tags, _) = parse_query_tags(value);
    Some(tags)
}

trait StripPrefixCi {
    fn strip_prefix_ci(&self, prefix: &str) -> Option<&str>;
}
impl StripPrefixCi for str {
    fn strip_prefix_ci(&self, prefix: &str) -> Option<&str> {
        if self.len() >= prefix.len() && self[..prefix.len()].eq_ignore_ascii_case(prefix) {
            Some(&self[prefix.len()..])
        } else {
            None
        }
    }
}

/// Parse `USE db` / `USE \`db\`` sent as COM_QUERY text. Returns the database name.
fn try_parse_use(sql_lower: &str) -> Option<String> {
    let s = sql_lower.trim().trim_end_matches(';');
    let rest = if s == "use" {
        ""
    } else if let Some(r) = s.strip_prefix("use ") {
        r
    } else if let Some(r) = s.strip_prefix("use\t") {
        r
    } else {
        return None;
    };
    let rest = rest.trim();
    if rest.is_empty() {
        return Some(String::new());
    }
    Some(rest.trim_matches('`').to_string())
}

/// Detect `SELECT DATABASE()` (with optional whitespace variations).
fn is_select_database(sql_lower: &str) -> bool {
    let compact: String = sql_lower
        .trim()
        .trim_end_matches(';')
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect();
    compact.starts_with("selectdatabase()") && !compact.contains("from")
}

/// Strip a trailing `FROM dual` / `FROM DUAL` (with any whitespace) from a SELECT
/// body. MySQL/JDBC often appends `FROM dual` to pure variable selects.
fn strip_from_dual(select_body: &str) -> &str {
    // Work on the already-lowercased input. Strip trailing whitespace, then look
    // at the last two non-empty tokens. If they are `from` and `dual`, strip
    // them (and any preceding whitespace); otherwise, return the original body.
    let tail = select_body.trim_end();

    // Fast path: if there's nothing (or only whitespace), or fewer than two
    // tokens, we can't have a trailing `from dual`.
    let mut tokens: Vec<(&str, usize)> = Vec::new();
    for token in tail.split_whitespace() {
        // Compute the byte offset of this token within `tail`.
        let start = token.as_ptr() as usize - tail.as_ptr() as usize;
        tokens.push((token, start));
    }

    if tokens.len() >= 2 {
        let (last_tok, _) = tokens[tokens.len() - 1];
        let (prev_tok, prev_start) = tokens[tokens.len() - 2];

        // `select_body` is already lowercased by the caller, so we can do
        // direct string comparisons.
        if last_tok == "dual" && prev_tok == "from" {
            // Slice everything before the `from` token, then trim any
            // whitespace left at the end of that prefix.
            let before_from = &tail[..prev_start];
            let stripped = before_from.trim_end();
            return stripped;
        }
    }
    select_body
}

/// Default value for a MySQL `@@variable`. Strips `@@`, `@@session.`, `@@global.`
/// prefixes before matching. Returns a static string suitable for mysql-connector-j
/// to parse — numeric variables get digit strings so the Java JDBC driver doesn't
/// throw `NumberFormatException: For input string: ""`.
fn mysql_var_default(expr_lower: &str) -> &'static str {
    let name = expr_lower
        .trim_start_matches("@@")
        .trim_start_matches("session.")
        .trim_start_matches("global.");
    match name {
        "auto_increment_increment" | "auto_increment_offset" => "1",
        "character_set_client"
        | "character_set_connection"
        | "character_set_results"
        | "character_set_server" => "utf8mb4",
        "collation_server" | "collation_connection" => "utf8mb4_0900_ai_ci",
        "init_connect" => "",
        "interactive_timeout" | "wait_timeout" => "28800",
        "license" => "GPL",
        "lower_case_table_names" => "0",
        "max_allowed_packet" => "67108864",
        "net_buffer_length" => "16384",
        "net_write_timeout" => "60",
        "performance_schema" => "1",
        "query_cache_size" => "1048576",
        "query_cache_type" => "0",
        "sql_mode" => "STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION",
        "system_time_zone" => "UTC",
        "time_zone" => "SYSTEM",
        "transaction_isolation" | "tx_isolation" => "REPEATABLE-READ",
        // Boolean / integer variables that mysql-connector-j 9.x reads per-query
        // (e.g. before each statement to detect read-only mode). Returning "" causes
        // Java's parseInt("") → NumberFormatException: For input string: "".
        "transaction_read_only" | "read_only" | "innodb_read_only" => "0",
        "foreign_key_checks" | "unique_checks" => "1",
        "sql_auto_is_null" | "max_execution_time" | "session_track_state_change" => "0",
        "version" => "8.0.0-queryflux",
        "version_comment" => "QueryFlux",
        _ => "",
    }
}

/// If `expr_lower` (already lowercased, AS alias already stripped) is a MySQL-only
/// synthetic expression, return its value. `Some(Some(s))` = string, `Some(None)` = NULL.
/// Returns `None` if it's a real expression that must be dispatched to the backend.
fn mysql_synthetic_value(expr_lower: &str, session: &SessionContext) -> Option<Option<String>> {
    if expr_lower.starts_with("@@") {
        return Some(Some(mysql_var_default(expr_lower).to_string()));
    }
    match expr_lower {
        "version()" => Some(Some("8.0.0-queryflux".to_string())),
        "current_schema()" | "schema()" | "database()" => {
            Some(session.database().map(|s| s.to_string()))
        }
        "current_user()" | "user()" | "system_user()" | "session_user()" => {
            Some(Some(session.user().unwrap_or("").to_string()))
        }
        "connection_id()" => Some(Some("1".to_string())),
        _ => None,
    }
}

/// Detect a SELECT whose column list is composed entirely of MySQL-only metadata
/// expressions (`@@vars`, `VERSION()`, `CURRENT_SCHEMA()`, etc.) with no real FROM
/// clause. Returns `(column_label, value)` pairs on success, `None` if any column
/// requires actual dispatch.
///
/// Handles:
///   - Pure `@@var` lists (mysql-connector-j JDBC init query)
///   - Mixed `VERSION(), @@version_comment, CURRENT_SCHEMA()` probes (DataGrip)
///   - Optional trailing `FROM dual` or `LIMIT n`
fn try_parse_mysql_metadata_select(
    sql_lower: &str,
    session: &SessionContext,
) -> Option<Vec<(String, Option<String>)>> {
    let trimmed = sql_lower.trim().trim_end_matches(';');
    let rest = trimmed.strip_prefix("select")?.trim_start();
    let rest = strip_from_dual(rest);

    // Real FROM clause (not dual) — fall through to dispatch.
    if rest.split_whitespace().any(|w| w == "from") {
        return None;
    }

    let main = if let Some(i) = rest.find(" limit ") {
        &rest[..i]
    } else {
        rest
    };

    let parts: Vec<&str> = main.split(',').map(str::trim).collect();
    if parts.is_empty() {
        return None;
    }

    let mut col_vals = Vec::with_capacity(parts.len());
    for part in &parts {
        // Derive the column label (the AS alias, or the expression itself).
        let label = {
            if let Some(pos) = part.rfind(" as ") {
                part[pos + 4..].trim().to_string()
            } else {
                part.trim().to_string()
            }
        };
        // Base expression without alias.
        let expr = if let Some(pos) = part.rfind(" as ") {
            part[..pos].trim().to_lowercase()
        } else {
            part.trim().to_lowercase()
        };
        match mysql_synthetic_value(&expr, session) {
            Some(val) => col_vals.push((label, val)),
            None => return None, // unknown expression — needs real dispatch
        }
    }

    if col_vals.is_empty() {
        return None;
    }
    Some(col_vals)
}

// ── HandshakeResponse / SSLRequest parsing ────────────────────────────────────

/// SSLRequest is a 32-byte packet with CLIENT_SSL set but no username.
/// If we mistake it for a login and send OK, the client starts TLS while
/// we expect MySQL commands — both sides block indefinitely.
fn is_ssl_request(payload: &[u8]) -> bool {
    if payload.len() != 32 {
        return false;
    }
    let caps = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
    (caps & CLIENT_SSL) != 0
}

/// Extracts (username, optional database) from a MySQL 4.1+ HandshakeResponse packet.
fn parse_handshake_response(payload: &[u8]) -> (String, Option<String>) {
    // Layout: capabilities(4) + max_packet_size(4) + charset(1) + reserved(23) = 32 bytes
    if payload.len() < 32 {
        return (String::new(), None);
    }
    // Read the client's capability flags from the first 4 bytes of the response.
    let client_caps = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
    let mut pos = 32usize;

    // Null-terminated username.
    let user = read_nul_str(payload, &mut pos);

    // Auth response: 1-byte length prefix (CLIENT_SECURE_CONNECTION).
    if pos < payload.len() {
        let auth_len = payload[pos] as usize;
        pos += 1 + auth_len;
    }

    // Optional null-terminated database — only present if CLIENT_CONNECT_WITH_DB is set
    // by the client. Without this check, the auth-plugin name ("mysql_native_password")
    // that immediately follows would be misread as the schema.
    let database = if (client_caps & CLIENT_CONNECT_WITH_DB) != 0 && pos < payload.len() {
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
