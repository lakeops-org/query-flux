pub mod admin;
pub mod dispatch;
pub mod flight_sql;
pub mod mcp;
pub mod mysql_wire;
pub mod postgres_wire;
pub mod state;
pub mod trino_http;

use async_trait::async_trait;
use queryflux_core::error::Result;

/// Implemented by each frontend protocol server (Trino HTTP, PG wire, MySQL wire, etc.).
///
/// Each listener binds to a port, accepts connections in its native protocol,
/// translates requests into `IncomingQuery`, submits them to the `QueryDispatcher`,
/// and encodes results back into its native wire format.
#[async_trait]
pub trait FrontendListenerTrait: Send + Sync {
    /// Start the listener. Runs until the returned future is dropped or errors.
    async fn listen(&self) -> Result<()>;
}
