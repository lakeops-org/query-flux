use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::tags::QueryTags;

/// Protocol-specific metadata that travels with a query from the frontend.
///
/// Used by:
/// - Routers: inspect headers, database name, user to pick a cluster group
/// - CatalogProvider: extract catalog/database hints for schema lookup
/// - Engine adapters: forward relevant auth/session info to the backend
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SessionContext {
    TrinoHttp {
        /// All HTTP headers from the client request (X-Trino-User, X-Trino-Source, etc.)
        headers: HashMap<String, String>,
        /// Query tags extracted from X-Trino-Client-Tags and X-Trino-Session at request time.
        tags: QueryTags,
    },
    PostgresWire {
        /// Database from the PG startup message.
        database: Option<String>,
        /// Authenticated user.
        user: Option<String>,
        /// Session parameters set via SET commands.
        session_params: HashMap<String, String>,
        /// Query tags extracted from startup params (query_tags / query_tag key).
        tags: QueryTags,
    },
    MySqlWire {
        /// Current schema (from USE statement or initial handshake).
        schema: Option<String>,
        /// Authenticated user.
        user: Option<String>,
        /// Session variables set via SET SESSION.
        session_vars: HashMap<String, String>,
        /// Query tags, updated when client issues SET SESSION query_tags / SET query_tags.
        tags: QueryTags,
    },
    ClickHouseHttp {
        /// HTTP headers from the client request.
        headers: HashMap<String, String>,
        /// URL query parameters (?database=x&user=y).
        query_params: HashMap<String, String>,
        /// Query tags extracted from X-QueryFlux-Tags header or query_tags param at request time.
        tags: QueryTags,
    },
    Mcp {
        /// Authenticated user extracted from the MCP request (via bearer token lookup).
        user: Option<String>,
        /// Stable agent identifier forwarded in the `X-Agent-Id` header.
        agent_id: Option<String>,
        /// Conversation or session identifier forwarded in the `X-Conversation-Id` header.
        conversation_id: Option<String>,
        /// Query tags (MCP clients may forward `X-QueryFlux-Tags`).
        tags: QueryTags,
    },
}

impl SessionContext {
    /// Return the pre-extracted query tags for this session.
    pub fn tags(&self) -> &QueryTags {
        match self {
            SessionContext::TrinoHttp { tags, .. } => tags,
            SessionContext::PostgresWire { tags, .. } => tags,
            SessionContext::MySqlWire { tags, .. } => tags,
            SessionContext::ClickHouseHttp { tags, .. } => tags,
            SessionContext::Mcp { tags, .. } => tags,
        }
    }

    /// Extract the user identity from any protocol variant.
    pub fn user(&self) -> Option<&str> {
        match self {
            SessionContext::TrinoHttp { headers, .. } => {
                headers.get("x-trino-user").map(|s| s.as_str())
            }
            SessionContext::PostgresWire { user, .. } => user.as_deref(),
            SessionContext::MySqlWire { user, .. } => user.as_deref(),
            SessionContext::ClickHouseHttp { query_params, .. } => {
                query_params.get("user").map(|s| s.as_str())
            }
            SessionContext::Mcp { user, .. } => user.as_deref(),
        }
    }

    /// Extract the target database/catalog hint from any protocol variant.
    pub fn database(&self) -> Option<&str> {
        match self {
            SessionContext::TrinoHttp { headers, .. } => {
                headers.get("x-trino-catalog").map(|s| s.as_str())
            }
            SessionContext::PostgresWire { database, .. } => database.as_deref(),
            SessionContext::MySqlWire { schema, .. } => schema.as_deref(),
            SessionContext::ClickHouseHttp { query_params, .. } => {
                query_params.get("database").map(|s| s.as_str())
            }
            SessionContext::Mcp { .. } => None,
        }
    }

    /// Extract the client source/application name (used for routing and metrics).
    pub fn client_source(&self) -> Option<&str> {
        match self {
            SessionContext::TrinoHttp { headers, .. } => {
                headers.get("x-trino-source").map(|s| s.as_str())
            }
            SessionContext::PostgresWire { session_params, .. } => {
                session_params.get("application_name").map(|s| s.as_str())
            }
            SessionContext::MySqlWire { session_vars, .. } => {
                session_vars.get("application_name").map(|s| s.as_str())
            }
            SessionContext::ClickHouseHttp { query_params, .. } => {
                query_params.get("client_name").map(|s| s.as_str())
            }
            SessionContext::Mcp { agent_id, .. } => agent_id.as_deref(),
        }
    }
}
