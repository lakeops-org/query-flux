use std::collections::HashMap;

use async_trait::async_trait;
use queryflux_core::{
    error::Result,
    query::{ClusterGroupName, FrontendProtocol},
    session::SessionContext,
};

use crate::RouterTrait;

/// Routes based on a specific HTTP header value.
/// Only applies to Trino HTTP frontend (other protocols don't have arbitrary headers).
pub struct HeaderRouter {
    header_name: String,
    /// header_value → cluster group name
    mapping: HashMap<String, ClusterGroupName>,
}

impl HeaderRouter {
    pub fn new(header_name: String, mapping: HashMap<String, ClusterGroupName>) -> Self {
        Self {
            header_name: header_name.to_lowercase(),
            mapping,
        }
    }
}

#[async_trait]
impl RouterTrait for HeaderRouter {
    fn type_name(&self) -> &'static str {
        "Header"
    }

    async fn route(
        &self,
        _sql: &str,
        session: &SessionContext,
        _frontend_protocol: &FrontendProtocol,
    ) -> Result<Option<ClusterGroupName>> {
        if let SessionContext::TrinoHttp { headers } = session {
            if let Some(value) = headers.get(&self.header_name) {
                return Ok(self.mapping.get(value).cloned());
            }
        }
        Ok(None)
    }
}
