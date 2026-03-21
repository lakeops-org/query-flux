use std::collections::HashMap;

use async_trait::async_trait;
use queryflux_core::{
    error::Result,
    query::{ClusterGroupName, FrontendProtocol},
    session::SessionContext,
};

use crate::RouterTrait;

/// Routes based on Trino client tags (`X-Trino-Client-Tags` header).
/// If the client sends any tag that matches a rule, that group is selected.
/// First matching tag wins.
pub struct ClientTagsRouter {
    tag_to_group: HashMap<String, ClusterGroupName>,
}

impl ClientTagsRouter {
    pub fn new(tag_to_group: HashMap<String, ClusterGroupName>) -> Self {
        Self { tag_to_group }
    }
}

#[async_trait]
impl RouterTrait for ClientTagsRouter {
    fn type_name(&self) -> &'static str { "ClientTags" }

    async fn route(
        &self,
        _sql: &str,
        session: &SessionContext,
        _frontend_protocol: &FrontendProtocol,
    ) -> Result<Option<ClusterGroupName>> {
        if let SessionContext::TrinoHttp { headers } = session {
            // X-Trino-Client-Tags is a comma-separated list of tags.
            if let Some(tags_header) = headers.get("x-trino-client-tags") {
                for tag in tags_header.split(',').map(|t| t.trim()) {
                    if let Some(group) = self.tag_to_group.get(tag) {
                        return Ok(Some(group.clone()));
                    }
                }
            }
        }
        Ok(None)
    }
}
