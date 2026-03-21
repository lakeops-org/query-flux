use async_trait::async_trait;
use queryflux_core::{
    error::Result,
    query::{ClusterGroupName, FrontendProtocol},
    session::SessionContext,
};
use regex::Regex;

use crate::RouterTrait;

/// Routes based on regex patterns matched against the SQL text.
/// Rules are evaluated in order — first match wins.
pub struct QueryRegexRouter {
    rules: Vec<(Regex, ClusterGroupName)>,
}

impl QueryRegexRouter {
    /// Build from a list of (pattern, group) pairs. Skips rules with invalid regex
    /// and logs a warning so a bad pattern doesn't crash startup.
    pub fn new(rules: Vec<(String, String)>) -> Self {
        let compiled = rules
            .into_iter()
            .filter_map(|(pattern, group)| match Regex::new(&pattern) {
                Ok(re) => Some((re, ClusterGroupName(group))),
                Err(e) => {
                    tracing::warn!(
                        "QueryRegexRouter: skipping invalid regex {:?}: {}",
                        pattern,
                        e
                    );
                    None
                }
            })
            .collect();
        Self { rules: compiled }
    }
}

#[async_trait]
impl RouterTrait for QueryRegexRouter {
    fn type_name(&self) -> &'static str {
        "QueryRegex"
    }

    async fn route(
        &self,
        sql: &str,
        _session: &SessionContext,
        _frontend_protocol: &FrontendProtocol,
    ) -> Result<Option<ClusterGroupName>> {
        for (re, group) in &self.rules {
            if re.is_match(sql) {
                return Ok(Some(group.clone()));
            }
        }
        Ok(None)
    }
}
