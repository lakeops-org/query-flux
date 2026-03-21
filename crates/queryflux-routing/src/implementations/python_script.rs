use async_trait::async_trait;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use queryflux_core::{
    error::{QueryFluxError, Result},
    query::{ClusterGroupName, FrontendProtocol},
    session::SessionContext,
};

use crate::RouterTrait;

/// Routes queries using a user-supplied Python function.
///
/// The script must define a function with this signature:
/// ```python
/// def route(sql: str, user: str | None, protocol: str) -> str | None:
///     # return a cluster group name, or None to pass to next router
///     return None
/// ```
pub struct PythonScriptRouter {
    script: String,
}

impl PythonScriptRouter {
    pub fn new(script: String) -> Self {
        Self { script }
    }

    /// Load from a file path instead of inline script.
    pub fn from_file(path: &str) -> Result<Self> {
        let script = std::fs::read_to_string(path).map_err(|e| {
            QueryFluxError::Routing(format!("Failed to read routing script {path}: {e}"))
        })?;
        Ok(Self::new(script))
    }
}

#[async_trait]
impl RouterTrait for PythonScriptRouter {
    fn type_name(&self) -> &'static str {
        "PythonScript"
    }

    async fn route(
        &self,
        sql: &str,
        session: &SessionContext,
        frontend_protocol: &FrontendProtocol,
    ) -> Result<Option<ClusterGroupName>> {
        let sql = sql.to_string();
        let script = self.script.clone();
        let user = session.user().map(|s| s.to_string());
        let protocol = format!("{frontend_protocol:?}");

        tokio::task::spawn_blocking(move || {
            call_python_router(&script, &sql, user.as_deref(), &protocol)
        })
        .await
        .map_err(|e| QueryFluxError::Routing(format!("spawn_blocking error: {e}")))?
    }
}

fn call_python_router(
    script: &str,
    sql: &str,
    user: Option<&str>,
    protocol: &str,
) -> Result<Option<ClusterGroupName>> {
    Python::attach(|py| {
        // Execute the script to define the `route` function.
        // PyO3 0.28 requires a CStr — convert via a temporary CString.
        let globals = PyDict::new(py);
        let cscript = std::ffi::CString::new(script)
            .map_err(|e| QueryFluxError::Routing(format!("Script contains null byte: {e}")))?;
        py.run(&cscript, Some(&globals), None)
            .map_err(|e| QueryFluxError::Routing(format!("Python routing script error: {e}")))?;

        let route_fn = globals
            .get_item("route")
            .map_err(|e| QueryFluxError::Routing(format!("Script has no 'route' function: {e}")))?
            .ok_or_else(|| QueryFluxError::Routing("Script has no 'route' function".to_string()))?;

        let result = route_fn
            .call1((sql, user, protocol))
            .map_err(|e| QueryFluxError::Routing(format!("route() call failed: {e}")))?;

        if result.is_none() {
            return Ok(None);
        }

        let group: String = result.extract().map_err(|e| {
            QueryFluxError::Routing(format!("route() must return str or None: {e}"))
        })?;

        Ok(Some(ClusterGroupName(group)))
    })
}
