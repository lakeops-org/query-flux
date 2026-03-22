use thiserror::Error;

#[derive(Debug, Error)]
pub enum QueryFluxError {
    #[error("Engine error: {0}")]
    Engine(String),

    #[error("Translation error: {0}")]
    Translation(String),

    #[error("Routing error: {0}")]
    Routing(String),

    #[error("Catalog error: {0}")]
    Catalog(String),

    #[error("Persistence error: {0}")]
    Persistence(String),

    #[error("Config error: {0}")]
    Config(String),

    #[error("Query not found: {0}")]
    QueryNotFound(String),

    #[error("Cluster not found: {0}")]
    ClusterNotFound(String),

    #[error("No cluster group available: {0}")]
    NoClusterGroupAvailable(String),

    /// Returned by `dispatch_query` when the acquired cluster only supports Arrow (sync)
    /// execution. The caller should retry via `execute_to_sink` instead.
    #[error("Cluster {0} requires Arrow execution path")]
    SyncEngineRequired(String),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, QueryFluxError>;
