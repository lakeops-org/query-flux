//! Single place to register backend adapters for the `queryflux` binary:
//! descriptors for validation / admin API and dispatch to per-adapter factories.

use std::sync::Arc;

use anyhow::{Context, Result};
use queryflux_core::config::{ClusterConfig, EngineConfig};
use queryflux_core::engine_registry::EngineDescriptor;
use queryflux_core::error::QueryFluxError;
use queryflux_core::query::{ClusterGroupName, ClusterName};
use queryflux_engine_adapters::athena::AthenaAdapter;
use queryflux_engine_adapters::duckdb::http::DuckDbHttpAdapter;
use queryflux_engine_adapters::duckdb::DuckDbAdapter;
use queryflux_engine_adapters::starrocks::StarRocksAdapter;
use queryflux_engine_adapters::trino::TrinoAdapter;
use queryflux_engine_adapters::EngineAdapterTrait;

/// All engine descriptors for [`queryflux_core::engine_registry::EngineRegistry`].
pub fn all_descriptors() -> Vec<EngineDescriptor> {
    vec![
        TrinoAdapter::descriptor(),
        DuckDbAdapter::descriptor(),
        DuckDbHttpAdapter::descriptor(),
        StarRocksAdapter::descriptor(),
        AthenaAdapter::descriptor(),
    ]
}

fn map_qf_err(e: QueryFluxError) -> anyhow::Error {
    anyhow::Error::new(e)
}

/// Build an adapter for `cluster_cfg`. `cluster_name_str` is used only in error context messages.
pub async fn build_adapter(
    cluster_name: ClusterName,
    placeholder_group: ClusterGroupName,
    cluster_cfg: &ClusterConfig,
    cluster_name_str: &str,
) -> Result<Arc<dyn EngineAdapterTrait>> {
    let engine = cluster_cfg
        .engine
        .as_ref()
        .context(format!("cluster '{cluster_name_str}' missing required 'engine' field"))?;

    let adapter: Arc<dyn EngineAdapterTrait> = match engine {
        EngineConfig::Trino => Arc::new(
            TrinoAdapter::try_from_cluster_config(
                cluster_name,
                placeholder_group,
                cluster_cfg,
                cluster_name_str,
            )
            .map_err(map_qf_err)?,
        ),
        EngineConfig::DuckDb => Arc::new(
            DuckDbAdapter::try_from_cluster_config(
                cluster_name,
                placeholder_group,
                cluster_cfg,
                cluster_name_str,
            )
            .map_err(map_qf_err)?,
        ),
        EngineConfig::DuckDbHttp => Arc::new(
            DuckDbHttpAdapter::try_from_cluster_config(
                cluster_name,
                placeholder_group,
                cluster_cfg,
                cluster_name_str,
            )
            .map_err(map_qf_err)?,
        ),
        EngineConfig::StarRocks => Arc::new(
            StarRocksAdapter::try_from_cluster_config(
                cluster_name,
                placeholder_group,
                cluster_cfg,
                cluster_name_str,
            )
            .map_err(map_qf_err)?,
        ),
        EngineConfig::Athena => Arc::new(
            AthenaAdapter::try_from_cluster_config(
                cluster_name,
                placeholder_group,
                cluster_cfg,
                cluster_name_str,
            )
            .await
            .map_err(map_qf_err)?,
        ),
        EngineConfig::ClickHouse => {
            anyhow::bail!("Engine ClickHouse not yet implemented")
        }
    };

    Ok(adapter)
}
