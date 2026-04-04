"use client";

import type { FC } from "react";
import { AthenaClusterConfig } from "./athena-cluster-config";
import { SnowflakeClusterConfig } from "./snowflake-cluster-config";
import { StarRocksClusterConfig } from "./starrocks-cluster-config";
import { TrinoClusterConfig } from "./trino-cluster-config";
import type { FlatClusterConfig, PatchClusterConfig } from "./types";

type ClusterFormProps = {
  flat: FlatClusterConfig;
  onPatch: PatchClusterConfig;
};

/**
 * Keys must match {@link StudioEngineModule.customFormId} in `lib/studio-engines/engines/*`.
 */
export const STUDIO_CUSTOM_CLUSTER_FORMS: Record<string, FC<ClusterFormProps>> = {
  trino: TrinoClusterConfig,
  starRocks: StarRocksClusterConfig,
  athena: AthenaClusterConfig,
  snowflake: SnowflakeClusterConfig,
};
