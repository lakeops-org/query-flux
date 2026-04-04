---
name: Snowflake Backend Adapter
overview: Add a Snowflake backend engine adapter using the `snowflake-connector-rs` crate, following the same sync-execution pattern as the StarRocks adapter. This enables QueryFlux to route queries to real Snowflake warehouses as a backend engine.
todos:
  - id: core-types
    content: Add Snowflake variant to EngineConfig, EngineType, SqlDialect, and engine_registry helpers. Add account/warehouse/role/schema fields to ClusterConfig.
    status: completed
  - id: adapter-impl
    content: "Create SnowflakeAdapter in queryflux-engine-adapters with snowflake-connector-rs: auth mapping, execute_as_arrow with Snowflake-to-Arrow type conversion, health check, catalog discovery, and EngineDescriptor."
    status: completed
  - id: registration
    content: Wire SnowflakeAdapter into registered_engines.rs (descriptor + build_adapter match arm).
    status: completed
  - id: frontend-dialect
    content: Update SnowflakeHttp/SnowflakeSqlApi default_dialect() to return SqlDialect::Snowflake.
    status: completed
  - id: studio-engine-module
    content: Create snowflake.ts StudioEngineModule in lib/studio-engines/engines/ and register it in manifest.ts.
    status: completed
  - id: studio-cluster-form
    content: Create snowflake-cluster-config.tsx with fields for account, warehouse, role, schema, endpoint, catalog, and auth type selector. Register in studio-engine-forms.tsx and index.ts.
    status: completed
  - id: studio-catalog-persist
    content: Update engine-catalog.ts to use studio slot for Snowflake, and extend cluster-persist-form.ts with new Snowflake JSON keys (account, warehouse, role, schema).
    status: completed
  - id: build-verify
    content: Add snowflake-connector-rs + url deps to Cargo.toml, verify cargo check passes.
    status: completed
isProject: false
---

# Snowflake Backend Adapter

## Architecture

The Snowflake backend adapter follows the same pattern as the existing StarRocks adapter -- a **synchronous engine** that implements `execute_as_arrow` (not `submit_query`/`poll_query`). The `snowflake-connector-rs` crate handles Snowflake authentication, session management, and query execution. The adapter converts `SnowflakeRow` results into Arrow `RecordBatch` streams.

```mermaid
flowchart LR
    Frontend["Any Frontend\n(Trino HTTP, PG Wire,\nSnowflake HTTP, etc.)"]
    Dispatch["execute_to_sink"]
    Adapter["SnowflakeAdapter\n(new)"]
    Connector["snowflake-connector-rs"]
    SF["Snowflake\nWarehouse"]
    
    Frontend --> Dispatch --> Adapter --> Connector --> SF
```



## Auth Mapping

The `snowflake-connector-rs` crate supports multiple auth methods. We map from existing `ClusterAuth` variants:

- `ClusterAuth::Basic { username, password }` --> `SnowflakeAuthMethod::Password(password)` (username from the auth block)
- `ClusterAuth::KeyPair { username, private_key_pem, private_key_passphrase: Some(pass) }` --> `SnowflakeAuthMethod::KeyPair { encrypted_pem, password }`
- `ClusterAuth::KeyPair { username, private_key_pem, private_key_passphrase: None }` --> `SnowflakeAuthMethod::KeyPairUnencrypted { pem }`
- `ClusterAuth::Bearer { token }` --> `SnowflakeAuthMethod::Oauth { token }`

## Config Fields on `ClusterConfig`

Snowflake requires an `account` identifier (e.g. `xy12345.us-east-1`) to derive the API URL. Additional session-level fields (`warehouse`, `role`, `schema`) are needed. We add these as optional fields to `ClusterConfig`:

- `**account**` (new, `Option<String>`) -- Snowflake account identifier. Required for engine `snowflake`.
- `**warehouse**` (new, `Option<String>`) -- Default Snowflake virtual warehouse.
- `**role**` (new, `Option<String>`) -- Default Snowflake role.
- `**schema**` (new, `Option<String>`) -- Default Snowflake schema.
- `**endpoint**` (existing) -- Optional custom base URL override (e.g. PrivateLink). When omitted, derived from `account`.
- `**catalog**` (existing) -- Reused for Snowflake database (Snowflake's "database" is analogous to other engines' "catalog").

## Files to Change

### 1. Core types -- `[crates/queryflux-core/src/config.rs](crates/queryflux-core/src/config.rs)`

- Add `Snowflake` variant to `EngineConfig` enum
- Add `account`, `warehouse`, `role`, `schema` fields to `ClusterConfig`

### 2. Core types -- `[crates/queryflux-core/src/query.rs](crates/queryflux-core/src/query.rs)`

- Add `Snowflake` to `EngineType` enum
- Add `Snowflake` to `SqlDialect` enum
- Update `EngineType::dialect()` to return `SqlDialect::Snowflake`
- Update `SqlDialect::sqlglot_name()` to return `"snowflake"`

### 3. Engine registry -- `[crates/queryflux-core/src/engine_registry.rs](crates/queryflux-core/src/engine_registry.rs)`

- Add `Snowflake` arm to `engine_key()`, `parse_engine_key()`, and `From<&EngineConfig> for EngineType`

### 4. Engine adapters crate -- `[crates/queryflux-engine-adapters/Cargo.toml](crates/queryflux-engine-adapters/Cargo.toml)`

- Add `snowflake-connector-rs` dependency (latest version)
- Add `url` dependency (for endpoint parsing)

### 5. Adapter module -- `[crates/queryflux-engine-adapters/src/lib.rs](crates/queryflux-engine-adapters/src/lib.rs)`

- Add `pub mod snowflake;`

### 6. New file: `crates/queryflux-engine-adapters/src/snowflake/mod.rs`

The main adapter implementation, modeled after `[crates/queryflux-engine-adapters/src/starrocks/mod.rs](crates/queryflux-engine-adapters/src/starrocks/mod.rs)`:

- `**SnowflakeAdapter**` struct holding `SnowflakeClient`, cluster name, account, endpoint
- `**try_from_cluster_config**` -- validates config, builds `SnowflakeClientConfig` with the appropriate `SnowflakeAuthMethod`, `SnowflakeSessionConfig` (warehouse/role/database/schema), and optional `SnowflakeEndpointConfig::custom_base_url` when `endpoint` is set
- `**execute_as_arrow**` -- creates a Snowflake session, runs the query, converts `Vec<SnowflakeRow>` to Arrow `RecordBatch` using Snowflake column type metadata
- `**health_check**` -- runs `SELECT 1` on a fresh session
- `**submit_query**` / `**poll_query**` -- return errors (sync engine)
- `**list_catalogs**` -- `SHOW DATABASES`
- `**list_databases**` -- `SHOW SCHEMAS IN DATABASE {catalog}`
- `**list_tables**` -- `SHOW TABLES IN {catalog}.{database}`
- `**describe_table**` -- `DESCRIBE TABLE {catalog}.{database}.{table}`
- `**descriptor()**` -- `EngineDescriptor` with engine_key `"snowflake"`, `ConnectionType::Http`, default port 443, supported auth `[Basic, KeyPair, Bearer]`
- **Type mapping helper** -- maps Snowflake column types (`fixed`, `real`, `text`, `boolean`, `date`, `timestamp_ntz`, `timestamp_ltz`, `timestamp_tz`, `time`, `binary`, `variant`, `array`, `object`) to Arrow `DataType`

### 7. Registration -- `[crates/queryflux/src/registered_engines.rs](crates/queryflux/src/registered_engines.rs)`

- Import `SnowflakeAdapter`
- Add `SnowflakeAdapter::descriptor()` to `all_descriptors()`
- Add `EngineConfig::Snowflake` arm to `build_adapter()` (calls `try_from_cluster_config`)

### 8. Snowflake frontend dialect -- `[crates/queryflux-core/src/query.rs](crates/queryflux-core/src/query.rs)`

- Update `SnowflakeHttp` and `SnowflakeSqlApi` `default_dialect()` to return `SqlDialect::Snowflake` instead of `SqlDialect::Generic`

## Example YAML Configuration

```yaml
clusters:
  my-snowflake:
    engine: snowflake
    account: "xy12345.us-east-1"
    warehouse: "COMPUTE_WH"
    role: "ANALYST"
    catalog: "MY_DATABASE"
    schema: "PUBLIC"
    auth:
      type: keyPair
      username: "SVC_QUERYFLUX"
      privateKeyPem: |
        -----BEGIN PRIVATE KEY-----
        ...
        -----END PRIVATE KEY-----
```

## QueryFlux Studio Frontend Changes

The Studio UI needs a custom cluster config form and engine registration so users can add/edit Snowflake clusters from the UI. This follows the same 5-step pattern documented in `[queryflux-studio/lib/studio-engines/manifest.ts](queryflux-studio/lib/studio-engines/manifest.ts)`.

### 9. New file: `queryflux-studio/lib/studio-engines/engines/snowflake.ts`

StudioEngineModule registration (modeled after `[engines/athena.ts](queryflux-studio/lib/studio-engines/engines/athena.ts)`):

- `descriptor` with `engineKey: "snowflake"`, `displayName: "Snowflake"`, `hex: "29B5E8"`, `connectionType: "http"`, `defaultPort: 443`, `supportedAuth: ["basic", "keyPair", "bearer"]`, `implemented: true`
- `configFields` for: `account` (required), `endpoint` (optional, for PrivateLink), `warehouse`, `role`, `catalog` (labeled "Database"), `schema`, plus auth fields
- `catalog: { category: "Cloud Warehouse", simpleIconSlug: "siSnowflake", catalogDescription: "Cloud-native data warehouse built for the cloud" }`
- `validateFlat` -- require `account`, require username+password when auth=basic, require username+privateKeyPem when auth=keyPair
- `customFormId: "snowflake"`

### 10. Update: `[queryflux-studio/lib/studio-engines/manifest.ts](queryflux-studio/lib/studio-engines/manifest.ts)`

- Import `snowflakeStudioEngine` and append to `STUDIO_ENGINE_MODULES` array

### 11. New file: `queryflux-studio/components/cluster-config/snowflake-cluster-config.tsx`

Custom cluster config form component with:

- **Account** (required text input) -- Snowflake account identifier
- **Endpoint** (optional URL input) -- custom base URL / PrivateLink override
- **Warehouse** (text input) -- default virtual warehouse
- **Role** (text input) -- default Snowflake role
- **Database** (text input, mapped to `catalog` field) -- default database
- **Schema** (text input)
- **Auth type selector** (dropdown: Password / Key Pair / OAuth Token)
  - Password: username + password fields
  - Key Pair: username + private key PEM (textarea) + optional passphrase
  - OAuth: token field

Styled consistently with existing forms (Athena, StarRocks patterns).

### 12. Update: `[queryflux-studio/components/cluster-config/studio-engine-forms.tsx](queryflux-studio/components/cluster-config/studio-engine-forms.tsx)`

- Import `SnowflakeClusterConfig` and add `snowflake: SnowflakeClusterConfig` entry

### 13. Update: `[queryflux-studio/components/cluster-config/index.ts](queryflux-studio/components/cluster-config/index.ts)`

- Export `SnowflakeClusterConfig`

### 14. Update: `[queryflux-studio/components/engine-catalog.ts](queryflux-studio/components/engine-catalog.ts)`

- Replace the static Snowflake `EngineDef` entry (lines 98-106, `supported: false`) with `{ k: "studio", engineKey: "snowflake" }`

### 15. Update: `[queryflux-studio/lib/cluster-persist-form.ts](queryflux-studio/lib/cluster-persist-form.ts)`

- Add `"account"`, `"warehouse"`, `"role"`, `"schema"` to `MANAGED_CONFIG_JSON_KEYS`
- Add `flat.account`, `flat.warehouse`, `flat.role`, `flat.schema` mappings in `persistedClusterConfigToFlat`, `flatToPersistedConfig`, and `mergeClusterConfigFromFlat`
- Add to `buildValidateShape` as well

## Out of Scope

- Async query polling (Snowflake supports it, but the connector crate handles polling internally via `async_query_completion_timeout`)
- External browser SSO auth (server-side proxy cannot open browsers)
- SQL translation rules (Snowflake dialect in sqlglot -- can be added separately)

