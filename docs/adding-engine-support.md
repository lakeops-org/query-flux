# Adding engine and protocol support

This guide separates two ideas that are easy to conflate:

| Concept | Meaning | Example |
|--------|---------|---------|
| **Backend engine** | A **cluster** type QueryFlux routes queries **to**. It has an adapter that talks to the real database (HTTP, MySQL wire, embedded library, AWS SDK, …). | Trino, DuckDB, StarRocks, Athena |
| **Frontend protocol** | How **clients connect to QueryFlux** (ingress). SQL enters with a `FrontendProtocol` and a default source dialect for translation. | Trino HTTP, **PostgreSQL wire**, MySQL wire, Flight SQL |

Adding **PostgreSQL wire** as a client entrypoint is **not** the same as adding “PostgreSQL” as a backend: today, `PostgresWire` is already a frontend in `queryflux-frontend`; traffic still lands on the shared dispatch path and is sent to whatever **backend adapter** routing chose (often Trino).

Use the sections below depending on whether you are extending **Studio**, a **backend adapter**, or a **frontend listener**.

---

## Part A — Backend engine (Rust)

Goal: a new `engine` value in cluster config, a live adapter, validation, translation target dialect, and wiring in the binary.

### 1. Core model (`queryflux-core`)

- **`EngineConfig`** — Add a variant in `crates/queryflux-core/src/config.rs` (serde **camelCase** in JSON/YAML, e.g. `myEngine`).
- **`EngineType`** — Add a variant in `crates/queryflux-core/src/query.rs` if the backend is distinct for metrics, translation, or dispatch.
- **`engine_registry::engine_key()`** — Map `EngineConfig` → stable string key (must match the adapter descriptor and Studio). See `crates/queryflux-core/src/engine_registry.rs`.
- **`EngineType::dialect()`** — Return the `SqlDialect` used as the **translation target** (and extend `SqlDialect` / `is_compatible_with` in translation if needed). See [query-translation.md](query-translation.md).
- **`ClusterConfig` fields** — Add any new top-level fields (region, paths, engine-specific blobs). Prefer keeping engine-specific secrets and options in `config` JSON for Postgres-backed clusters; extend the typed struct when YAML and validation need them everywhere.

### 2. Adapter crate (`queryflux-engine-adapters`)

- Add a module (e.g. `src/myengine/mod.rs`) implementing **`EngineAdapterTrait`** (`submit_query`, `poll_query`, `cancel_query`, `health_check`, `engine_type`, `supports_async`, and optionally `fetch_running_query_count`, `base_url`, Arrow/catalog hooks as needed).
- Implement **`descriptor() -> EngineDescriptor`** with:
  - `engine_key`, `display_name`, `description`, `hex`
  - `connection_type` (`Http`, `MySqlWire`, `Embedded`, `ManagedApi`)
  - `supported_auth` and **`config_fields`** (these drive `/admin/engine-registry` and should stay aligned with Studio)
  - `implemented: true` when the adapter is actually wired in `main`
- Export the module from `crates/queryflux-engine-adapters/src/lib.rs` and add the crate dependency if you introduce new third-party crates.

### 3. Binary wiring (`crates/queryflux`)

- **`EngineRegistry::new(vec![...])`** — Append `MyEngineAdapter::descriptor()` next to the other adapters (startup).
- **Pass 1 adapter construction** — In the block that matches `EngineConfig` and builds `Arc<dyn EngineAdapterTrait>`, add your variant (mirror the pattern used for Trino / DuckDB / Athena).
- **`reload_live_config` / `build_live_config`** — The same `EngineConfig` match exists for **hot reload**; add your engine there too (search for `EngineConfig::` in `main.rs`).
- **`engine_type_from_config`** — Map config → `EngineType` for cluster state and metrics.
- **Special rules** — Search for engine-specific checks (e.g. `queryAuth` / impersonation) and extend validation if your engine has constraints.

### 4. Dispatch and frontends (`queryflux-frontend`)

- Shared query execution goes through **`dispatch_query`** / **`execute_to_sink`**. Usually no change if the new engine only differs in the adapter; if you need a **special execution path** (like Trino raw HTTP), follow the existing engine-specific branches.
- Per-protocol handlers (Trino HTTP, Postgres wire, …) should keep using the shared dispatch layer unless the protocol requires a dedicated contract.

### 5. Persistence (`queryflux-persistence`) — why touch it if config is JSON?

The table stores **`engine_key` as its own column** plus a **`config` JSONB** blob. The DB does not load straight into the proxy as opaque JSON: code paths call **`ClusterConfigRecord::to_core()`**, which must produce a typed **`ClusterConfig`** (including **`EngineConfig`**).

So persistence changes are **not** “because Postgres needs a JSON schema.” They are because of this **explicit conversion layer**:

1. **`match self.engine_key.as_str()`** — Maps the string in the `engine_key` column to a Rust **`EngineConfig::*` variant**. If you add a variant in `queryflux-core` but forget this arm, reload/startup fails with **“Unknown engine key in DB”** even though the JSON looks fine.
2. **`UpsertClusterConfig::from_core`** — The inverse when YAML (or similar) is turned into a row: which **`engine_key` string** to write for your new variant.

**Extra JSON keys** that only live inside `config` and are **already** read in `to_core` (e.g. `endpoint`, `region`, `authType`, …) usually need **no** persistence change beyond the engine-key match. You only extend the `s("…")` / `b("…")` helpers in `to_core` (and the matching `from_core` inserts) if you add **new top-level persisted fields** on `ClusterConfig` that should round-trip through that JSON.

**Hot reload** often uses `list_cluster_configs` → records → `to_core()` → `build_live_config`; the same conversion applies.

### 6. Optional: routing config

- If operators choose the new group via **router JSON** (`RouterConfig` variants), no change unless you add a new router **type**.
- **Protocol-based routing** maps frontend labels to **group names**; it does not list backend engines.

### 7. Tests and docs

- Add or extend **e2e** tests under `crates/queryflux-e2e-tests` if you have a dockerized target.
- Update [architecture.md](architecture.md) component status if you document supported engines there.

---

## Part B — QueryFlux Studio (UI, TypeScript / React)

Studio is the Next.js app under `ui/queryflux-studio/`. It does **not** run wire protocols; it calls the **Admin API** (`ADMIN_API_URL`, default `http://localhost:9000`) for clusters, groups, routing, and scripts.

Today the **engine schema** is duplicated: forms use the static **`ENGINE_REGISTRY`** in TypeScript, while the proxy exposes the same shape at **`GET /admin/engine-registry`**. Keeping them aligned is manual until Studio loads descriptors from the API at runtime.

### Where users see engines

| User action | UI entrypoint | What must know your engine |
|-------------|---------------|----------------------------|
| Create cluster | **Clusters → Add cluster** (`components/add-cluster-dialog.tsx`) | `ENGINE_CATALOG` (picker) + `findEngineDescriptor` + `validateClusterConfig` / `validateEngineSpecific` + `toUpsertBody` |
| Edit cluster | **Clusters** grid → cluster card → Edit (`app/clusters/clusters-grid.tsx`) | Same + `mergeClusterConfigFromFlat` / `buildClusterUpsertFromForm` + `EngineClusterConfig` |
| View config | Cluster detail / engine config view in `clusters-grid.tsx` | `findEngineDescriptor` for labels; unknown key shows “add to engine registry” warning |
| Group strategy **engine affinity** | **Engines →** group dialog → strategy (`components/group-form-dialog.tsx`) | `ENGINE_AFFINITY_OPTIONS` in `lib/cluster-group-strategy.ts` (allowed `preference` values) |
| Live utilization cards | **Engines (Groups)** page (`app/engines/page.tsx`) | `findEngineByType` + **`ENGINE_TYPE_ALIASES`** in `engine-catalog.ts` so Rust `engine_type` debug strings map to catalog rows for icons |

### 1. Engine registry (required for create/edit/validation)

**File:** `ui/queryflux-studio/lib/engine-registry.ts`

- Append an **`EngineDescriptor`** to **`ENGINE_REGISTRY`** with the same **`engineKey`**, **`connectionType`**, **`supportedAuth`**, and **`configFields`** as Rust’s `EngineDescriptor` (`ConfigField.key` strings must match, including dotted paths like `auth.username`).
- Extend **`ConnectionType`** / **`AuthType`** unions if Rust added a new variant.
- **`findEngineDescriptor`** — No code change; it searches `ENGINE_REGISTRY` by `engineKey` (and normalizes case).
- **`validateClusterConfig(clusterName, engineKey, payload, options?)`** — Add branches if your engine needs schema checks beyond generic “required fields from descriptor” (see existing patterns).
- **`listImplementedEngines()`** / **`isClusterOnboardingSelectable`** — Driven by `implemented: true` on the descriptor and catalog entries; ensure catalog and registry agree.

### 2. Add-cluster wizard catalog (required for “pick engine” UX)

**File:** `ui/queryflux-studio/components/engine-catalog.ts`

- Add an **`EngineDef`** row: **`name`**, **`simpleIconSlug`** (or `null` for initials fallback), **`hex`**, **`category`**, **`description`**, **`engineKey`** (same string as YAML/API), **`supported: true`** when the adapter ships.
- **`isClusterOnboardingSelectable`** (in `engine-registry.ts`) requires `supported && engineKey` and a matching implemented descriptor — all three must line up.
- **`ENGINE_TYPE_ALIASES`** — If the **live** `/admin/clusters` snapshot returns a new Rust `EngineType` debug string (e.g. `MyEngine`), add a lowercase alias → **`EngineDef.name`** so **Engines** page cluster rows resolve icons via `findEngineByType`.

### 3. Cluster config forms

**Router:** `ui/queryflux-studio/components/cluster-config/engine-cluster-config.tsx`

- Default: **`GenericEngineClusterConfig`** renders fields from the descriptor’s `configFields`.
- Custom panel: add `if (engineKey === "myEngine") return <MyEngineClusterConfig … />` and a sibling file under `components/cluster-config/` (see `trino-cluster-config.tsx`, `athena-cluster-config.tsx`, …).

**Dedicated components (reference only):** `trino-cluster-config.tsx`, `starrocks-cluster-config.tsx`, `athena-cluster-config.tsx`, `generic-engine-cluster-config.tsx`, `config-field-row.tsx`.

### 4. Persisted JSON ↔ flat form (create + edit save path)

**File:** `ui/queryflux-studio/lib/cluster-persist-form.ts`

- **`MANAGED_CONFIG_JSON_KEYS`** — Include any new top-level keys inside `cluster_configs.config` JSON that edit mode should overwrite or clear when the user empties a field.
- **`persistedClusterConfigToFlat`** — Seed flat state from DB (camelCase JSON keys like `authType`, `authUsername`, …).
- **`flatToPersistedConfig`** — New-cluster path from add-cluster dialog.
- **`mergeClusterConfigFromFlat`** — Edit path: merge onto existing `config` without dropping unknown keys.
- **`buildValidateShape`** — Build the nested object passed into **`validateClusterConfig`** (must include `endpoint`, `auth`, `tls`, etc. as your engine expects).
- **`validateEngineSpecific(engineKey, flat)`** — Cross-field rules (e.g. “basic auth requires username+password”) before PUT/PATCH.

### 5. Clusters page (grid, dialog, validation)

**File:** `ui/queryflux-studio/app/clusters/clusters-grid.tsx`

- Uses **`findEngineDescriptor`**, **`validateClusterConfig`**, **`validateEngineSpecific`**, **`buildValidateShape`**, **`skipImplementedCheck`** for editing clusters that are in Postgres but not yet marked implemented in TS (if you use that flag during rollout).
- No per-engine switch here beyond what **`EngineClusterConfig`** does.

**File:** `ui/queryflux-studio/components/add-cluster-dialog.tsx`

- Wires catalog → descriptor → `EngineClusterConfig` → `toUpsertBody` → `upsertClusterConfig` API.

### 6. Group strategy (engine affinity)

**File:** `ui/queryflux-studio/lib/cluster-group-strategy.ts`

- **`ENGINE_AFFINITY_OPTIONS`** — If operators can target your backend via **`engineAffinity`** strategy (`preference` list of engine keys), add `{ value: "<engineKey>", label: "…" }`. **`buildStrategyPayload` / validation** only allow keys in this list (see **`ENGINE_SET`** and parse errors).

If the new engine is never used in `engineAffinity`, you can skip this.

### 7. Display helpers

**File:** `ui/queryflux-studio/lib/merge-clusters-display.ts`

- Merges live + persisted cluster rows; uses **`findEngineDescriptor(p.engineKey)`** for display name. Registry entry must exist.

**File:** `ui/queryflux-studio/components/ui-helpers.tsx` (**`EngineBadge`**)

- Uses **`ENGINE_CATALOG`** by **display name** for some badges; catalog entry should match.

**File:** `ui/queryflux-studio/components/engine-icon.tsx`

- Consumes **`EngineDef`** from the catalog (simple-icons path or initials). No change if the catalog row is complete.

### 8. API types (usually unchanged)

**File:** `ui/queryflux-studio/lib/api-types.ts`

- **`ClusterConfigRecord`** / **`UpsertClusterConfig`** use generic **`config: Record<string, unknown>`** and **`engineKey: string`** — no new TypeScript types are required per engine unless you add strongly typed helpers.

### 9. Optional: fetch registry from the proxy

To remove duplication, a follow-up could load **`GET /admin/engine-registry`** in a server component or hook and pass descriptors into add-cluster / edit forms. Until then, **Rust `descriptor()` and `ENGINE_REGISTRY` must stay in sync by hand.**

### Studio checklist (copy-paste)

- [ ] `lib/engine-registry.ts` — `ENGINE_REGISTRY` entry, auth/connection unions, `validateClusterConfig` if needed  
- [ ] `components/engine-catalog.ts` — `EngineDef`, `supported` + `engineKey`, optional **`ENGINE_TYPE_ALIASES`** for live `engine_type` strings  
- [ ] `components/cluster-config/engine-cluster-config.tsx` — custom form branch if not generic-only  
- [ ] `lib/cluster-persist-form.ts` — managed keys, flat ↔ JSON, `buildValidateShape`, `validateEngineSpecific`  
- [ ] `lib/cluster-group-strategy.ts` — `ENGINE_AFFINITY_OPTIONS` if engine affinity should list this engine  
- [ ] Smoke-test: Add cluster → save → edit → save; create/edit group with engine affinity if applicable  

---

## Part C — Frontend protocol (e.g. “more Postgres wire”)

Goal: clients speak a **wire protocol to QueryFlux**, not a new backend.

### Where the code lives

- **PostgreSQL wire:** `crates/queryflux-frontend/src/postgres_wire/`
- **MySQL wire:** `crates/queryflux-frontend/src/mysql_wire/`
- **Trino HTTP:** `crates/queryflux-frontend/src/trino_http/`
- **Flight SQL:** `crates/queryflux-frontend/src/flight_sql/`

### Typical steps

1. **`FrontendProtocol`** — Already defined in `queryflux_core::query::FrontendProtocol`; add a variant only for a **new** ingress protocol.
2. **`default_dialect()`** — Set the sqlglot **source** dialect for translation (see [query-translation.md](query-translation.md)).
3. **Listener** — Bind a port, parse the protocol, build **`SessionContext`** and **`InboundQuery`**, then call shared **`dispatch_query`** (or the same helpers Trino HTTP uses).
4. **Routing** — Optionally extend **protocol-based routing** in config / persisted routing so this frontend maps to the right default group.
5. **Tests** — Protocol-level tests or e2e clients as appropriate.

Studio does **not** implement wire protocols; it only talks to the **Admin API** for config and metrics.

---

## Checklist summary

**Backend engine**

- [ ] `EngineConfig` + `EngineType` + `engine_key()` + dialect mapping  
- [ ] `EngineAdapterTrait` + `descriptor()`  
- [ ] `main.rs`: registry, pass-1 construction, reload path, `engine_type_from_config`  
- [ ] Persistence / YAML seed mappings if applicable  
- [ ] Translation / compatibility if dialect is new  

**Studio (UI)**

- [ ] `lib/engine-registry.ts` — `ENGINE_REGISTRY`, unions, `validateClusterConfig` as needed  
- [ ] `components/engine-catalog.ts` — `EngineDef` + `ENGINE_TYPE_ALIASES` for live metrics icons  
- [ ] `components/cluster-config/engine-cluster-config.tsx` — custom form or generic-only  
- [ ] `lib/cluster-persist-form.ts` — JSON ↔ flat, `validateEngineSpecific`, `buildValidateShape`  
- [ ] `lib/cluster-group-strategy.ts` — `ENGINE_AFFINITY_OPTIONS` if strategy should mention the engine  
- [ ] Verify add-cluster + edit-cluster flows and Engines page cluster icons  

**New client protocol**

- [ ] `FrontendProtocol` + dialect + listener module + dispatch integration + routing docs  

---

## Related reading

- [architecture.md](architecture.md) — End-to-end flow  
- [query-translation.md](query-translation.md) — Dialects and sqlglot  
- [routing-and-clusters.md](routing-and-clusters.md) — Routers and groups  
- [observability.md](observability.md) — Admin API (including engine registry JSON)  
