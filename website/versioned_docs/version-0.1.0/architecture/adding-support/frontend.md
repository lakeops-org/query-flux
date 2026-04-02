---
description: Step-by-step guide to adding a new client protocol (frontend) to QueryFlux — listener, dispatch, routing, admin.
---

# Adding a new frontend (client protocol)

This page is for contributors who want **clients to connect to QueryFlux using a new wire or HTTP protocol** (ingress). It is **not** about adding a **backend engine** that runs SQL; for that, see **[Backend](backend.md)**.

**What you are building**

- A **listener** that speaks the client’s protocol, parses SQL (and auth), builds **`SessionContext`** and **`IncomingQuery`**, and hands work to the shared **dispatch** layer.
- **Config** so operators can enable the listener (port, TLS, etc.).
- Optional **protocol-based routing** so traffic from this frontend maps to a default cluster group.

Existing protocols are documented under **[Frontends](../frontends/overview.md)**. Use them as templates before writing a new one from scratch.

---

## How traffic flows (short)

1. Client connects using **its** protocol.
2. Frontend builds **`IncomingQuery`** with SQL, **`SessionContext`**, and **`FrontendProtocol`** (your enum variant).
3. **Routers** pick a **`ClusterGroupName`** (optional **`protocolBased`** entry for your variant).
4. **Translation** uses **`FrontendProtocol::default_dialect()`** as the sqlglot **source** dialect when needed.
5. **Dispatch** runs the query on the chosen backend adapter.
6. Results flow back through a **`ResultSink`** (or Trino-style async polling for engines that support it).

See the diagram in **[Frontends overview](../frontends/overview.md#shared-architecture)**.

| Dispatch style | Typical use |
|----------------|-------------|
| **`execute_to_sink`** | Most wire/gRPC frontends: run to completion, stream Arrow into a protocol-specific sink. |
| **`dispatch_query`** | Trino HTTP and other async-capable paths: return handles / `nextUri` for polling. |

---

## Reference implementations

| Protocol | Crate module | Good template when… |
|----------|----------------|---------------------|
| Trino HTTP | [`trino_http/`](https://github.com/lakeops-org/queryflux/tree/main/crates/queryflux-frontend/src/trino_http) | HTTP, async polling, custom headers |
| PostgreSQL wire | [`postgres_wire/`](https://github.com/lakeops-org/queryflux/tree/main/crates/queryflux-frontend/src/postgres_wire) | Binary framing, startup/auth, simple sync execution |
| MySQL wire | [`mysql_wire/`](https://github.com/lakeops-org/queryflux/tree/main/crates/queryflux-frontend/src/mysql_wire) | Similar to Postgres wire, different packet format |
| Flight SQL | [`flight_sql/`](https://github.com/lakeops-org/queryflux/tree/main/crates/queryflux-frontend/src/flight_sql) | gRPC / Arrow Flight |
| Snowflake HTTP | [`snowflake/`](https://github.com/lakeops-org/queryflux/tree/main/crates/queryflux-frontend/src/snowflake) | JSON REST, multiple routes on one Axum app |

All listeners implement **`FrontendListenerTrait`** (`async fn listen`) in [`queryflux-frontend/src/lib.rs`](https://github.com/lakeops-org/queryflux/blob/main/crates/queryflux-frontend/src/lib.rs).

---

## Follow this order

### Step 1 — Protocol identity (`queryflux-core`)

In **`crates/queryflux-core/src/query.rs`**:

1. Add **`FrontendProtocol::YourProtocol`** to the enum (serde **`camelCase`** in JSON where this appears).
2. Implement **`default_dialect()`** for your variant: this is the **source** dialect for sqlglot when translating client SQL. If nothing fits, **`SqlDialect::Generic`** is acceptable; you may need to extend **`SqlDialect`** and translation rules — see [query-translation.md](../query-translation.md).

### Step 2 — Session metadata (`queryflux-core`)

In **`crates/queryflux-core/src/session.rs`**, either:

- **Reuse** an existing **`SessionContext`** variant if semantics match (e.g. HTTP headers → **`TrinoHttp`** or **`ClickHouseHttp`**), or  
- Add a **new** **`SessionContext`** variant and update **`tags()`**, **`user()`**, **`database()`**, **`client_source()`** (and any other helpers) so routers and catalog code can read what they need.

Every **`IncomingQuery`** carries **`SessionContext`** + **`FrontendProtocol`**.

### Step 3 — YAML config (`queryflux-core`)

In **`crates/queryflux-core/src/config.rs`**:

1. Add a field on **`FrontendsConfig`** for your listener (usually **`Option<FrontendConfig>`** unless it is always on). Use serde **`camelCase`** for the YAML key (e.g. `myProtocol`).
2. **`FrontendConfig`** already carries **`port`**, **`bind_address`**, optional **TLS**, etc. Extend it only if every frontend needs a new knob; prefer protocol-specific structs only when necessary.

### Step 4 — Listener crate (`queryflux-frontend`)

1. Add **`pub mod your_protocol;`** in **`crates/queryflux-frontend/src/lib.rs`**.
2. Implement a type that implements **`FrontendListenerTrait`**: bind, accept connections, parse protocol, call **`dispatch_query`** or **`execute_to_sink`** from **`dispatch.rs`** with the correct **`FrontendProtocol`** and **`SessionContext`**.
3. Run credentials through the shared **auth** path like the other frontends.
4. Add **`Cargo.toml`** dependencies (codec libraries, gRPC, etc.).

**Tip:** Copy the smallest existing frontend that resembles yours, then delete protocol-specific code you do not need.

### Step 5 — Binary startup (`queryflux`)

In **`crates/queryflux/src/main.rs`**:

- Construct your frontend when config is **`Some`**.
- Spawn **`listen()`** alongside Trino / Postgres / … (see **`select!`** around other frontends).
- Thread any **protocol-based default group** strings into **`LiveConfig`** / router construction the same way **`mysql_wire`** and **`flight_sql`** do.

Hot reload behavior for frontends follows whatever **`main`** already does for listeners (many frontends are **startup-only**; check comments in **`main.rs`**).

### Step 6 — Protocol-based routing

If operators should map “clients using this protocol” → “default group”:

1. **`RouterConfig::ProtocolBased`** in **`config.rs`** — add an optional field (camelCase YAML key).
2. **`ProtocolBasedRouter`** in **`crates/queryflux-routing/src/implementations/protocol_based.rs`** — add a field and a **`match`** arm on **`FrontendProtocol`**.
3. **`main.rs`** — when building **`ProtocolBasedRouter`**, pass the configured group name (two places if there is a cold path and a reload path).
4. **`crates/queryflux-persistence/src/routing_json.rs`** — extend **`PROTO_CAMEL_SNAKE`** so Studio/admin routing JSON validation and group collection understand your new key (see existing `trinoHttp` / `postgresWire` entries).
5. Document the YAML shape in **[routing-and-clusters.md](../routing-and-clusters.md)** if operators rely on it.

Other router types (**header**, **tags**, **queryRegex**, …) usually need **no** change for a new frontend.

### Step 7 — Admin “frontends status”

**`GET /admin/frontends`** builds a snapshot from **`FrontendsConfig`** in **`queryflux-frontend/src/admin.rs`** (`build_frontends_status`). Add a branch for your frontend so the Admin API and **QueryFlux Studio** (Protocols page) can show port / enabled state.

### Step 8 — Studio (optional)

- **`queryflux-studio/app/protocols/page.tsx`** — if you want an icon on the Protocols page, extend **`PROTOCOL_SIMPLE_ICONS`** keyed by the **`id`** your admin DTO uses (match **`build_frontends_status`**).

Studio does **not** implement wire protocols; it only displays status from the Admin API.

### Step 9 — Tests and docs

- Unit or integration tests in **`queryflux-frontend`** or **`queryflux-routing`** (router tests live in **`crates/queryflux-routing/tests/router_tests.rs`**).
- Add a page under **`website/docs/architecture/frontends/`** and a row in **[Frontends overview](../frontends/overview.md)** when the protocol is ready for users.

---

## Checklist

- [ ] **`FrontendProtocol`** + **`default_dialect()`** in `queryflux-core/src/query.rs`
- [ ] **`SessionContext`** (new variant or reuse) + helper methods in `session.rs`
- [ ] **`FrontendsConfig`** field + YAML shape in `config.rs`
- [ ] **`queryflux-frontend`** module + **`FrontendListenerTrait`** + dispatch wiring
- [ ] **`main.rs`** startup (and reload paths if applicable)
- [ ] **`ProtocolBasedRouter`** + **`RouterConfig::ProtocolBased`** + **`main.rs`** wiring (if using protocol-based routing)
- [ ] **`routing_json.rs`** **`PROTO_CAMEL_SNAKE`** (if protocol-based routing is stored/edited via JSON)
- [ ] **`build_frontends_status`** in `admin.rs`
- [ ] Studio Protocols icons (optional)
- [ ] Tests + user-facing frontend doc

---

## Related reading

- [Extending QueryFlux — overview](overview.md)
- [Backend](backend.md)
- [Frontends overview](../frontends/overview.md)
- [query-translation.md](../query-translation.md)
- [routing-and-clusters.md](../routing-and-clusters.md)
- [observability.md](../observability.md)

**Key Rust files**

- [`crates/queryflux-core/src/query.rs`](https://github.com/lakeops-org/queryflux/blob/main/crates/queryflux-core/src/query.rs) — `FrontendProtocol`, `IncomingQuery`
- [`crates/queryflux-core/src/session.rs`](https://github.com/lakeops-org/queryflux/blob/main/crates/queryflux-core/src/session.rs) — `SessionContext`
- [`crates/queryflux-core/src/config.rs`](https://github.com/lakeops-org/queryflux/blob/main/crates/queryflux-core/src/config.rs) — `FrontendsConfig`, `RouterConfig`
- [`crates/queryflux-frontend/src/dispatch.rs`](https://github.com/lakeops-org/queryflux/blob/main/crates/queryflux-frontend/src/dispatch.rs) — `dispatch_query`, `execute_to_sink`
- [`crates/queryflux-routing/src/implementations/protocol_based.rs`](https://github.com/lakeops-org/queryflux/blob/main/crates/queryflux-routing/src/implementations/protocol_based.rs) — protocol → group
- [`crates/queryflux/src/main.rs`](https://github.com/lakeops-org/queryflux/blob/main/crates/queryflux/src/main.rs) — listener startup, router construction
