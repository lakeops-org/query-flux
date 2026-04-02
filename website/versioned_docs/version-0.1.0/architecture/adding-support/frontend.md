---
description: Adding a new frontend client protocol to QueryFlux — FrontendProtocol, listener, dispatch, and routing.
---

# Frontend

Goal: clients speak a **wire or HTTP protocol to QueryFlux** (ingress), not a new backend.

See **[Extending QueryFlux](overview.md)** for how this differs from a **backend** engine. Documentation for **existing** frontends (Trino HTTP, Postgres wire, MySQL wire, Flight SQL, Snowflake) lives under **[Frontends](../frontends/overview.md)**.

### Where the code lives

- **PostgreSQL wire:** `crates/queryflux-frontend/src/postgres_wire/`
- **MySQL wire:** `crates/queryflux-frontend/src/mysql_wire/`
- **Trino HTTP:** `crates/queryflux-frontend/src/trino_http/`
- **Flight SQL:** `crates/queryflux-frontend/src/flight_sql/`
- **Snowflake:** `crates/queryflux-frontend/src/snowflake/`

### Typical steps for a new protocol

1. **`FrontendProtocol`** — Already defined in `queryflux_core::query::FrontendProtocol`; add a variant only for a **new** ingress protocol.
2. **`default_dialect()`** — Set the sqlglot **source** dialect for translation (see [query-translation.md](../query-translation.md)).
3. **Listener** — Bind a port, parse the protocol, build **`SessionContext`** and **`InboundQuery`**, then call shared **`dispatch_query`** (or the same helpers Trino HTTP uses).
4. **Routing** — Optionally extend **protocol-based routing** in config / persisted routing so this frontend maps to the right default group.
5. **Tests** — Protocol-level tests or e2e clients as appropriate.

Studio does **not** implement wire protocols; it only talks to the **Admin API** for config and metrics.

---

## Checklist (frontend)

- [ ] `FrontendProtocol` + dialect + listener module + dispatch integration + routing docs  

---

## Related reading

- [Extending QueryFlux — overview](overview.md)  
- [Backend](backend.md) — Rust adapter and Studio  
- [Frontends](../frontends/overview.md) — Existing protocol listeners  
- [query-translation.md](../query-translation.md) — Dialects and sqlglot  
- [routing-and-clusters.md](../routing-and-clusters.md) — Routers and groups  
