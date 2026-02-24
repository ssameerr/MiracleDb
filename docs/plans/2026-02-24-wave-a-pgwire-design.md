# Wave A — Protocol & Compatibility Design

**Date:** 2026-02-24
**Status:** Approved
**Scope:** v0.4 — PostgreSQL Extended Query Protocol + SSL/TLS via pgwire

---

## Overview

Replace the existing 244-line manual PostgreSQL wire protocol implementation with `pgwire` (0.29+) as the message-handling foundation. Add extended query protocol support (Parse / Bind / Execute / Describe / Sync), SSL/TLS, and verify psql / JDBC / DBeaver compatibility.

**PostgreSQL Wire Protocol: 60% → 90%** after this wave.

---

## Architecture

### Approach: pgwire trait-based handler

A single `MiracleDbHandler` struct holds `Arc<Engine>` (the existing DataFusion engine) and implements two pgwire traits:

- `SimpleQueryHandler` — handles `Q` (simple query) messages
- `ExtendedQueryHandler` — handles `P/B/E/D/S` (extended query) messages

pgwire handles all message framing, protocol state machine, error responses, and TLS negotiation. We implement only the business logic.

### Module Layout

```
src/protocol/
  mod.rs         — re-exports
  postgres.rs    — MiracleDbHandler (pgwire-based, replaces current manual parser)
  type_map.rs    — Arrow DataType → PostgreSQL OID mapping (new)
  mysql.rs       — unchanged stub (basic handshake only)
```

### New Dependency

```toml
pgwire = { version = "0.29", features = ["server-api"] }
```

pgwire is production-proven: used by GreptimeDB, SpacetimeDB, risinglight, PeerDB.

### Auth

`NoopStartupHandler` — no credential check. Matches existing behavior. Can be upgraded to SCRAM later.

---

## Data Flow

### Simple Query (`Q` message)

```
Client: "SELECT * FROM users"
  → pgwire routes to MiracleDbHandler::do_query()
  → engine.query(sql) → Vec<RecordBatch>
  → Arrow schema → Vec<FieldInfo> (PostgreSQL column descriptors)
  → DataRowEncoder streams each row as DataRow messages
  → CommandComplete("SELECT N") + ReadyForQuery
```

### Extended Query (`P/B/E/D/S` messages)

```
Parse("stmt1", "SELECT * FROM orders WHERE id = $1", [INT4])
  → store in DashMap<String, StoredStatement>
  → ParseComplete

Bind("portal1", "stmt1", params=[42])
  → create Portal with bound values
  → BindComplete

Execute("portal1", max_rows=0)
  → substitute $1 → 42, pass SQL to engine
  → stream rows as DataRow messages
  → CommandComplete

Describe("S", "stmt1")
  → return ParameterDescription + RowDescription without executing

Sync → ReadyForQuery
```

**Parameter substitution:** Replace `$1`, `$2`, … with bound literal values before passing SQL to DataFusion. Simple string substitution — no separate parameter binding layer needed at this stage.

### SSL/TLS

```
Client sends SSLRequest (8-byte startup message, protocol 1234.5679)
  → if TLS configured: respond 'S', wrap socket with tokio-rustls via pgwire
  → if not configured: respond 'N', continue plain TCP (no breaking change)
```

Enabled when `miracledb.toml` has:

```toml
[tls]
cert_file = "/path/to/cert.pem"
key_file  = "/path/to/key.pem"
```

---

## Type Mapping (`src/protocol/type_map.rs`)

Arrow DataType → PostgreSQL OID (text format encoding):

| Arrow DataType | PostgreSQL | OID |
|---|---|---|
| `Int8 / Int16 / Int32 / UInt8 / UInt16` | `INT4` | 23 |
| `Int64 / UInt32 / UInt64` | `INT8` | 20 |
| `Float32` | `FLOAT4` | 700 |
| `Float64` | `FLOAT8` | 701 |
| `Utf8 / LargeUtf8` | `TEXT` | 25 |
| `Boolean` | `BOOL` | 16 |
| `Date32` | `DATE` | 1082 |
| `Timestamp(*, *)` | `TIMESTAMP` | 1114 |
| `Binary / LargeBinary` | `BYTEA` | 17 |
| everything else | `TEXT` (safe fallback) | 25 |

All values encoded as **text format** (PostgreSQL default). Binary encoding is deferred.

---

## Testing Strategy

### Tier 1 — Unit tests (always run, `cargo test --lib`)

- Type mapping: `test_arrow_int32_maps_to_int4`, `test_arrow_float64_maps_to_float8`, `test_arrow_utf8_maps_to_text`, `test_arrow_bool_maps_to_bool`, `test_arrow_unknown_maps_to_text` (~8 tests)
- Simple query handler: mock engine returning fixed RecordBatch, verify correct `FieldInfo` and `DataRow` output (~4 tests)
- Extended query: parse stores statement in map, bind creates portal, execute returns rows, describe returns metadata without executing (~6 tests)
- Parameter substitution: `test_substitute_single_param`, `test_substitute_multiple_params`, `test_substitute_no_params` (~3 tests)

**Total: ~20 new unit tests**

### Tier 2 — Integration tests (opt-in)

```bash
cargo test --features integration   # requires psql in PATH
```

- `SELECT 1` round-trip via psql (verifies full wire protocol)
- Prepared statement via libpq

CI stays fast — no external deps required for `cargo test --lib`.

---

## Files to Create / Modify

| File | Action |
|---|---|
| `Cargo.toml` | Add `pgwire = "0.29"` |
| `src/protocol/postgres.rs` | Rewrite — `MiracleDbHandler` using pgwire traits |
| `src/protocol/type_map.rs` | Create — Arrow → PostgreSQL OID mapping |
| `src/protocol/mod.rs` | Modify — add `pub mod type_map` |
| `src/protocol/mysql.rs` | No change (stub stays as-is) |
| `ROADMAP.md` | Update PostgreSQL Wire Protocol: 60% → 90% |

---

## Success Criteria

- `cargo test --lib` stays green (344+ tests, 0 failures)
- `MiracleDbHandler` implements both `SimpleQueryHandler` and `ExtendedQueryHandler`
- Extended query state machine: parse → bind → execute → describe → sync all handled
- SSL/TLS: config-gated, plain TCP when not configured
- Parameter substitution works for `$1`…`$N` in prepared statements
- ROADMAP updated to 90%
