# MiracleDB 100% Completion Design

**Date**: 2026-02-22
**Approach**: Test-First (TDD) Completion
**Current state**: ~60–65% of 50-category checklist complete; 256/256 library tests passing

---

## Approach

Strict TDD in 3 rounds ordered by production-criticality:

1. Write failing tests for target feature
2. Implement minimum code to pass
3. Commit tests + implementation together (no green-without-tests)

Parallel agents own isolated modules per round — no merge conflicts.

---

## Round 1 — Production-Critical

### 1A. PITR / Backup & Recovery (`src/recovery/`)
**Tests:**
- `test_create_snapshot()` — Lance checkpoint written to archive path
- `test_restore_to_timestamp()` — dataset restored from snapshot + WAL replay
- `test_incremental_backup()` — only changed segments since last backup
- `test_zstd_compression()` — backup file is smaller than raw
- `test_backup_verification()` — checksum validation on restore

**Implementation:**
- `SnapshotManager`: writes Lance dataset checkpoints to configurable archive dir
- `WalArchiver`: copies WAL segments to archive on commit
- `RestoreManager`: replays WAL segments from a given snapshot to target timestamp
- Zstd compression wrapper around archive writes

---

### 1B. PostgreSQL Wire Protocol (`src/protocol/`)
**Tests:**
- `test_startup_handshake()` — client connects, authentication OK
- `test_simple_query()` — `Query` message → `RowDescription` + `DataRow` + `CommandComplete`
- `test_error_response()` — invalid SQL → `ErrorResponse` packet
- `test_parameter_status()` — server sends encoding/timezone params
- `test_ready_for_query()` — correct state transitions

**Implementation:**
- `PgProtocolServer`: tokio TCP listener on port 5432 (configurable)
- Message codec: `StartupMessage`, `Query`, `Parse`/`Bind`/`Execute` (extended query)
- Response encoder: `RowDescription`, `DataRow`, `CommandComplete`, `ErrorResponse`
- Route incoming SQL through existing DataFusion engine
- Sufficient for psql, DBeaver, and JDBC clients to connect

---

### 1C. Distributed Query Routing (`src/cluster/`)
**Tests:**
- `test_shard_key_extraction()` — correct shard for a given row key
- `test_fanout_dispatch()` — query sent to correct worker nodes
- `test_result_merge()` — RecordBatch streams from multiple nodes merged correctly
- `test_node_failure_fallback()` — query succeeds when one worker is down
- `test_cross_shard_join()` — join across two shards returns correct rows

**Implementation:**
- `QueryCoordinator::route()`: parse query, extract shard key, dispatch sub-queries
- Worker RPC via existing gRPC/tonic channel
- `ResultMerger`: merge sorted `RecordBatch` streams (union + sort)
- Failure handling: retry on next available replica

---

### 1D. Time Series Completions (`src/timeseries/`)
**Tests:**
- `test_downsampling_mean()` — 1-second readings → 1-minute averages
- `test_downsampling_last()` — last-value downsampling
- `test_gap_fill_linear()` — missing timestamps filled by linear interpolation
- `test_gap_fill_forward()` — LOCF (last observation carried forward)
- `test_moving_average()` — N-period moving average
- `test_lead_lag()` — lead/lag offset functions

**Implementation:**
- DataFusion window UDFs: `downsample(col, interval, agg)`, `gap_fill(col, method)`, `moving_avg(col, n)`, `lead(col, n)`, `lag(col, n)`
- Register UDFs at engine startup in `engine/mod.rs`

---

### 1E. CDC from PostgreSQL (`src/integration/`)
**Tests:**
- `test_replication_slot_create()` — slot created on PG source
- `test_wal_decode_insert()` — INSERT WAL record → CdcEvent::Insert
- `test_wal_decode_update()` — UPDATE → CdcEvent::Update with before/after
- `test_wal_decode_delete()` — DELETE → CdcEvent::Delete
- `test_event_bus_delivery()` — events delivered to internal subscriber

**Implementation:**
- `PostgresCdcSource` using `tokio-postgres` logical replication protocol
- WAL decoder for `pgoutput` plugin format
- Deliver `CdcEvent` to internal pub/sub bus (already implemented)

---

## Round 2 — Differentiated Features

### 2A. NLP / Text-to-SQL (`src/nlp/`)
**Tests:**
- `test_text_to_sql_select()` — "show all users" → `SELECT * FROM users`
- `test_text_to_sql_filter()` — "users older than 30" → `WHERE age > 30`
- `test_ner_person()` — extract PERSON entities from text
- `test_ner_date()` — extract DATE entities
- `test_sentiment_positive()` / `test_sentiment_negative()`
- `test_language_detection()` — detect English vs Spanish

**Implementation:**
- Template-based Text-to-SQL matcher (regex + schema-aware keyword mapping)
- Optional LLM call via HTTP to configurable endpoint for complex queries
- NER using candle-core transformer or regex rules for common entity types
- Sentiment: simple lexicon-based classifier

---

### 2B. HTAP Dual-Store Sync (`src/engine/htap.rs`)
**Tests:**
- `test_write_propagates_to_columnar()` — heap insert → Lance row appears
- `test_read_routing_oltp()` — point lookup → heap store
- `test_read_routing_olap()` — aggregation → columnar store
- `test_sync_latency()` — columnar store lag < 100ms after write
- `test_zero_copy_sharing()` — Arrow buffer not duplicated between stores

**Implementation:**
- `HtapRouter`: wraps both `HeapTableProvider` and `LanceTableProvider`
- Write path: write to heap, async task propagates to Lance
- Read path: query analyzer picks store based on query shape
- Use `tokio::sync::watch` channel for propagation

---

### 2C. Raft Consensus (`src/cluster/`)
**Tests:**
- `test_leader_election()` — one node becomes leader
- `test_log_replication()` — entry committed on majority
- `test_term_advancement()` — stale leader steps down
- `test_snapshot_install()` — lagging node catches up via snapshot

**Implementation:**
- Integrate `openraft` crate (well-maintained, async-native)
- `MiracleRaftStorage`: implements `RaftStorage` trait backed by WAL
- Wire into cluster coordinator for distributed write consistency

---

### 2D. Chaos Testing (`src/testing/`)
**Tests (self-testing):**
- `test_node_kill_recovery()` — kill a worker, query still returns
- `test_network_partition_heal()` — partition then heal, data consistent
- `test_disk_full_graceful()` — disk full → error returned, no corruption
- `test_concurrent_writes_mvcc()` — 100 concurrent writers, no lost updates

**Implementation:**
- `ChaosInjector`: trait with `kill_node()`, `partition_network()`, `fill_disk()`
- In-process simulation using tokio task cancellation + temp dir limits
- Pluggable into integration test harness

---

## Round 3 — Remaining Stubs

### 3A. MCP Endpoint (`src/api/mcp.rs`)
- Tests: tool listing, tool invocation, schema exposure
- Implement: Model Context Protocol JSON-RPC handler exposing DB as AI tool

### 3B. WebSocket CDC Feed (`src/api/websocket.rs`)
- Tests: connect → receive INSERT/UPDATE/DELETE events in real time
- Implement: tokio-tungstenite handler that subscribes to pub/sub bus

### 3C. Rate Limiting (`src/ratelimit/`)
- Tests: token bucket, sliding window, per-IP and per-user limits
- Implement: axum middleware using `governor` crate

### 3D. DICOM Support (`src/healthcare/dicom.rs`)
- Tests: parse DICOM header tags, extract patient/study metadata
- Implement: `dicom-object` crate integration

### 3E. Internationalization (`src/collation/`, `src/format/`)
- Tests: locale-aware number/date formatting, Unicode collation order
- Implement: `icu` crate for collation; `chrono-tz` for timezone formatting

### 3F. SQL IDE Backend (`src/api/`)
- Tests: execute query → JSON results, schema browser endpoint, query history
- Implement: dedicated REST endpoints + basic query history store

### 3G. Homomorphic Encryption Stub (`src/security/`)
- Tests: encrypt value, compute on ciphertext, decrypt result
- Implement: TFHE-rs integration for basic add/compare on encrypted integers

### 3H. zk-SNARKs Stub (`src/blockchain/`)
- Tests: prove statement, verify proof
- Implement: `bellman` crate integration for basic Groth16 proof

### 3I. Documentation (`docs/`)
- Auto-generate OpenAPI from axum routes (utoipa already wired)
- ERD generation from schema metadata
- Architecture diagrams as Mermaid in docs/

---

## Success Criteria

- `cargo test --lib` → 0 failures, 400+ tests
- `cargo test` (integration) → 0 failures
- Every one of the 50 categories has at least one passing test
- psql can connect and run basic SQL via PG wire protocol
- A backup can be created and restored to a specific timestamp

---

## File Ownership (Parallel Agents, No Conflicts)

| Agent | Files |
|-------|-------|
| A1 | `src/recovery/`, `src/wal/` |
| A2 | `src/protocol/` |
| A3 | `src/cluster/sharding.rs`, `src/cluster/executor.rs` |
| A4 | `src/timeseries/` |
| A5 | `src/integration/` (CDC) |
| A6 | `src/nlp/` |
| A7 | `src/engine/htap.rs` |
| A8 | `src/cluster/` (Raft — `raft.rs` new file) |
| A9 | `src/testing/chaos.rs` (new) |
| A10 | `src/api/mcp.rs`, `src/api/websocket.rs`, `src/ratelimit/`, `src/healthcare/dicom.rs`, `src/collation/`, `src/format/`, `src/security/homomorphic.rs` |
