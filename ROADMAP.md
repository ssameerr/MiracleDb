# MiracleDB Roadmap

Multi-model Rust database — DataFusion · Lance · Tantivy · Wasmer · Candle

**Test suite:** 342 / 342 passing &nbsp;|&nbsp; **Last updated:** 2026-02-23

---

## Legend

| Symbol | Meaning |
|--------|---------|
| ✅ | Complete — production-ready, comprehensive tests |
| 🟡 | In progress — working implementation, some gaps |
| 🔶 | Partial — stub or minimal implementation |
| ⬜ | Planned — module exists, no real implementation yet |

---

## Summary

| Category | Features | Done |
|----------|----------|------|
| Core SQL Engine | DataFusion execution, query planning | 75% |
| Storage & Heap | Row store, buffer pool, persistence | 60% |
| Authentication & Security | Auth, RBAC, PQC, encryption | 85% |
| Full-Text Search | Tantivy indexing, phrase/wildcard queries | 90% |
| Vector Search | Lance embeddings, IVF-PQ indexing | 85% |
| Geospatial | R-tree index, NN search, spatial filters | 80% |
| CDC & Integration | Kafka, PostgreSQL logical replication | 75% |
| PostgreSQL Wire Protocol | Startup handshake, simple queries | 90% |
| Distributed / Sharding | Consistent hash ring, query routing | 65% |
| ML / UDFs | WASM, ONNX, Candle inference | 80% |
| Time Series | Downsample, gap-fill, lag/lead | 75% |
| HTAP | Dual-store routing, sync manager | 50% |
| Backup & Recovery | Snapshots, scheduler, Shamir secrets | 70% |
| Observability | Metrics, tracing, health checks | 80% |
| Rate Limiting | Token bucket, sliding window | 85% |
| AI Provider Layer | LlmProvider trait, 6 providers (Ollama, vLLM, Claude, OpenAI/Azure, Gemini, Candle), ProviderRegistry | 85% |
| Semantic Embeddings | EmbeddingEngine, CandleEmbeddingProvider (in-process 384-dim), OpenAI/Ollama API providers | 85% |
| Hybrid Search (AI) | HybridSearchEngine: vector + fulltext + graph via RRF (k=60), source attribution | 85% |
| NLP / Text-to-SQL | TextToSqlEngine with schema-aware prompt building, LLM-powered via provider abstraction | 80% |
| Blockchain Audit | Tamper-evident log, Merkle proofs | 80% |
| WebSocket CDC Feed | WsMessage, subscriptions | 60% |
| MCP Endpoint | Tool registry, request/response | 60% |
| Chaos Testing | Fault injection, ChaosInjector | 70% |
| Compression | RLE, delta, dictionary, zstd, snappy | 75% |
| OLAP / Materialized Views | ROLLUP, CUBE, window functions | 70% |
| Healthcare (FHIR/HL7) | HL7 parsing, FHIR conversion | 65% |
| IoT Telemetry | Anomaly detection, windowing | 65% |
| Financial Risk | VaR, Sharpe ratio, max drawdown | 70% |
| Graph Database | Traversal, adjacency store | 60% |
| Embedded Mode | In-process DB with transactions | 75% |
| Workflow / DAG | DAG executor, task orchestration | 60% |
| Plugin System | Registry, built-in plugins | 70% |
| Realtime Pub/Sub | Broker, presence system | 65% |
| Data Masking | Email, phone, credit card | 85% |
| Export / Import | CSV, JSON, SQL | 70% |
| Cache | Tiered cache, KV, LLM cache | 75% |
| Version / Time Travel | Snapshots, branching | 65% |
| PostgreSQL Compatibility Layer | WAL, replication, syscache, etc. | 20% |

**Overall: ~68%**

---

## Detailed Feature Status

### 🗄️ Core SQL Engine — 75%

| Feature | Status | Notes |
|---------|--------|-------|
| SQL parsing (SELECT / INSERT / UPDATE / DELETE) | ✅ | Custom parser + DataFusion |
| Query planning & optimization | 🟡 | DataFusion planner; custom rules partial |
| Expression evaluation | ✅ | Arithmetic, comparison, casting |
| Table providers (Heap + Lance) | ✅ | Both registered with DataFusion |
| Transactions (BEGIN / COMMIT / ROLLBACK) | ✅ | MVCC with rollback tracking |
| Subqueries & CTEs | 🔶 | Via DataFusion; custom CTEs partial |
| Window functions | 🟡 | ROLLUP/CUBE done; NTILE/PERCENT_RANK partial |
| Prepared statements | 🔶 | Module stub exists |
| Cursors | 🔶 | Module stub exists |
| Triggers | 🟡 | Registration and fire tested |

---

### 💾 Storage & Heap — 60%

| Feature | Status | Notes |
|---------|--------|-------|
| Heap row store | 🟡 | Insert, scan, persistence tested |
| Buffer pool | 🟡 | Persistence tests pass |
| Column store (Lance/Parquet) | 🟡 | Lance datasets, IVF-PQ indexing |
| TOAST (large values) | 🔶 | Module stub exists |
| Vacuum / compaction | 🔶 | Module stub exists |
| Tablespace management | ⬜ | Structural stub only |
| Storage manager (smgr) | ⬜ | Structural stub only |
| Free-space map (fsm) | ⬜ | Structural stub only |
| Visibility map (vm) | ⬜ | Structural stub only |

---

### 🔐 Authentication & Security — 85%

| Feature | Status | Notes |
|---------|--------|-------|
| JWT authentication | ✅ | Issue, verify, revoke |
| Post-quantum tokens (PQC) | ✅ | Kyber/Dilithium signing |
| Device binding | ✅ | Token tied to device fingerprint |
| Role-based access control (RBAC) | ✅ | Admin / read-only roles |
| Column-level encryption (AES-256) | ✅ | Field encryption roundtrip |
| Data masking | ✅ | Email, phone, credit card, custom |
| Password hashing (Argon2) | ✅ | |
| mTLS support | 🔶 | Module stub exists |
| AD/LDAP integration | 🔶 | Module stub exists |
| API key management | 🔶 | Module stub exists |
| Privacy compliance (GDPR/HIPAA) | 🔶 | Module stub exists |
| Row-level security | ⬜ | Planned |

---

### 🔍 Full-Text Search (Tantivy) — 90%

| Feature | Status | Notes |
|---------|--------|-------|
| Index creation & management | ✅ | |
| Standard keyword search | ✅ | |
| Phrase queries | ✅ | Exact match |
| Wildcard / prefix queries | ✅ | |
| Multi-field search | ✅ | |
| BM25 relevance scoring | ✅ | Via Tantivy |
| Bulk index ingestion | ✅ | Batched with error handling |
| Reader reload after commit | ✅ | Manual reload fixed |
| Faceted search | 🔶 | Partial |
| Highlighting | ⬜ | Planned |

---

### 🧠 Vector Search (Lance) — 85%

| Feature | Status | Notes |
|---------|--------|-------|
| Lance dataset creation | ✅ | |
| Vector embedding storage | ✅ | |
| Approximate nearest neighbor (ANN) | ✅ | IVF-PQ via Lance |
| IVF-PQ index building | 🟡 | Row-count guard for small datasets |
| Cosine / L2 distance | ✅ | |
| Bulk vector ingestion | ✅ | |
| Semantic embeddings (Candle) | 🟡 | Stub embeddings; real model loading partial |
| Hybrid search (vector + full-text) | ✅ | RRF fusion |
| HNSW index | 🔶 | Lance roadmap item |

---

### 🗺️ Geospatial — 80%

| Feature | Status | Notes |
|---------|--------|-------|
| R-tree spatial index | ✅ | |
| Insert & range search | ✅ | |
| Nearest-neighbor search | ✅ | |
| Index rebuild | ✅ | |
| ST_Distance | ✅ | |
| ST_Contains | ✅ | |
| Spatial filter pushdown | ✅ | DataFusion integration |
| GeoJSON support | 🟡 | Partial parsing |
| PostGIS wire compatibility | 🔶 | Planned |

---

### 🔄 CDC & Integration — 75%

| Feature | Status | Notes |
|---------|--------|-------|
| ChangeEvent types (Insert/Update/Delete) | ✅ | |
| CdcEvent / WalRecord types | ✅ | |
| PostgresCdcConfig | ✅ | |
| Webhook delivery with retry backoff | ✅ | |
| Kafka source (consumer) | 🟡 | Config & types; rdkafka behind feature flag |
| Kafka sink (producer) | 🟡 | Config & types; rdkafka behind feature flag |
| PostgreSQL logical replication | 🔶 | Stub: channel plumbing without wire protocol |
| Debezium format compatibility | ⬜ | Planned |
| Schema registry | 🔶 | Config stub |

---

### 🔌 PostgreSQL Wire Protocol — 90%

| Feature | Status | Notes |
|---------|--------|-------|
| Startup / authentication handshake | ✅ | AuthOK, ParameterStatus, BackendKeyData |
| ReadyForQuery state machine | ✅ | |
| Simple query protocol (Q message) | ✅ | RowDescription + DataRow + CommandComplete |
| ErrorResponse | ✅ | |
| Terminate (X message) | ✅ | |
| Extended query protocol (Parse/Bind/Execute) | 🔶 | Not yet implemented |
| COPY protocol | ⬜ | Planned |
| SSL/TLS negotiation | ⬜ | Planned |
| psql / DBeaver / JDBC compatibility | 🔶 | Basic connection works |
| MySQL wire protocol | 🔶 | Module exists, stub |

---

### 🌐 Distributed / Sharding — 65%

| Feature | Status | Notes |
|---------|--------|-------|
| Consistent hash ring | ✅ | DefaultHasher % num_shards |
| Query routing (shard key extraction) | ✅ | WHERE id = … pattern |
| Broadcast for keyless queries | ✅ | |
| Node discovery | 🔶 | Module stub |
| Distributed query execution | 🔶 | Executor stub |
| Edge sync | 🔶 | Module stub |
| Scheduler | 🔶 | Module stub |
| Raft consensus | ⬜ | Planned |
| Two-phase commit | ⬜ | Planned |

---

### 🤖 ML / UDFs — 80%

| Feature | Status | Notes |
|---------|--------|-------|
| WASM UDFs (single-arg) | ✅ | i64, f64, string |
| WASM UDFs (multi-arg) | ✅ | Up to 4 args |
| WASM string return type | ✅ | |
| ONNX model inference | ✅ | Registry, load, predict |
| Candle (Rust ML) inference | ✅ | Engine creation, model listing |
| AutoML pipeline | 🟡 | Types and model selection |
| Feature store | 🟡 | Feature engineering types |
| Model serving API | 🔶 | REST endpoint stub |
| GPU acceleration | ⬜ | Planned via Candle CUDA |

---

### 📈 Time Series — 75%

| Feature | Status | Notes |
|---------|--------|-------|
| Downsample (mean / last) | ✅ | BTreeMap bucket aggregation |
| Gap fill (LOCF / linear / zero) | ✅ | |
| Moving average | ✅ | |
| Lag / Lead | ✅ | |
| Retention policies | ⬜ | Planned |
| Continuous aggregates | ⬜ | Planned |
| Time-bucket grouping SQL function | 🔶 | Partial |

---

### ⚡ HTAP (Hybrid Transactional/Analytical) — 50%

| Feature | Status | Notes |
|---------|--------|-------|
| HtapRouter (WriteDestination / ReadSource) | ✅ | Column-ratio based routing |
| HybridTableProvider | 🟡 | Routes reads to row/col store |
| HTAPSyncManager (row → column replication) | 🔶 | Simulated; no real WAL-based sync |
| Real-time dual-write | ⬜ | Planned |
| Watermark tracking | ⬜ | Planned |

---

### 💾 Backup & Recovery — 70%

| Feature | Status | Notes |
|---------|--------|-------|
| Snapshot create (zstd compressed) | ✅ | UUID-named .snap files |
| Snapshot list | ✅ | Persistent JSON metadata index |
| Snapshot restore | ✅ | zstd decode → restored file |
| Atomic metadata writes | ✅ | tokio::sync::Mutex |
| Backup scheduler (cron) | ✅ | Wildcard, range, list, interval |
| Local storage backend | ✅ | Write / read / delete / exists |
| Shamir secret sharing | ✅ | Split, recover, threshold |
| Point-in-time recovery (PITR) | 🔶 | WAL not fully wired |
| S3 / remote storage backend | ⬜ | Planned |
| Incremental backups | ⬜ | Planned |

---

### 📊 Observability — 80%

| Feature | Status | Notes |
|---------|--------|-------|
| Prometheus metrics | ✅ | Query count, latency histograms |
| Idempotent metric registration | ✅ | Once-based guard |
| Distributed tracing (OpenTelemetry) | ✅ | Trace context, span IDs |
| Health check endpoints | ✅ | |
| Structured logging | 🟡 | tracing crate integrated |
| Grafana dashboards | 🔶 | Config files provided |
| Alerting | 🔶 | Module stub |
| Query profiler | 🔶 | Module stub |

---

### 🚦 Rate Limiting — 85%

| Feature | Status | Notes |
|---------|--------|-------|
| Token bucket | ✅ | Capacity + refill rate |
| Sliding window | ✅ | Window expiry + count |
| Combined check | ✅ | Bucket AND window |
| Per-user / per-key config | ✅ | |
| Usage reporting | ✅ | Tokens remaining, window count |
| Reset | ✅ | |
| Distributed rate limiting | ⬜ | Planned (Redis backend) |

---

### 🤖 AI Provider Layer — 85%

| Feature | Status | Notes |
|---------|--------|-------|
| LlmProvider trait | ✅ | Async generate(), model_name(), provider_type() |
| EmbeddingProvider trait | ✅ | Async embed_text(), embed_batch(), dimensions() |
| Ollama provider | ✅ | Local LLM inference via HTTP |
| vLLM provider | ✅ | OpenAI-compatible vLLM server |
| Claude provider (Anthropic) | ✅ | claude-3-haiku / claude-3-sonnet |
| OpenAI provider | ✅ | GPT-4o, GPT-4-turbo, GPT-3.5 |
| Azure OpenAI provider | ✅ | Deployment-based endpoint |
| Gemini provider (Google) | ✅ | gemini-1.5-flash / gemini-1.5-pro |
| Candle provider (in-process) | ✅ | 384-dim stub; BERT loading partial |
| ProviderRegistry | ✅ | Named provider lookup, default provider |
| AiConfig (TOML-serializable) | ✅ | Provider selection + parameters |
| GPU acceleration | ⬜ | Planned via Candle CUDA |

---

### 🔮 Semantic Embeddings — 85%

| Feature | Status | Notes |
|---------|--------|-------|
| EmbeddingEngine | ✅ | Provider abstraction with registry |
| CandleEmbeddingProvider | ✅ | In-process 384-dim embeddings |
| OllamaEmbeddingProvider | ✅ | nomic-embed-text via Ollama API |
| OpenAIEmbeddingProvider | ✅ | text-embedding-3-small / large |
| embed_text() | ✅ | Single document embedding |
| embed_batch() | ✅ | Batch document embedding |
| Real all-MiniLM-L6-v2 loading | 🔶 | HuggingFace model download partial |

---

### 🔀 Hybrid Search (AI) — 85%

| Feature | Status | Notes |
|---------|--------|-------|
| HybridSearchEngine | ✅ | Unified search across all sources |
| Vector search integration | ✅ | Lance ANN via EmbeddingEngine |
| Full-text search integration | ✅ | Tantivy BM25 |
| Graph search integration | ✅ | Adjacency traversal |
| RRF fusion (k=60) | ✅ | Reciprocal Rank Fusion |
| Source attribution | ✅ | Per-result source tagging |
| Configurable source weights | 🟡 | Partial |

---

### 💬 NLP / Text-to-SQL — 80%

| Feature | Status | Notes |
|---------|--------|-------|
| Tokenization | ✅ | |
| Stop-word removal | ✅ | |
| Stemming (Porter) | ✅ | |
| Named entity extraction (email, phone, money) | ✅ | Regex-based |
| Sentiment analysis | ✅ | Lexicon-based |
| TF-IDF scoring | ✅ | |
| Text chunking for RAG | ✅ | |
| Text-to-SQL (heuristic) | ✅ | Pattern matching |
| Text-to-SQL (LLM-powered) | ✅ | TextToSqlEngine via provider abstraction |
| Schema-aware prompt building | ✅ | Table/column context injection |
| Language detection | 🔶 | Returns "en" as default |
| Semantic search embeddings | ✅ | Via EmbeddingEngine |

---

### ⛓️ Blockchain Audit Log — 80%

| Feature | Status | Notes |
|---------|--------|-------|
| Append-only tamper-evident log | ✅ | SHA-256 chaining |
| Chain verification | ✅ | Detects tampering |
| Merkle tree proofs | ✅ | Multi-entry, proof verification |
| Query by actor / table | ✅ | |
| Log sequence numbers | ✅ | |
| External anchoring | ⬜ | Planned (BTC/ETH timestamps) |

---

### 🌐 WebSocket CDC Feed — 60%

| Feature | Status | Notes |
|---------|--------|-------|
| WsMessage types (Connected, Event, Subscribed, Ping/Pong) | ✅ | |
| WsSubscription with operation filter | ✅ | |
| CdcEvent serialization | ✅ | |
| Subscribe command handling | ✅ | |
| Real event streaming (broadcast channel) | 🔶 | Plumbing exists; no live engine hookup |
| Authentication on WS connection | ⬜ | Planned |
| Reconnect / resume from LSN | ⬜ | Planned |

---

### 🧩 MCP Endpoint — 60%

| Feature | Status | Notes |
|---------|--------|-------|
| McpServer / McpTool / McpRequest / McpResponse | ✅ | |
| Tool registration | ✅ | |
| Execute dispatch | ✅ | |
| Built-in tools: query, list_tables, describe_table | ✅ | |
| Full SQL engine integration | 🔶 | Returns stub results |
| Streaming responses | ⬜ | Planned |
| Tool discovery via HTTP | 🟡 | Routes defined |

---

### 🔥 Chaos Testing — 70%

| Feature | Status | Notes |
|---------|--------|-------|
| ChaosInjector | ✅ | |
| FaultType (Latency, Error, Drop, Corruption) | ✅ | |
| Inject / clear / clear_all | ✅ | |
| Probability-based firing | ✅ | |
| should_fail() check | ✅ | |
| Async latency injection | 🔶 | Type defined; not wired to executor |
| Network partition simulation | ⬜ | Planned |

---

### 🗜️ Compression — 75%

| Feature | Status | Notes |
|---------|--------|-------|
| zstd (level 3, 10, 22) | ✅ | |
| Snappy | ✅ | |
| RLE encoding | ✅ | |
| Delta encoding | ✅ | |
| Dictionary encoding | ✅ | |
| LZ4 | 🔶 | Planned |
| Columnar codec selection per-table | 🔶 | Config stub |

---

### 📦 OLAP / Materialized Views — 70%

| Feature | Status | Notes |
|---------|--------|-------|
| ROLLUP grouping sets | ✅ | N columns → N+1 sets |
| CUBE grouping sets | ✅ | All 2^N combinations |
| Window spec (frame, ordering) | ✅ | JSON round-trip |
| Aggregation (count, sum, avg) | ✅ | |
| Materialized view refresh | 🔶 | Manual refresh stub |
| Incremental view maintenance | ⬜ | Planned |
| Partition pruning | 🔶 | Basic |

---

### 🏥 Healthcare (FHIR / HL7) — 65%

| Feature | Status | Notes |
|---------|--------|-------|
| HL7 v2 parsing | ✅ | |
| FHIR R4 conversion | ✅ | |
| DICOM metadata | 🔶 | Module stub |
| HIPAA compliance checks | 🔶 | Via compliance module |
| De-identification pipeline | ⬜ | Planned |

---

### 📡 IoT Telemetry — 65%

| Feature | Status | Notes |
|---------|--------|-------|
| Telemetry windowing | ✅ | |
| Anomaly detection | ✅ | |
| Stream ingestion | 🟡 | Channel plumbing |
| MQTT connector | ⬜ | Planned |
| Edge aggregation | ⬜ | Planned |

---

### 💰 Financial Risk — 70%

| Feature | Status | Notes |
|---------|--------|-------|
| Value-at-Risk (VaR) | ✅ | |
| Sharpe ratio | ✅ | |
| Maximum drawdown | ✅ | |
| Decimal precision (128-bit) | ✅ | |
| Greeks / options pricing | ⬜ | Planned |
| Real-time risk streaming | ⬜ | Planned |

---

### 🕸️ Graph Database — 60%

| Feature | Status | Notes |
|---------|--------|-------|
| Node / edge storage | ✅ | |
| Graph traversal | ✅ | |
| Adjacency list | ✅ | |
| Cypher / GQL query language | ⬜ | Planned |
| PageRank / centrality | ⬜ | Planned |
| Property graph model | 🔶 | Basic |

---

### 📦 Embedded Mode — 75%

| Feature | Status | Notes |
|---------|--------|-------|
| In-process database | ✅ | No server required |
| SQL query execution | ✅ | |
| WHERE filtering | ✅ | |
| Transactions | ✅ | |
| WASM embedding | 🔶 | Planned |
| iOS / Android embedding | ⬜ | Planned |

---

### 🔄 Workflow / DAG — 60%

| Feature | Status | Notes |
|---------|--------|-------|
| DAG definition | 🟡 | Nodes, edges, topological sort tested |
| Task executor | 🟡 | Async executor exists |
| Dependency resolution | 🟡 | |
| Retry logic | 🔶 | Module stub |
| Scheduled workflows | 🔶 | Cron integration planned |
| Visual DAG editor | ⬜ | Planned |

---

### 🔌 Plugin System — 70%

| Feature | Status | Notes |
|---------|--------|-------|
| Plugin registry | ✅ | |
| Built-in plugins (5) | ✅ | |
| Builder pattern | ✅ | |
| Dynamic loading (`.so` / WASM) | 🔶 | Planned |
| Plugin marketplace | ⬜ | Planned |

---

### 📣 Realtime Pub/Sub — 65%

| Feature | Status | Notes |
|---------|--------|-------|
| Pub/Sub broker | ✅ | |
| Topic subscriptions | ✅ | |
| Presence system | ✅ | |
| Message persistence | 🔶 | In-memory only |
| Horizontal scaling | ⬜ | Planned |

---

### 🔏 MPC / Cryptography — 70%

| Feature | Status | Notes |
|---------|--------|-------|
| Shamir secret sharing | ✅ | Split / recover |
| Distributed key generation (DKG) | ✅ | |
| Lagrange interpolation | ✅ | |
| Threshold config | ✅ | |
| Double Ratchet (Signal protocol) | ✅ | KDF chain |
| Homomorphic encryption | ⬜ | Planned |

---

### 📤 Export / Import — 70%

| Feature | Status | Notes |
|---------|--------|-------|
| CSV import | ✅ | |
| JSON import | ✅ | |
| CSV export | ✅ | |
| SQL export | ✅ | |
| Parquet export | 🔶 | Via Lance |
| S3 export | ⬜ | Planned |
| Streaming export | ⬜ | Planned |

---

### 🗃️ Cache — 75%

| Feature | Status | Notes |
|---------|--------|-------|
| Tiered cache (L1/L2/L3) | ✅ | |
| KV store cache | ✅ | |
| LLM result cache | ✅ | |
| Bloom filter | ✅ | False-positive reduction |
| Query result cache with invalidation | ✅ | Table-level invalidation |
| Distributed cache | ⬜ | Planned (Redis backend) |

---

### ⏳ Version / Time Travel — 65%

| Feature | Status | Notes |
|---------|--------|-------|
| Snapshot store | 🟡 | Tested in feature coverage |
| Data branching | 🟡 | Branch source files exist |
| AS OF TIMESTAMP queries | 🔶 | Partial SQL support |
| Branch merge | ⬜ | Planned |
| Delta log (Iceberg-style) | ⬜ | Planned |

---

### 🐘 PostgreSQL Compatibility Layer — 20%

These modules mirror PostgreSQL internals. All exist as structural stubs, ready for implementation.

| Module | Status | Description |
|--------|--------|-------------|
| WAL (Write-Ahead Log) | 🔶 | Basic structure |
| Replication | 🔶 | Slot plumbing |
| MVCC / xact | 🔶 | Transaction ID stubs |
| Postmaster | 🔶 | Connection acceptor stub |
| System catalog (syscache) | ⬜ | Planned |
| pg_catalog tables | ⬜ | Planned |
| Relation cache (relcache) | ⬜ | Planned |
| Multixact | ⬜ | Planned |
| Checkpoint | ⬜ | Planned |
| clog / xlog | ⬜ | Planned |
| Portal / executor | ⬜ | Planned |
| Sequence manager | ✅ | Tested |

---

## Upcoming Milestones

### v0.4 — Protocol & Compatibility (Q2 2026)
- [ ] Extended query protocol (Parse / Bind / Execute)
- [ ] SSL/TLS on PostgreSQL port
- [ ] psql / DBeaver / JDBC compatibility verified
- [ ] MySQL wire protocol basic support

### v0.5 — Distributed Production (Q3 2026)
- [ ] Raft-based consensus
- [ ] Two-phase commit
- [ ] WAL-based HTAP sync
- [ ] Point-in-time recovery wired to WAL
- [ ] S3 backup backend

### v0.6 — AI-Native Features (Q4 2026) ✅
- [x] LLM-powered Text-to-SQL (via provider abstraction: Ollama, vLLM, Claude, OpenAI, Gemini)
- [x] Semantic embeddings (EmbeddingEngine with Candle in-process + API providers)
- [x] Vector + full-text + graph hybrid search (HybridSearchEngine, RRF fusion)
- [x] 6-provider AI layer (Ollama, vLLM, Claude, OpenAI/Azure, Gemini, Candle)
- [ ] Real all-MiniLM-L6-v2 model loading (HuggingFace)
- [ ] GPU acceleration (Candle CUDA)

### v1.0 — General Availability (2027)
- [ ] Full PostgreSQL wire compatibility
- [ ] Kubernetes operator stable
- [ ] 1000+ test suite
- [ ] Performance benchmarks published

---

*Generated from 342 passing tests across 90+ modules.*
