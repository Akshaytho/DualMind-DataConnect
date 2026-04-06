# Shared Memory — DataConnect

## Architecture (from spec)
- Layer 1: Scanner (schema + profiling + relationship discovery + semantic descriptions)
- Layer 2: Router (embeddings + graph + LLM cross-check)
- Layer 3: Verifier (6 deterministic checks + confidence scoring)

## Code Map
_Update as files are created:_
- `workspace/dataconnect/__init__.py` — package root, exports PROJECT_NAME + __version__
- `workspace/dataconnect/config.py` — PROJECT_NAME, constants, sanitize_connection_string()
- `workspace/dataconnect/exceptions.py` — 12 typed exceptions (DataConnectError base)
- `workspace/dataconnect/models.py` — 14 Pydantic models (Scanner: ColumnInfo, ColumnProfile, TableInfo, RelationshipType, RelationshipInfo, ScanResult; Router: MatchMethod, TableMatch, RouteResult; Verifier: CheckStatus, CheckResult, VerificationResult; Output: QueryResult)
- `workspace/dataconnect/database.py` — create_readonly_engine() with write-blocking event listener
- `workspace/dataconnect/storage.py` — StorageBackend class (SQLite CRUD for ScanResult)
- `workspace/dataconnect/verifier/base.py` — CheckProtocol + make_result() helper
- `workspace/dataconnect/scanner/__init__.py` — scan_database() orchestrator: schema→profiling→relationships→token estimation→ScanResult
- `workspace/dataconnect/scanner/schema.py` — extract_schema(engine) → (list[TableInfo], list[RelationshipInfo]) via SQLAlchemy inspect()
- `workspace/dataconnect/scanner/profiler.py` — profile_table/profile_tables: data sampling + ColumnProfile stats (null_fraction, distinct_count, sample_values, min/max)
- `workspace/dataconnect/scanner/relationships.py` — discover_relationships(): name matching (FK naming conventions) + value overlap (Jaccard similarity) for non-FK relationship discovery
- `workspace/dataconnect/router/__init__.py` — route_query() orchestrator: embeddings + graph + LLM cross-check → merged RouteResult
- `workspace/dataconnect/router/embeddings.py` — EmbeddingIndex: sentence-transformers + numpy cosine similarity for semantic table matching. table_to_text() converts TableInfo to embeddable text. Lazy model loading.
- `workspace/dataconnect/router/graph.py` — RelationshipGraph: NetworkX graph from RelationshipInfo, BFS walk with confidence-weighted scoring and depth decay
- `workspace/dataconnect/verifier/schema_conformity.py` — SchemaConformityCheck: validates all SQL table/column references exist in schema (sqlparse + regex, case-insensitive, alias-aware)
- `workspace/dataconnect/verifier/join_validation.py` — JoinValidationCheck: validates join columns exist, types compatible, relationships known (regex-based JOIN parsing, type group matching, bidirectional relationship lookup)
- `workspace/dataconnect/verifier/aggregation_validation.py` — AggregationValidationCheck: GROUP BY completeness, aggregate function type safety (SUM/AVG on numerics), HAVING clause validation
- `workspace/dataconnect/verifier/filter_validation.py` — FilterValidationCheck: WHERE value validation against column profiles — numeric range (min/max), string enum (sample_values), NULL plausibility, IN lists, BETWEEN overlap
- `workspace/dataconnect/verifier/result_plausibility.py` — ResultPlausibilityCheck: pre-execution plausibility — empty tables (FAIL), unbounded results, SELECT *, cartesian products, high-null columns (WARN)
- `workspace/dataconnect/verifier/__init__.py` — stub
- `workspace/dataconnect/api/__init__.py` — stub
- `workspace/tests/conftest.py` — sample_engine, sample_scan_result, storage fixtures
- `workspace/tests/test_models.py` — model validation tests (14 tests)
- `workspace/tests/test_database.py` — read-only enforcement tests (6 tests)
- `workspace/tests/test_storage.py` — storage CRUD tests (7 tests)
- `workspace/tests/test_verifier_base.py` — protocol + helper tests (4 tests)
- `workspace/tests/test_scanner_schema.py` — schema extraction tests (10 tests)
- `workspace/tests/test_scanner_profiler.py` — profiling tests (17 tests)
- `workspace/tests/test_scanner_relationships.py` — relationship discovery tests (27 tests)
- `workspace/tests/test_scanner_orchestrator.py` — scan_database pipeline tests (24 tests)
- `workspace/tests/test_router_embeddings.py` — embedding index tests with mocked model (27 tests)
- `workspace/tests/test_router_graph.py` — relationship graph walk tests (15 tests)
- `workspace/tests/test_router_orchestrator.py` — route_query pipeline tests (38 tests)
- `workspace/tests/test_verifier_schema_conformity.py` — schema conformity check tests (30 tests)
- `workspace/tests/test_verifier_join_validation.py` — join validation check tests (19 tests)
- `workspace/tests/test_verifier_aggregation.py` — aggregation validation check tests (48 tests)
- `workspace/tests/test_verifier_filter.py` — filter validation check tests (40 tests)
- `workspace/tests/test_verifier_result_plausibility.py` — result plausibility check tests (45 tests)
- `workspace/requirements.txt` — pinned deps (pydantic, sqlalchemy, pytest, hypothesis, numpy, networkx, litellm, sqlparse)

## Tech Stack (locked)
- Python 3.11+, SQLAlchemy 2.0, sentence-transformers, FAISS, NetworkX
- sqlparse, SQLite, FastAPI, pydantic v2, litellm, pytest+hypothesis

## Key Decisions
- BYOK: users bring own API key via litellm
- Name in ONE place: config.py → PROJECT_NAME
- Read-only SQL only. No writes ever.
- BIRD benchmark for accuracy testing (Mini-Dev 500, PostgreSQL format)
- Router embeddings: numpy cosine sim for now (sufficient for <100 tables), FAISS can be added as optimization later

## Patterns & Conventions
- models.py flat initially, split at 300 lines into models/ package
- Verifier: per-check files with shared CheckProtocol in base.py
- storage.py at root level (shared SQLite index interface)
- Phase 1 compressed to single turn (scaffolding + models + db + tests)
- Dep direction: models ← storage ← scanner/router ← verifier ← cli
- StorageBackend: upsert by database_name, takes directory path

## Known Bugs & Tech Debt
- database.py `_block_writes` checks first keyword only — CTE wrapping (`WITH x AS (DELETE...)`) could bypass. Low risk since we control all SQL generation, but harden later.

## Additional Security (v3.1)
- [x] Connection strings sanitized in logs (mask passwords) — config.py
- [x] All deps pinned in requirements.txt
- [ ] FastAPI requires X-API-Key header on all endpoints
- [ ] Rate limiting: 60 queries/min per API key
- [ ] No secrets in git ever

## Bridge Protections (v3.1)
- 10-minute timeout per turn (kills if hung)
- Stuck detection: 2 retries then rollback + pause
- Git recovery: auto-clean dirty state
- Rate limit: 15 turns/hour max
