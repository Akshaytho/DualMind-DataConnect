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
- `workspace/dataconnect/scanner/__init__.py` — stub
- `workspace/dataconnect/scanner/schema.py` — extract_schema(engine) → (list[TableInfo], list[RelationshipInfo]) via SQLAlchemy inspect()
- `workspace/dataconnect/router/__init__.py` — stub
- `workspace/dataconnect/verifier/__init__.py` — stub
- `workspace/dataconnect/api/__init__.py` — stub
- `workspace/tests/conftest.py` — sample_engine, sample_scan_result, storage fixtures
- `workspace/tests/test_models.py` — model validation tests (14 tests)
- `workspace/tests/test_database.py` — read-only enforcement tests (6 tests)
- `workspace/tests/test_storage.py` — storage CRUD tests (7 tests)
- `workspace/tests/test_verifier_base.py` — protocol + helper tests (4 tests)
- `workspace/tests/test_scanner_schema.py` — schema extraction tests (10 tests)
- `workspace/requirements.txt` — pinned deps

## Tech Stack (locked)
- Python 3.11+, SQLAlchemy 2.0, sentence-transformers, FAISS, NetworkX
- sqlparse, SQLite, FastAPI, pydantic v2, litellm, pytest+hypothesis

## Key Decisions
- BYOK: users bring own API key via litellm
- Name in ONE place: config.py → PROJECT_NAME
- Read-only SQL only. No writes ever.
- BIRD benchmark for accuracy testing (Mini-Dev 500, PostgreSQL format)

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
