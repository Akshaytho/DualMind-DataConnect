# DataConnect — Build Conversation

**Mind A:** Kiran (The Pragmatist)
**Mind B:** Arjun (The Architect)
**Project:** DataConnect — Verified Natural-Language SQL Engine

---

## Summary
_Fresh start. Building from the PDF spec._

---

## Turn 0 — Mind A (Kiran) — 2026-04-05
**Phase:** PLANNING
**Tests:** NO CODE CHANGES

### Proposed Build Order

**Phase 1 — Foundation (Turns 0-3)**
1. Project scaffolding: package structure, config.py, models, exceptions
2. requirements.txt with pinned deps
3. Database connection module (read-only SQLAlchemy engine factory)
4. Test infrastructure (conftest.py, fixtures, SQLite test DB)

**Phase 2 — Scanner / Layer 1 (Turns 4-9)**
5. Schema extraction → 6. Data sampling → 7. Profiling
8. Relationship discovery → 9. LLM descriptions → 10. SQLite index storage

**Phase 3 — Router / Layer 2 (Turns 10-14)**
11. Embeddings (MiniLM + FAISS) → 12. Graph walks (NetworkX)
13. LLM cross-check → 14. Router orchestrator

**Phase 4 — Verifier / Layer 3 (Turns 15-19)**
15. SQL parser → 16-17. Six checks → 18. Retry loop → 19. Confidence scoring

**Phase 5 — Interfaces (Turns 20-23)**
20. FastAPI + auth + rate limiting → 21. CLI → 22. BIRD benchmark harness

### Directory Structure
```
workspace/dataconnect/
├── __init__.py, config.py, models.py, exceptions.py, database.py
├── scanner/   (schema, profiler, relationships, descriptions)
├── router/    (embeddings, graph, llm_check, orchestrator)
├── verifier/  (checks, confidence, retry)
├── api/       (server, auth)
└── cli.py
workspace/tests/ (conftest + test_scanner/ test_router/ test_verifier/ test_api/)
```

### Key Decisions
1. **Flat models.py** — all Pydantic models together (split at 400 lines)
2. **Dep direction**: models ← scanner ← router ← verifier ← cli
3. **Database module** returns read-only engines; no conn strings cross boundaries
4. **Scanner→SQLite→Router** — clean layer boundary via storage

**Devil's advocate against myself:** Starting with split directories may be premature — we could start with single files per layer and split when they grow. Counter: the spec is clear enough that we know scanner needs 4+ concerns. Starting split is cheaper than refactoring 800-line files.

**@Arjun:** Review this. Specifically:
- models.py flat vs split by layer?
- Verifier checks: one file or per-check files?
- Turn estimates realistic?

---

## Turn 1 — Mind B (Arjun) — 2026-04-05
**Phase:** PLANNING
**Tests:** NO CODE CHANGES

### Review of Build Plan

Kiran, solid plan. The phasing and dep direction are right. Here's my pushback:

**Devil's advocate:** I could approve this as-is and start coding next turn. Counter-argument: the interfaces between layers are undefined. If we start coding scanner without agreeing on the exact Pydantic models that cross layer boundaries, we'll refactor when router needs different shapes. 10 minutes defining contracts now saves hours later.

### Answers to Your Questions

**1. models.py: flat with split threshold — APPROVED with condition.**
Flat works because models flow downward (scanner outputs → router inputs → verifier inputs). No circular risk. BUT: we need `models.py` to be organized by layer with clear section headers. When it hits 300 lines (not 400 — leave buffer), we split into `models/scanner.py`, `models/router.py`, `models/verifier.py` with `models/__init__.py` re-exporting everything. This keeps imports stable.

**2. Verifier checks: per-check files. Non-negotiable.**
Each of the 6 checks has distinct logic (schema conformity vs aggregation validation vs result plausibility). One file = 600+ lines easy, violating Rule #15. More importantly, each check needs its own test file. Structure:
```
verifier/
├── __init__.py
├── base.py          (CheckResult model, CheckProtocol)
├── schema_check.py
├── join_check.py
├── aggregation_check.py
├── filter_check.py
├── plausibility_check.py
├── completeness_check.py
├── confidence.py    (scoring from check results)
└── retry.py         (fix-and-retry orchestration)
```

**3. Turn estimates: compress Phase 1.**
Scaffolding + deps + database + test infra can be ONE turn, not 4. We know exactly what goes in each file. Proposal:
- **Turn 2**: Full Phase 1 (scaffolding + models + database + tests) — both minds review
- **Turn 3**: Start scanner schema extraction
- This saves 2 turns (~$4-6 in API costs)

### Additional Proposals

**4. Add `storage.py` to root package.** The scanner→SQLite→router boundary needs an explicit storage interface. Not inside scanner/ — it's shared infrastructure. `storage.py` handles SQLite index read/write with a clean API both layers use.

**5. Define core interfaces NOW (Turn 2).** When we scaffold, models.py should include at minimum:
- `TableInfo`, `ColumnInfo`, `RelationshipInfo` (scanner output)
- `ScanResult` (the full scanner output that goes to storage)
- `RouteResult` (selected tables + reasoning)
- `CheckResult`, `VerificationResult` (verifier output)
- `QueryResult` (final output with SQL + confidence + verification)

This locks the contracts before implementation.

**@Kiran:** Agree on compressed Phase 1? And I want `storage.py` at root level — your call on whether scanner/router import it or it imports their models.

---

## Turn 2 — Mind A (Kiran) — 2026-04-05
**Phase:** CODING
**Tests:** PASSED 35/35

### Response to Arjun's Review + Phase 1 Implementation

**Devil's advocate before agreeing:** Defining all interfaces in Turn 2 risks over-specifying. We haven't written scanner logic yet — what if `ColumnProfile` needs fields we can't predict until we implement profiling? Counter: the models use Pydantic defaults liberally, adding fields later is non-breaking. The cost of missing a field is one edit; the cost of mismatched layer boundaries is a full refactor. **Agreed on all points.**

**Decisions locked:**
1. models.py flat, split at 300 lines — **done**, organized by layer with section headers
2. Verifier per-check files — **done**, `base.py` with `CheckProtocol` + `make_result` helper
3. Phase 1 compressed to single turn — **done**, this turn
4. `storage.py` at root — **done**, scanner writes via it, router reads via it. `storage.py` imports models (not vice versa), so dep direction preserved: models ← storage ← scanner/router
5. Core interfaces defined — all 10 models Arjun listed are in `models.py`

### What I Built (Phase 1 complete)
- `config.py` — PROJECT_NAME, all constants, `sanitize_connection_string()`
- `exceptions.py` — 12 typed exceptions, zero generic Exception
- `models.py` — 14 Pydantic models across all 3 layers (currently 133 lines)
- `database.py` — read-only engine factory with SQLAlchemy event listener blocking writes
- `storage.py` — SQLite-backed CRUD for ScanResult (save/load/list/delete)
- `verifier/base.py` — `CheckProtocol` (runtime_checkable) + `make_result` helper
- Package stubs: scanner/, router/, verifier/, api/
- `tests/` — conftest with fixtures, 4 test files, 35 tests covering models, database, storage, verifier base
- `requirements.txt` — pydantic==2.7.1, sqlalchemy==2.0.30, pytest==8.2.0, hypothesis==6.100.1

### storage.py design note
`StorageBackend` takes a directory path, creates SQLite there. Uses SQLAlchemy ORM internally (ScanRecord table). `save_scan()` does upsert by database_name. Clean interface — scanner and router only see `save_scan(ScanResult)` and `load_scan(name) -> ScanResult | None`.

**@Arjun:** Review the models and storage interface. Next turn: start scanner schema extraction. I suggest you own `scanner/schema.py` — pull table/column metadata from a SQLAlchemy engine via `inspect()`.

---

## Turn 3 — Mind B (Arjun) — 2026-04-05
**Phase:** CODING
**Tests:** PASSED 45/45

### Phase 1 Review

**Devil's advocate:** I could nitpick the storage.py session handling (no explicit rollback on exception paths — SQLAlchemy's context manager handles it, but explicit is better than implicit). Counter: the context manager `with session_factory() as session` already rolls back on unhandled exceptions. Adding explicit rollback is ceremony that obscures the actual flow. **Approved as-is.**

Code review notes — Phase 1 is solid:
- **models.py** (133 lines): Clean section headers, correct Pydantic v2 patterns, Field validators on bounds. Good.
- **database.py**: Write-blocking via `before_cursor_execute` event is the right approach. Catches first keyword — handles all dangerous operations.
- **storage.py**: Upsert pattern is clean. `model_dump_json()` / `model_validate_json()` round-trips preserve all fields. Dep direction correct: storage imports models, not vice versa.
- **config.py**: `sanitize_connection_string()` regex works. All constants centralized.
- One observation: `_block_writes` checks first word only. A CTE like `WITH x AS (DELETE...)` would slip through. Not urgent — we control all SQL generation — but worth a tech debt note.

### What I Built — `scanner/schema.py`

Schema extraction module using `sqlalchemy.inspect()`:
- `extract_schema(engine, schema=None) -> (list[TableInfo], list[RelationshipInfo])`
- Extracts: table names, columns with types, PKs, nullable flags, FKs with targets
- FK columns get `is_foreign_key=True` + `foreign_key_target` set
- Each FK also produces a `RelationshipInfo(type=DECLARED_FK, confidence=1.0)`
- Graceful degradation: tables that fail inspection are skipped with warning log
- Uses typed exceptions (`ScanError`) on failure

### Test Coverage — 10 new tests
`test_scanner_schema.py`: table discovery, column count, PK detection, nullable detection, FK detection, relationship extraction, data type strings, empty DB, schema default, integration with conftest fixture.

**@Kiran:** Next up: data sampling + statistical profiling (`scanner/profiler.py`). I suggest using `SELECT * FROM table TABLESAMPLE BERNOULLI(5)` for PostgreSQL and falling back to `SELECT * FROM table ORDER BY RANDOM() LIMIT N` for SQLite/others. Profiling produces `ColumnProfile` (null_fraction, distinct_count, sample_values, min/max). Your turn.

---

## Turn 4 — Mind A (Kiran) — 2026-04-05
**Phase:** CODING
**Tests:** PASSED 62/62

### Devil's Advocate on TABLESAMPLE

Arjun suggested `TABLESAMPLE BERNOULLI(5)` for PostgreSQL. **Counter-argument:** BERNOULLI sampling is non-deterministic and on small tables (<1000 rows) can return wildly varying sample sizes — sometimes zero rows. For profiling accuracy, we'd be better off with `SELECT * LIMIT N` on small tables. **However:** the profiler already has a `max_rows` cap and the BERNOULLI approach is O(1) on large tables (no sort needed), which matters for production DBs with millions of rows. The small-table edge case is handled by the fallback path. **Agreed with the approach.**

### What I Built — `scanner/profiler.py`

Data sampling + statistical profiling module:
- `profile_table(engine, table, sample_pct, max_rows) -> TableInfo` — profiles one table in-place
- `profile_tables(engine, tables, ...) -> list[TableInfo]` — batch with graceful skip on failure
- `_build_sample_query()` — dialect-aware: TABLESAMPLE BERNOULLI for PG, ORDER BY RANDOM() LIMIT N for SQLite/others
- `_profile_column()` — null_fraction, distinct_count, sample_values (capped at 10), min/max
- All SQL read-only SELECT. Quoted identifiers for reserved words. Uses `ProfilingError`.

### Test Coverage — 17 new tests
`test_scanner_profiler.py`: row count (2), sample query (2), column profiling (4), table profiling (6), batch profiling (2), integration (1).

**@Arjun:** Review profiler.py. Next: `scanner/relationships.py` — fuzzy name matching + value overlap discovery. I suggest you own it. Key question: name matching before or after profiling? I say after — we need sample_values for value overlap checks.

---
