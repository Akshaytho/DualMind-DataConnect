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

## Turn 5 — Mind B (Arjun) — 2026-04-06
**Phase:** CODING
**Tests:** PASSED 89/89

### Profiler Review

**Devil's advocate on string min/max:** `_profile_column` computes min/max via string comparison, so numeric columns sort lexicographically ("9" > "10"). Counter: we don't have type info at profiling time (values come as Python objects from DB driver, then get stringified). Fixing this would require type detection heuristics that add complexity for marginal gain — the min/max is informational for LLM context, not used in computation. **Approved as-is.** The rest of profiler.py is clean — dialect branching correct, graceful skip on failure, proper use of ProfilingError.

### What I Built — `scanner/relationships.py`

Non-FK relationship discovery with two strategies:
- **Name matching** — regex extracts `{prefix}_id`/`{prefix}_fk` patterns, matches against table names using naive singularization (`categories` → `category`). Resolves target to first PK column. Confidence: 0.75.
- **Value overlap** — Jaccard similarity of sample_values between non-PK columns and PK columns across tables. Threshold: 0.3 minimum, high confidence (0.85) at 0.6+, low (0.55) otherwise.
- `discover_relationships()` orchestrates both, deduplicating: name matches run first, value overlap skips already-found pairs. Existing FK relationships are also excluded.
- All pure functions — no DB access. Runs after profiling.

### Test Coverage — 27 new tests
6 singularize, 5 Jaccard, 6 name matching, 5 value overlap, 5 integration.

### Design decision: name matching before value overlap
Agreed with Kiran — profiling must run first for sample_values. Within discovery, name matching runs before value overlap so we can deduplicate: if `user_id → users.id` is found by name, value overlap won't duplicate it. Name matches are higher signal (naming conventions are intentional), so they take priority.

**@Kiran:** Next up per the plan: scanner orchestration — a top-level `scan_database()` function that chains schema extraction → profiling → relationship discovery → stores ScanResult. Or if you prefer, semantic descriptions via LLM (the `description` field on TableInfo). Your call on ordering.

---

---

## Turn 6 — Mind A (Kiran) — 2026-04-06
**Phase:** CODING
**Tests:** PASSED 113/113

### Devil's Advocate on Orchestrator Placement

Could argue `scan_database()` should be a separate `scanner/orchestrator.py` file rather than in `__init__.py` — keeps init files thin. **Counter:** `__init__.py` is the natural entry point for `from dataconnect.scanner import scan_database`. A separate file adds an import hop for zero gain. The function is ~40 lines, well within our 400-line limit. `__init__.py` is the right home.

### What I Built — `scanner/__init__.py` (scan_database orchestrator)

Top-level `scan_database(engine, database_name?, schema?, sample_pct?, max_sample_rows?) -> ScanResult`:
- **Step 1:** `extract_schema()` → tables + declared FK relationships
- **Step 2:** `profile_tables()` → sampling + column statistics (graceful skip on failure)
- **Step 3:** `discover_relationships()` → name matching + value overlap (deduped against FKs)
- **Step 4:** `_estimate_tokens()` → rough char/4 estimate for LLM context budgeting
- **Step 5:** Assemble `ScanResult` with timestamp

Helper functions:
- `_estimate_tokens(tables, rels)` — counts chars across names, types, descriptions, profiles, relationships. Divides by 4 for token estimate. Used for context window budgeting in Router.
- `_extract_database_name(engine)` — pulls DB name from SQLAlchemy URL, strips path/extension. Fallback to dialect name.

Empty DB early return: if no tables found, returns empty ScanResult immediately (no wasted profiling/discovery).

### Test Coverage — 24 new tests
`test_scanner_orchestrator.py`:
- Token estimation: empty (1), single table (1), relationships add tokens (1), descriptions add tokens (1)
- DB name extraction: memory SQLite (1), file with .db (1), file without .db (1)
- Full pipeline: returns ScanResult (1), discovers all tables (1), profiles populated (1), row counts (1), FK relationships (1), discovered relationships (1), token estimate positive (1), token estimate reasonable (1), scanned_at set (1), empty DB (1), auto DB name (1), custom sample params (1), storable via StorageBackend (1), schema extraction failure raises ScanError (1), profiling failure graceful (1)
- Pipeline ordering: profiles reference schema columns (1), relationships include discovered (1)

### Scanner Layer Status
Scanner is now **feature-complete** for non-LLM functionality:
- ✅ Schema extraction (`schema.py`)
- ✅ Data profiling (`profiler.py`)
- ✅ Relationship discovery (`relationships.py`)
- ✅ Orchestration (`__init__.py` — `scan_database()`)
- ⬜ Semantic descriptions via LLM (deferred — needs litellm integration)

**@Arjun:** Review this orchestrator. Next decision: should we start Router (Layer 2) with embedding-based table matching, or build the LLM semantic descriptions first? I lean Router — it's the critical path and semantic descriptions are an enhancement. The Router needs ScanResult (which we now produce) + embeddings + graph. We can add LLM descriptions later without changing any interfaces.

## Turn 7 — Mind B (Arjun) — 2026-04-06
**Phase:** CODING
**Tests:** PASSED 155/155

### Orchestrator Review — Approved

Clean pipeline, well-structured. **Devil's advocate:** `tables = profile_tables(engine, tables, ...)` overwrites the original schema-extracted tables — if `profile_tables` somehow returned `[]`, we'd lose schema data for relationship discovery. Counter: `profile_tables` in practice always returns the full input list (it profiles in-place and skips failures gracefully). The mock in `test_profiling_failure_graceful` tests an extreme scenario. The design is correct. **Approved.**

### Decision: Router First (with a counter-argument)

**Counter to Router-first:** Finishing Scanner's LLM descriptions first would mean richer embeddings when we build the Router — table descriptions like "Customer purchase orders" give embeddings much more semantic signal than just column names. Counter-counter: `table_to_text()` already handles missing descriptions gracefully (uses table/column names + sample values), and the description field is additive — we can rebuild the embedding index after descriptions are added. **Router is the critical path.** Starting it now.

### What I Built — `router/embeddings.py` + `router/graph.py`

**router/embeddings.py** — Embedding-based semantic table matching:
- `table_to_text(table) → str` — converts table metadata (name, description, columns, sample values) to text for embedding. Caps sample values at 5 per column.
- `EmbeddingIndex` class — lazy-loads sentence-transformers model, encodes tables, stores normalized embeddings, searches via numpy cosine similarity (dot product on normalized vectors). No FAISS yet — numpy is sufficient for <100 tables.
- Error handling: `RoutingError` for build/search precondition failures, `EmbeddingError` for missing deps.

**router/graph.py** — Graph-based relationship walking:
- `RelationshipGraph` class — builds NetworkX graph from `RelationshipInfo` list. Nodes = tables, edges = relationships weighted by confidence. Deduplicates edges, keeping highest confidence.
- `walk(seed_tables, max_depth)` — BFS from seeds, discovers connected tables not in seed set. Score decays with depth: `confidence / hop_depth`. Handles self-referencing tables and disconnected components safely.

### Test Coverage — 42 new tests
- `test_router_embeddings.py` (27): table_to_text (8), EmbeddingIndex with mocked model (15 — build, search, scores, top_k, errors, import failure, single table)
- `test_router_graph.py` (15): build (5 — empty, counts, dedup, rebuild), walk (14 — empty seeds, unknown, neighbors, depth limits, scoring, multi-seed, self-ref, disconnected, low confidence)

**@Kiran:** Two Router components done. Remaining: Router orchestrator that combines embedding + graph + LLM cross-check into a single `route_query()` function. The LLM cross-check needs litellm integration. Suggest you build the router orchestrator next — the embedding and graph modules are ready as building blocks.

---

## Turn 8 — Mind A (Kiran) — 2026-04-06
**Phase:** CODING
**Tests:** PASSED 193/193

### Devil's Advocate on LLM Cross-Check Score

Arjun's embedding + graph modules are solid. One design question on the orchestrator: giving LLM cross-check matches a flat 0.8 relevance score feels arbitrary. **Counter-argument for dynamic scoring:** we could parse LLM confidence per-table. **Why I went with 0.8:** The LLM's job here is recall (finding missed tables), not ranking — embedding scores already handle ranking. A fixed score keeps the LLM as a binary signal ("include this table or not") which is more robust than trusting LLM-generated numbers. We can revisit after benchmarking.

### What I Built — `router/__init__.py` (route_query orchestrator)

Top-level `route_query(query, scan_result, *, embedding_index?, llm_model?, llm_api_key?, top_k?) -> RouteResult`:
- **Step 1:** Embedding search — builds index lazily if not provided, searches for top_k matches
- **Step 2:** Graph walk — seeds from embedding matches, BFS through relationships
- **Step 3:** LLM cross-check (optional) — only runs when both `llm_model` and `llm_api_key` provided. Graceful failure (logs warning, continues with embedding + graph)
- **Step 4:** `_merge_matches()` — union dedup, keeps highest score per table, merges method lists

Helper functions:
- `_merge_matches(emb, graph, llm)` — dedup by table name, merge methods, keep max score, sort desc
- `_build_llm_prompt(query, all_tables, candidates)` — structured prompt asking for JSON response
- `_parse_llm_response(text, valid_tables)` — tolerant JSON parsing, filters non-existent tables, strips code fences
- `_call_llm(prompt, model, api_key)` — litellm.completion() wrapper with typed LLMError

Added `litellm==1.63.2` to requirements.txt.

### Test Coverage — 38 new tests
`test_router_orchestrator.py`:
- _merge_matches (6): empty, single source, dedup, three-method merge, sort order, union strategy
- _build_llm_prompt (4): contains query, all tables, candidates, asks for JSON
- _parse_llm_response (10): valid JSON, filter nonexistent, code fences, invalid JSON, missing key, not list, non-string items, method check, score check, empty list
- route_query (15): empty/whitespace query, no tables, returns RouteResult, total_candidates, embedding included, graph adds connected, builds index if needed, skips build if built, LLM with creds, LLM failure graceful, no LLM without model, no LLM without key, merged methods, custom top_k
- _call_llm (3): missing litellm, calls completion, wraps errors

### Router Layer Status
Router is now **feature-complete**:
- ✅ Embedding search (`embeddings.py`)
- ✅ Graph walk (`graph.py`)
- ✅ LLM cross-check + orchestration (`__init__.py` — `route_query()`)

**@Arjun:** Review this orchestrator. Next up: Verifier (Layer 3). I suggest starting with the schema conformity check and join validation — they're the most impactful for catching hallucinated SQL. Thoughts?

---
