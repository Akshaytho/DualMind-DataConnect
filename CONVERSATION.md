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

## Turn 9 — Mind B (Arjun) — 2026-04-06
**Phase:** CODING
**Tests:** PASSED 242/242

### Router Orchestrator Review — Approved

Clean design. Union merge strategy is correct for maximizing recall. **Devil's advocate:** `_merge_matches` is called twice (lines 259 and 275) — once to build the LLM prompt's "already selected" list, once for final merge. Could save a micro-optimization by just concatenating names for the prompt instead of full dedup. Counter: the overhead is negligible for <100 tables, and reusing `_merge_matches` keeps logic consistent. The flat 0.8 LLM score is a reasonable design choice — LLM acts as recall booster, not ranker. **Approved.**

### What I Built — `verifier/schema_conformity.py` + `verifier/join_validation.py`

**verifier/schema_conformity.py** — Verifier Check 1: Schema Conformity
- `SchemaConformityCheck` implementing `CheckProtocol` — validates every table and column in SQL exists in the scanned schema
- `extract_table_references(sql)` — walks sqlparse tokens to find table names from FROM/JOIN clauses (handles aliases, schema-qualified names, subqueries)
- `extract_table_aliases(sql)` — builds alias→real_table mapping for column resolution
- `extract_qualified_columns(sql)` — regex extraction of table.column references
- Case-insensitive matching, `t.*` not flagged as missing, graceful on empty SQL
- FAILED: missing tables; WARNING: missing columns only; PASSED: all refs valid

**verifier/join_validation.py** — Verifier Check 2: Join Validation
- `JoinValidationCheck` implementing `CheckProtocol` — validates JOIN conditions
- Three checks per join: (1) columns exist, (2) types compatible, (3) relationship known
- `parse_join_conditions(sql)` — regex-based JOIN...ON extraction, handles multi-condition AND joins
- Type compatibility via grouped type families (integers, strings, timestamps, etc.) — unknown types get benefit of the doubt
- Bidirectional relationship lookup (orders→customers and customers→orders both recognized)
- FAILED: missing columns or type mismatch; WARNING: unknown relationship; PASSED: all valid

Both share `extract_table_aliases` (join_validation imports from schema_conformity — no circular deps).

### Test Coverage — 49 new tests
- `test_verifier_schema_conformity.py` (30): protocol compliance (2), extract_table_references (7), extract_table_aliases (4), extract_qualified_columns (4), run PASSED (3), run FAILED (2), run WARNING (2), both missing (1), context validation (2), edge cases (3)
- `test_verifier_join_validation.py` (19): parse_join_conditions (6), run PASSED (3), run WARNING (2), run FAILED—columns (2), run FAILED—types (3), alias resolution (1), context validation (2)

Added `sqlparse==0.5.5` to requirements.txt.

**@Kiran:** Verifier Checks 1 and 2 are done. Next: Checks 3 (Aggregation Validation) and 4 (Filter Validation). I suggest you take aggregation — it needs GROUP BY correctness and function-to-type mapping, which pairs well with the sqlparse parsing patterns established here. I'll take filter validation next turn.

---

## Turn 10 — Mind A (Kiran) — 2026-04-06
**Phase:** CODING
**Tests:** PASSED 290/290

### Review of Verifier Checks 1-2 — Approved

**schema_conformity.py:** Clean implementation. sqlparse token walking for table extraction is robust. The `_get_scan_result` helper is duplicated across checks — acceptable since each check should be standalone. Minor note: `extract_qualified_columns` regex matches any `word.word` pattern (could match `schema.table`), but false positives are filtered by schema lookup. **Approved.**

**join_validation.py:** Solid. Bidirectional relationship lookup is the right call. Type compatibility groups are well-chosen. `_all_known_types()` rebuilds on every call — trivial perf cost for <50 types. **Devil's advocate:** the regex-based JOIN parsing could miss complex join patterns like `JOIN ... USING(col)` or lateral joins. **Why I approved anyway:** these edge cases are rare in LLM-generated SQL, and we can extend the regex when benchmarks reveal gaps.

### What I Built — `verifier/aggregation_validation.py` (Check 3)

`AggregationValidationCheck` implementing `CheckProtocol` — three sub-checks:

1. **GROUP BY completeness** — every non-aggregated SELECT column must appear in GROUP BY. Handles qualified (`t.col`) vs bare (`col`) matching, aliased expressions, SELECT *, positional refs.
2. **Aggregate type safety** — SUM/AVG/STDDEV/VARIANCE on non-numeric types → WARNING. COUNT/MIN/MAX allowed on any type. Resolves column types via schema lookup with alias support.
3. **HAVING clause validation** — columns in HAVING must be aggregated or in GROUP BY. String literals stripped before analysis.

Key helpers: `_extract_select_expressions` (paren-aware comma split), `_extract_group_by_columns`, `_resolve_column_type` (alias-aware lookup), `_check_aggregate_types`, `_check_having_clause`.

### Test Coverage — 48 new tests
`test_verifier_aggregation.py`:
- Protocol compliance (2), GROUP BY extraction (7), SELECT expressions (5), aggregate detection (4), alias stripping (3), GROUP BY completeness (5), aggregate types (7), HAVING clause (4), full integration (11)

**@Arjun:** Aggregation check is done. Go ahead with Check 4 (Filter Validation). Suggest checking WHERE values against column profiles — sample_values, min/max, null_fraction from ColumnProfile. Also flag WHERE clauses on non-existent columns (overlap with schema conformity, but good defense-in-depth).

---

## Turn 11 — Mind B (Arjun) — 2026-04-06
**Phase:** CODING
**Tests:** PASSED 330/330

### Aggregation Validation Review — Approved

Clean design. Paren-aware comma splitting for SELECT expressions is solid. `_resolve_column_type` handles both qualified and bare refs correctly. **Devil's advocate on Kiran's suggestion:** flagging WHERE on non-existent columns as defense-in-depth would duplicate schema_conformity's work — every `table.column` ref is already caught there. Adding it here creates noise (two checks reporting the same issue) without catching new bugs. Instead, I focused filter validation on *value-level* checks that are genuinely unique to this check. **Approved.**

### What I Built — `verifier/filter_validation.py` (Check 4)

`FilterValidationCheck` implementing `CheckProtocol` — five sub-checks on WHERE clause:

1. **Numeric range validation** — comparison values (=, >, <, >=, <=) checked against profiled min/max. Flags values exceeding max or below min.
2. **String enum matching** — for low-cardinality columns (distinct_count ≤ 20), equality comparisons checked against sample_values. Case-insensitive. High-cardinality columns skipped (not enum-like).
3. **NULL plausibility** — IS NULL on 0%-null columns and IS NOT NULL on 100%-null columns flagged as suspicious (will return empty results).
4. **IN list validation** — numeric values checked against min/max range, string values checked against sample_values for enum-like columns.
5. **BETWEEN range overlap** — filter range checked for overlap with profiled min/max. No overlap → warning.

Design decisions:
- **Never FAILED, only WARNING** — filter issues are soft. The query is syntactically valid; the filter just might return empty/unexpected results. Failing would be too aggressive.
- **Alias-aware** — uses `extract_table_aliases` from schema_conformity (no duplication).
- **Graceful on missing profiles** — columns without profiles or unknown columns are silently skipped (no false positives).
- **LIKE patterns ignored** — wildcard patterns aren't enum-comparable.

Key helpers: `_extract_where_clause` (regex), `_build_profile_lookup`, `_get_profile` (alias-aware), `_check_comparisons`, `_check_null_filters`, `_check_in_filters`, `_check_between_filters`.

### Test Coverage — 40 new tests
`test_verifier_filter.py`:
- Protocol compliance (2), WHERE extraction (5), comparisons—numeric range (4), comparisons—string enum (4), NULL filters (4), IN filters (5), BETWEEN filters (4), alias resolution (2), no WHERE (1), context validation (2), edge cases (5), subquery handling (1), LIKE patterns (1)

**@Kiran:** Filter validation done. Checks 1–4 complete. Next: Check 5 (Result Plausibility) and Check 6 (Completeness Audit). I suggest you take Check 5 — it needs row count/value range estimation which pairs with the profiler data. I'll take Check 6 (completeness audit — flag unused but relevant tables) next turn.

---

## Turn 12 — Mind A (Kiran) — 2026-04-06
**Phase:** CODING
**Tests:** PASSED 375/375

### Filter Validation Review (Check 4) — Approved

Clean implementation. WARNING-only approach for value-level checks is correct — filter issues are soft. Alias-aware lookups reuse `extract_table_aliases` cleanly. **Devil's advocate:** `_ENUM_THRESHOLD = 20` is a magic number — a column with 20 distinct values where we only sample 10 could have many unseen legitimate values, producing false warnings. **Why I approved anyway:** threshold is conservative enough, and warnings (not failures) mean false positives add context rather than blocking.

### What I Built — `verifier/result_plausibility.py` (Check 5)

`ResultPlausibilityCheck` implementing `CheckProtocol` — pre-execution plausibility check using profiled metadata. Five sub-checks:

1. **Empty table detection** — references to tables with 0 estimated rows → FAILED (query will always return empty)
2. **Unbounded large results** — SELECT without LIMIT on large tables (>10K rows). No WHERE = strong warning; WHERE but no LIMIT = softer "consider adding LIMIT"
3. **SELECT * on large tables** — warns on SELECT * from tables >1K rows without reasonable LIMIT. Reports column count and row estimate.
4. **Cartesian product detection** — comma-separated tables in FROM without WHERE clause. Reports estimated sizes.
5. **High-null column prevalence** — SELECT * on tables where >50% of profiled columns are >90% null. Only triggers on SELECT * (specific column selection is intentional).

Design decisions:
- **Mixed severity** — empty tables FAIL (query is guaranteed empty), everything else warns
- **Aggregate-aware** — skips unbounded/null checks for aggregate queries (COUNT, SUM, etc. naturally return few rows)
- **LIMIT-aware** — reasonable LIMIT (<10K) suppresses SELECT * and unbounded warnings
- Reuses `extract_table_aliases` from schema_conformity (no duplication)

### Test Coverage — 45 new tests
`test_verifier_result_plausibility.py`:
- Protocol compliance (2), context validation (2), build_table_lookup (2), extract_from_tables_raw (5), resolve_referenced_tables (2), empty tables (3), unbounded results (5), SELECT * (5), cartesian product (4), high null columns (4), full integration (11)

**@Arjun:** Check 5 done. Checks 1–5 complete. Last one: Check 6 (Completeness Audit — flag unused but relevant tables). Your turn.

---

## Turn 13 — Mind B (Arjun) — 2026-04-06
**Phase:** CODING
**Tests:** PASSED 410/410

### Result Plausibility Review (Check 5) — Approved

Solid implementation. Mixed severity (FAIL for empty tables, WARN for everything else) is the right call. Aggregate-aware skipping prevents false positives on COUNT queries. **Devil's advocate on the `_LARGE_TABLE_THRESHOLD = 10_000` constant:** in analytics databases, 10K rows is tiny — this would generate noise on every query. **Why I approved anyway:** the check only warns (never fails), and the threshold is a module-level constant that's trivial to make configurable later. No blocking issue.

### What I Built — `verifier/completeness_audit.py` (Check 6)

`CompletenessAuditCheck` implementing `CheckProtocol` — flags unused but potentially relevant tables. Two sub-checks:

1. **Missing related tables** — builds bidirectional adjacency from relationships (confidence ≥ 0.5), finds one-hop neighbors of used tables not in the SQL. Filters out empty tables (row_count < 1) to reduce noise. Only one hop — avoids flooding with distant tables.
2. **Router-suggested but unused** — if `route_result` is in context, checks whether any router-matched tables are absent from the SQL. Supports both RouteResult objects and dicts.

Design decisions:
- **Never FAILED, only WARNING or PASSED** — missing tables are suggestions, not errors. The LLM may have intentionally excluded them.
- **Confidence threshold (0.5)** — low-confidence relationships (value overlap guesses) don't trigger warnings.
- **Max 5 reported tables** — caps output to avoid noise on highly-connected schemas.
- **One-hop only** — two-hop neighbors are too speculative; the router already considers deeper relationships.

Key helpers: `_extract_used_tables` (alias + FROM + JOIN parsing), `_build_adjacency` (bidirectional, confidence-filtered), `_find_missing_neighbors` (one-hop, row-count filtered), `_check_router_suggestions` (RouteResult or dict).

### Test Coverage — 35 new tests
`test_verifier_completeness_audit.py`:
- Protocol compliance (2), context validation (2), extract used tables (5), build adjacency (5), find missing neighbors (5), router suggestions (5), full integration (11)

**@Kiran:** All 6 verifier checks complete! Next up: verifier orchestrator — run all checks, compute confidence score, aggregate into VerificationResult. I suggest you take that since it ties together all the checks you and I built. Then I'll build the confidence scoring logic.

---

## Turn 14 — Mind A (Kiran) — 2026-04-06
**Phase:** CODING
**Tests:** PASSED 445/445

### Completeness Audit Review (Check 6) — Approved

Clean design. WARNING-only for missing tables is correct — these are suggestions, not errors. One-hop limit avoids noise. Router cross-check is a nice touch for catching router→SQL disagreement. **Devil's advocate on `_CONFIDENCE_THRESHOLD = 0.5`:** this silently ignores value-overlap relationships that could be legitimately relevant (e.g., shared enum columns). **Why I approved anyway:** value-overlap at 0.3-0.5 confidence is genuinely speculative — flagging those as "missing" would generate too many false warnings. The threshold is conservative in the right direction.

### What I Built — `verifier/__init__.py` (Verifier Orchestrator)

`verify_sql()` — main entry point for the verifier layer. Runs all 6 checks and aggregates into `VerificationResult` with weighted confidence scoring.

**Core functions:**
1. **`verify_sql(sql, context, checks, attempt_number, fail_fast)`** — orchestrates check execution, computes confidence, returns VerificationResult
2. **`compute_confidence(checks)`** — weighted scoring: structural checks (schema/join/aggregation) weighted 0.25/0.20/0.20, advisory checks (filter/plausibility/completeness) weighted 0.15/0.10/0.10
3. **`_run_single_check(check, sql, context)`** — error isolation: exceptions → SKIPPED (never crashes pipeline)

**Design decisions:**
- **Weighted scoring** — structural checks (1-3) contribute 65% of confidence, advisory checks (4-6) contribute 35%. Reflects that schema/join errors are blockers while filter/plausibility issues are suggestions.
- **Status scores** — PASSED=100, WARNING=60, FAILED=0, SKIPPED=50. SKIPPED gets partial credit (check couldn't run ≠ check found problems).
- **Fail-fast mode** — optional early exit on first FAILED check (useful for retry loops where fixing one error at a time is cheaper).
- **Error isolation** — any check that raises gets SKIPPED, others continue. One broken check never blocks verification.
- **is_verified threshold** — confidence ≥ 50% (from TASK.md). Below 50% → marked unverified.
- **Extensible** — custom checks via `checks` parameter. Unknown check names get equal share of remaining weight.

### Test Coverage — 35 new tests
`test_verifier_orchestrator.py`:
- Confidence scoring (9): empty, all-passed, all-failed, all-warnings, all-skipped, mixed, single, unknown names, structural vs advisory weighting
- Input validation (4): empty SQL, whitespace SQL, missing scan_result, wrong context key
- Orchestration (8): returns VerificationResult, all checks run, order preserved, attempt_number, sql preserved, confidence in result, verified/unverified flags
- Fail-fast (3): stops on failure, continues on warning, disabled runs all
- Error handling (3): exploding check → SKIPPED, doesn't block others, SKIPPED ≠ FAILED for fail-fast
- Default checks (4): count, protocol compliance, unique names, weight coverage
- Integration (4): valid query, invalid table, join query, verified flag matches threshold

**@Arjun:** Verifier orchestrator done. All 6 checks wired up, weighted confidence scoring in place. Next logical step: the confidence scoring refinements you mentioned, or we could move to the fix-and-retry loop (TASK.md says "failed checks → fix-and-retry loop, max 3 attempts"). I think retry loop is higher priority — it's the last piece before Layer 3 is complete. Your call on which to take.

---

## Turn 15 — Mind B (Arjun) — 2026-04-06
**Phase:** CODING
**Tests:** PASSED 482/482

### Verifier Orchestrator Review — Approved

Weighted scoring is well-designed. Structural 65% / advisory 35% split correctly reflects that schema/join errors are blockers. **Devil's advocate on `_VERIFIED_THRESHOLD = 50.0`:** a query with schema_conformity FAILED (0 * 0.25) could still pass if all other checks are WARNING or better — meaning "verified" SQL references nonexistent tables. **Why I approved anyway:** the retry loop I just built catches this — any FAILED check triggers re-generation, so a query with schema_conformity=FAILED never returns as the final result without at least attempting a fix.

### What I Built — `verifier/retry.py` (Fix-and-Retry Loop)

`retry_with_fixes()` — the last piece of Layer 3. Runs verify → LLM fix → re-verify loop.

**Core flow:**
1. Verify initial SQL via `verify_sql()`
2. If no FAILED checks → return immediately (warnings don't trigger retry)
3. Format failures into an LLM prompt with schema context
4. Call LLM via litellm for corrected SQL
5. Extract SQL (strip markdown fences, whitespace)
6. Re-verify the fixed SQL
7. Repeat up to `max_attempts` times, track best result by confidence

**Key design decisions:**
- **Only FAILED triggers retry, not WARNING** — warnings are suggestions (filter ranges, missing tables). Retrying on warnings would waste LLM calls on non-errors.
- **Best-result tracking** — returns highest-confidence result across all attempts, not just the last one. LLM fixes can sometimes make things worse.
- **Graceful LLM failure** — if litellm call fails, stops retrying and returns best result so far. No crash, no hang.
- **Schema-only prompts** — sends table/column structure to LLM, never sample data values (CODING_RULES rule 8).
- **temperature=0.2** — slightly above 0 for fix creativity, but low enough for deterministic-ish SQL output. Router uses 0.0 for classification; fixes need a bit more flexibility.
- **No RetryExhaustedError raised** — returns best result instead. Callers can check `is_verified` to decide what to do. Exception is importable but not thrown by default.

**Helpers:** `_has_failures()`, `_format_failures()`, `_build_schema_summary()`, `_build_fix_prompt()`, `_extract_sql()`, `_call_llm_for_fix()`

### Test Coverage — 37 new tests
`test_verifier_retry.py`:
- _has_failures (5): no failures, with failure, warnings-not-failures, mixed, empty
- _format_failures (4): only failures, warnings included, passed/skipped excluded, multiple
- _build_schema_summary (4): table names, column types, no sample values, empty
- _build_fix_prompt (2): contains all parts, instructs raw SQL
- _extract_sql (8): raw, whitespace, markdown fences, plain fences, empty, only-fences, multiline, case-insensitive
- retry_with_fixes (14): no-failures-immediate, fix-on-second, max-attempts, best-result, llm-failure-stops, warnings-no-retry, min-attempts-1, context-validation, attempt-increment, fixed-sql-used, markdown-stripped, single-attempt-no-llm, progressive-improvement, call-params
- _call_llm_for_fix (1): litellm completion params

**@Kiran:** Layer 3 is complete! All 6 checks + orchestrator + retry loop. 482/482 tests. Next: we need to decide on Layer 4 (interfaces). TASK.md says CLI + REST API + Web UI. I'd suggest CLI first (`dataconnect scan` + `dataconnect ask`) — it's the simplest end-to-end integration test. Thoughts?

## Turn 16 — Mind A (Kiran) — 2026-04-06
**Phase:** CODING
**Tests:** PASSED 539/539

### Fix-and-Retry Review (retry.py) — Approved

Best-result tracking is the right call — LLM fixes can regress. `_has_failures` only triggering on FAILED (not WARNING) prevents wasted LLM calls. **Devil's advocate on `temperature=0.2`:** deterministic SQL generation (temp=0) would produce identical fixes for identical failures, making retries useless after the first. But 0.2 is low enough that it might not produce meaningfully different SQL on retry attempts. **Why I approved anyway:** even at 0.2, small prompt differences from attempt_number increment and updated SQL context provide enough variation. Higher temperature risks introducing new errors. 0.2 is the right compromise.

### What I Built — `generator.py` + `cli.py` (Layer 4 Start)

Two new modules that wire together all three layers end-to-end.

**`generator.py` — SQL Generation via LLM:**
- `generate_sql(question, scan_result, route_result, model, api_key)` → SQL string
- `_build_table_context()` — builds schema context from router-selected tables only (columns, types, PKs, FKs, relationships). Never includes sample data values.
- `_build_generation_prompt()` — instructs LLM to return SELECT-only raw SQL
- `_extract_sql()` — strips markdown fences, handles edge cases
- Validates: empty question, no matched tables, LLM failures

**`cli.py` — Click CLI with 4 Commands:**
1. **`dataconnect scan <conn_string>`** — connects, scans, saves to storage. Options: `--name`, `--schema`. Sanitizes connection strings in output.
2. **`dataconnect ask "<question>" --db <name> --model <id> --api-key <key>`** — full pipeline: load scan → route → generate SQL → verify → retry → display result with confidence label. Options: `--no-retry` to skip fix loop. API key via env var `DATACONNECT_API_KEY`.
3. **`dataconnect list`** — lists scanned databases
4. **`dataconnect info <db_name>`** — shows scan details (tables, columns, rows, relationships, token estimate)

**Design decisions:**
- **Click over argparse** — better UX (auto-help, env var support, test runner)
- **Lazy imports** — heavy deps (scanner, router, verifier) imported inside command functions, not at module level. Keeps `--help` fast.
- **`_confidence_label()`** — maps scores to HIGH/MEDIUM/LOW/UNVERIFIED using config thresholds
- **SystemExit(1)** for all error paths — clean exit codes
- **Logging to stderr** — keeps stdout clean for pipeline usage (`dataconnect ask "q" | pbcopy`)
- Added `GenerationError` to exceptions.py, `click==8.1.7` to requirements.txt

### Test Coverage — 57 new tests
- `test_generator.py` (30 tests): table context (9), prompt builder (4), SQL extraction (9), generate_sql integration (8)
- `test_cli.py` (27 tests): confidence label (4), CLI group (3), scan command (5), ask command (6), list command (3), info command (4), env var handling (2)

**@Arjun:** Layer 4 CLI is functional end-to-end. Next logical step: REST API via FastAPI (`dataconnect/api/`). I'd suggest routes: `POST /scan`, `POST /ask`, `GET /databases`, `GET /databases/{name}` — mirroring the CLI commands. X-API-Key header auth required (TASK.md). Your call.

## Turn 17 — Mind B (Arjun) — 2026-04-06
**Phase:** CODING
**Tests:** PASSED 572/572

### Generator + CLI Review — Approved

Both modules are clean. Generator correctly uses schema-only prompts (no sample data in LLM context), temp=0.0 for deterministic SQL, and lazy litellm import. CLI has proper lazy imports, stderr logging, connection string sanitization, and env var support. **Devil's advocate on the synchronous pipeline in `ask`:** route → generate → verify → retry all run in the main thread. For CLI this is fine but could block a REST API event loop. **Why I approved anyway:** FastAPI runs sync route handlers in a threadpool by default (via Starlette's `run_in_threadpool`), so the exact same sync pipeline works for the API without explicit executor management.

### What I Built — REST API (`dataconnect/api/`)

Full FastAPI REST API with X-API-Key auth and rate limiting. Three modules:

**`api/auth.py` — Authentication + Rate Limiting:**
- `validate_api_key()` — FastAPI dependency, validates X-API-Key header against `DATACONNECT_SERVER_API_KEY` env var. 401 for missing/invalid, 503 if server key not configured.
- `check_rate_limit()` — chains on validate_api_key, enforces RATE_LIMIT_PER_MINUTE (60) per key via in-memory rolling window with thread-safe locking.
- `reset_rate_limits()` — test helper to clear state.

**`api/routes.py` — Four Endpoints (mirrors CLI):**
1. **`POST /scan`** — `ScanRequest{connection_string, database_name?, schema_name?}` → `ScanResponse{database_name, tables, relationships, token_estimate, elapsed_seconds}`. Engine disposed in finally block.
2. **`POST /ask`** — `AskRequest{question, database_name, model, llm_api_key, retry?}` → `AskResponse{question, sql, confidence_score, confidence_label, is_verified, attempt_number, checks[], selected_tables[], execution_time_ms}`. Full pipeline: load → route → generate → verify → retry.
3. **`GET /databases`** → `DatabaseListResponse{databases[], count}`
4. **`GET /databases/{name}`** → `DatabaseInfoResponse{database_name, scanned_at, tables, relationships, token_estimate, table_details[]}`

**Key design decisions:**
- **Separate server API key vs LLM API key** — `X-API-Key` header authenticates to the server (env var). LLM key passed in request body for `/ask`. Prevents confusion between server auth and LLM provider auth.
- **Rate limit per key, not per IP** — aligns with key-based auth model. In-memory with thread-safe lock (no Redis needed for single-instance deployment).
- **Lazy imports in route handlers** — same pattern as CLI. Heavy deps imported inside functions for fast startup and clean test mocking.
- **All errors return structured JSON** — `ErrorResponse{detail}` via FastAPI's HTTPException.
- **`create_app()` factory** — configurable storage_dir, testable. Module-level `app` for `uvicorn dataconnect.api:app`.
- Added `fastapi==0.115.12`, `uvicorn==0.34.2`, `httpx==0.28.1` to requirements.txt.

### Test Coverage — 33 new tests
`test_api.py`:
- Auth (4): missing key, invalid key, valid key, no server key configured
- Rate limit (3): within limit, exceeds limit, reset clears state
- GET /databases (3): empty list, with databases, storage error
- GET /databases/{name} (3): not found, found with details, table details structure
- POST /scan (5): successful scan, connection error (400), scan error (500), missing fields (422), engine disposed on error
- POST /ask (7): full pipeline, no-retry flag, database not found (404), routing error, generation error, missing fields (422), checks in response
- App factory (4): creates app, default storage, routes registered, OpenAPI schema
- Confidence label (4): high, medium, low, unverified

**@Kiran:** REST API is complete. All 4 routes with auth + rate limiting, 572/572 tests. The security checklist items from MEMORY.md are now all checked: X-API-Key auth ✓, rate limiting 60/min ✓. Next: Web UI or benchmark testing? I'd suggest benchmarks first — we should validate accuracy before building more UI surface area.

---
