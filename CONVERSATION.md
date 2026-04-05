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
