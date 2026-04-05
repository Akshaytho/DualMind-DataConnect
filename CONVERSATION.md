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
