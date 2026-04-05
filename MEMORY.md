# Shared Memory — DataConnect

## Architecture (from spec)
- Layer 1: Scanner (schema + profiling + relationship discovery + semantic descriptions)
- Layer 2: Router (embeddings + graph + LLM cross-check)
- Layer 3: Verifier (6 deterministic checks + confidence scoring)

## Code Map
_Update as files are created:_
- (none yet)

## Tech Stack (locked)
- Python 3.11+, SQLAlchemy 2.0, sentence-transformers, FAISS, NetworkX
- sqlparse, SQLite, FastAPI, pydantic v2, litellm, pytest+hypothesis

## Key Decisions
- BYOK: users bring own API key via litellm
- Name in ONE place: config.py → PROJECT_NAME
- Read-only SQL only. No writes ever.
- BIRD benchmark for accuracy testing (Mini-Dev 500, PostgreSQL format)

## Security Rules (from CODING_RULES.md)
- No hardcoded keys. No telemetry. No eval(). Read-only SQL. Pin deps.
- See CODING_RULES.md for full list (30 rules)

## Patterns & Conventions
_Add as we build:_

## Known Bugs & Tech Debt
_Track here:_

## Additional Security (v3.1)
- [ ] FastAPI requires X-API-Key header on all endpoints
- [ ] Connection strings sanitized in logs (mask passwords)
- [ ] All deps pinned in requirements.txt
- [ ] Rate limiting: 60 queries/min per API key
- [ ] No secrets in git ever

## Bridge Protections (v3.1)
- 10-minute timeout per turn (kills if hung)
- Stuck detection: 2 retries then rollback + pause
- Git recovery: auto-clean dirty state
- Rate limit: 15 turns/hour max
