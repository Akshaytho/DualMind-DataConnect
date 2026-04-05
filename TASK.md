# TASK: Build DataConnect (name changeable via config)

## What It Is
Open-source Python framework: connect to PostgreSQL, auto-understand structure, query in plain English, get verified confidence-scored SQL results.

## Architecture (3 Layers)
- Layer 1 Scanner: SQLAlchemy schema extraction, data profiling, relationship discovery (FK + fuzzy + value overlap + AI), semantic descriptions. Output: Summary Index in SQLite.
- Layer 2 Router: Pick 3-8 relevant tables via embeddings (local MiniLM) + graph walking (NetworkX) + AI cross-check. Target: 90%+ recall.
- Layer 3 Verifier: 6 deterministic checks (schema, join, aggregation, filter, plausibility, completeness). Fix-and-retry (max 3). Confidence: 90-100%=trusted, <50%=unverified.

## Tech Stack (future-proof, upgradable)
- LLM: litellm (provider-agnostic — Claude, GPT, Gemini, Llama, local). User brings own key + chooses model.
- DB: SQLAlchemy 2.0 (20+ DB types future). Embeddings: sentence-transformers MiniLM (local, free).
- Vector: FAISS. Graph: NetworkX. SQL parse: sqlparse. Storage: SQLite. API: FastAPI. Validation: pydantic v2.
- Testing: pytest. Deploy: Docker.

## Product Name Rule
Single constant in config.py: PRODUCT_NAME = "DataConnect". All references use this. Change name = change 1 line.

## Security Rules (non-negotiable, violating = blocking issue)
1. Never store user API keys on disk
2. Never log SQL results (PII risk)
3. READ-ONLY only (SELECT queries, reject INSERT/UPDATE/DELETE/DROP)
4. SQL injection prevention (parameterized via SQLAlchemy)
5. No eval()/exec()
6. Rate limiting on API
7. Input sanitization
8. DB credentials encrypted at rest if stored
9. No telemetry without opt-in
10. HTTPS-only in production

## Benchmark Database
BIRD benchmark (PostgreSQL version) — multiple DBs, known Q&A pairs, complex schemas.
Also test against eClean production DB as real-world case.

## User Experience
- User provides API key via env var DATACONNECT_API_KEY or per-request header X-API-Key
- User chooses model via config or per-request param
- CLI: pip install dataconnect → dataconnect connect → dataconnect ask "question"
- API: FastAPI /scan, /ask, /status

## Build Order
1. Config + models  2. Scanner  3. Router  4. Verifier  5. SQL Generator  6. CLI  7. API  8. Web UI  9. Benchmark  10. Docker + PyPI

## Targets
Query: 2-5s. Scan: 5-15min. Clean DB accuracy: 88-93%. Messy: 70-80%. High-conf correct: 95%. Hallucination: <2%.
