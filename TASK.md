# DataConnect — Verified Natural-Language SQL Engine

## IMPORTANT: Name Policy
The project is called "DataConnect" everywhere in code, docs, and conversation.
The name MUST be stored in exactly ONE place: `workspace/dataconnect/config.py → PROJECT_NAME = "DataConnect"`
If we ever need to change the name, we change that ONE constant and nothing else breaks.

## What We're Building
An open-source Python framework that connects to PostgreSQL, auto-learns the database structure, and lets anyone query it in plain English with VERIFIED, confidence-scored SQL results.

## Three-Layer Architecture

### Layer 1 — The Scanner (runs once per database)
- Connects via SQLAlchemy
- Extracts schema: tables, columns, types, keys, constraints
- Samples data via TABLESAMPLE BERNOULLI (O(1))
- Statistical profiling: nulls, uniques, distributions, patterns
- Relationship discovery: declared FKs + fuzzy name matching + value overlap + AI inference
- Semantic descriptions via user's chosen LLM
- Output: Summary Index stored in SQLite (~3K-6K tokens for 50-table DB)

### Layer 2 — The Router (runs per query)
- Picks 3-8 relevant tables per question using 3 methods:
  1. Semantic embedding match (sentence-transformers MiniLM-L6-v2, runs locally, FREE)
  2. Relationship chain walking (NetworkX, 2 levels deep)
  3. LLM cross-check (sends candidate list to user's chosen model)
- Any table matched by ANY method is included (maximize recall)

### Layer 3 — The Verifier (runs per query, 100% deterministic, NO LLM)
Six checks:
1. Schema Conformity — do referenced tables/columns exist?
2. Join Validation — do join columns exist, types match, relationships known?
3. Aggregation Validation — GROUP BY correctness, function-to-type mapping
4. Filter Validation — WHERE values against known data ranges/enums
5. Result Plausibility — row counts, value ranges, null percentages
6. Completeness Audit — potentially relevant tables not used?

Failed checks → fix-and-retry loop (max 3 attempts)
Confidence: 90-100% all pass, 70-89% warnings, 50-69% concerns shown, <50% marked unverified

## BYOK — Bring Your Own Key
Users provide their OWN API key and choose their model:
- Anthropic (Claude Sonnet/Opus)
- OpenAI (GPT-4o/o1)
- Google (Gemini)
- Local models (Ollama)
- Any OpenAI-compatible API

We NEVER store or transmit keys except to the user's chosen provider.
Key is passed at runtime, NEVER saved to disk unless user explicitly configures it.

## Tech Stack (Future-Proof)
- Python 3.11+ (type hints everywhere)
- SQLAlchemy 2.0 (supports 20+ database types for expansion)
- sentence-transformers (MiniLM-L6-v2, 80MB, local, free)
- FAISS (millisecond vector search)
- NetworkX (graph operations)
- sqlparse (AST-level SQL analysis)
- SQLite (metadata storage, zero-dependency)
- FastAPI (async API server)
- pydantic v2 (all data validation)
- pytest + hypothesis (testing + property-based testing)
- litellm (unified LLM interface — supports ALL providers via one API)

## Benchmark Database
Use BIRD benchmark (NeurIPS 2023): 95 databases, 12,751 question-SQL pairs, real-world messy data.
Specifically use BIRD Mini-Dev (500 examples) in PostgreSQL format for development testing.
This gives us VERIFIED correct answers to measure accuracy against.

## Accuracy Targets
- Execution accuracy (clean DB): 88-93%
- Execution accuracy (messy DB): 70-80%
- Confidence calibration: 95% of high-confidence results correct
- Router recall: 90%+
- Hallucination rate (high-conf): <2%
- Median latency: <4 seconds

## Interfaces (Phase 4)
1. CLI: `pip install dataconnect` → `dataconnect scan` → `dataconnect ask "question"`
2. REST API: FastAPI with OpenAPI docs
3. Web UI: Simple query interface

## What NOT to Build
- No user auth (users run it on their own infrastructure)
- No cloud hosting (self-hosted only)
- No data storage (we never see user data)
- No payment system
- No multi-tenant anything
