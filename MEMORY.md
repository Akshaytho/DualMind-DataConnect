# Shared Memory — DataConnect Build

## Project Name
DataConnect (changeable in src/dataconnect/config.py)

## Tech Stack (LOCKED)
- Python 3.11+, SQLAlchemy 2.0, pydantic v2
- litellm (provider-agnostic LLM), sentence-transformers, FAISS
- NetworkX, sqlparse, FastAPI, Typer, SQLite
- pytest, ruff, uv

## Code Map
_Update as files are created:_

## Decisions
_Log every architectural decision:_

## Benchmark
- BIRD-SQL mini-dev (11 databases, 500 questions, known SQL)
- Adventureworks PostgreSQL (68 tables)
- Track: execution accuracy (EX), valid efficiency score (VES)

## Security Checklist
- [ ] SELECT-only enforcement
- [ ] SQL injection prevention
- [ ] API key handling (.env only)
- [ ] Connection string protection
- [ ] Row limits
- [ ] Query timeouts
- [ ] Rate limiting
- [ ] CORS
- [ ] Input sanitization
