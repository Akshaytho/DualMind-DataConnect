# Coding Rules — EVERY commit must follow these

## Security (non-negotiable)
1. NEVER hardcode API keys, tokens, or credentials anywhere
2. NEVER log user queries, results, or database content
3. ALL SQL is read-only (SELECT only). No INSERT/UPDATE/DELETE/DROP ever
4. SQLAlchemy connections use READ-ONLY mode where supported
5. User API keys exist ONLY in memory at runtime. Never written to disk unless user configures it
6. No telemetry, no analytics, no phone-home. Zero data leaves the user's machine
7. Sanitize ALL user input before passing to SQL or LLM
8. Never include database content in error messages sent to LLM
9. Pin ALL dependency versions in pyproject.toml
10. No eval(), no exec(), no dynamic code execution from user input

## Code Quality
11. Type hints on EVERY function signature
12. Pydantic models for ALL data structures (no raw dicts crossing module boundaries)
13. Every public function has a docstring
14. No function longer than 50 lines — split it
15. No file longer than 400 lines — split it
16. Tests for every module. Target: 90%+ coverage
17. Use `pytest` + `hypothesis` for property-based testing where applicable
18. No print() in production code — use `logging` module
19. All errors are typed exceptions, not generic Exception
20. No TODO/FIXME without a linked issue number

## Architecture
21. Name is stored in ONE place: config.py → PROJECT_NAME
22. LLM provider is abstracted through litellm — never import anthropic/openai directly
23. Every layer (Scanner, Router, Verifier) is a standalone module with clear interface
24. No circular imports. Dependency direction: models ← scanner ← router ← verifier ← cli
25. Database connection details never cross module boundaries — pass SQLAlchemy engine only

## Git Discipline
26. One logical change per commit
27. Commit message format: "[Mind A/B] Turn N: what changed"
28. Never force push
29. Run ALL tests before committing. Failing tests = blocked commit
30. No secrets in git history. If accidentally committed, rotate immediately
