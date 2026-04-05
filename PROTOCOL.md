# DualMind Protocol v3 — DataConnect Build

## Per-Turn Checklist (MANDATORY)
1. Read: STATUS.json, MEMORY.md, last 3 turns of CONVERSATION.md
2. Read: CODING_RULES.md (first turn only, then when writing code)
3. Read ONLY code files relevant to current task (check MEMORY.md code map)
4. Do your work
5. If you wrote code: `cd workspace && python -m pytest -x -q` — ALL MUST PASS
6. Append turn to CONVERSATION.md (under 60 lines)
7. Update STATUS.json (flip turn, increment number)
8. Update MEMORY.md if decisions/patterns/bugs found
9. `git add -A && git commit -m "[Mind A/B] Turn N: desc" && git push origin main`

## Rules
- Turn-based. Only act if STATUS.json says it's your turn
- Append only. Never edit the other mind's messages
- Test before push. Failing tests = DO NOT COMMIT
- Keep responses under 60 lines
- Argue with evidence (code, tests, benchmarks)
- 3-round max on disagreements, then prototype both
- Read CODING_RULES.md before writing ANY code
- Devil's advocate: before agreeing, state the strongest counter-argument

## Message Format
```
## Turn [N] — [Mind A (Kiran) / Mind B (Arjun)] — [timestamp]
**Phase:** PLANNING | CODING | REVIEWING | TESTING
**Tests:** PASSED X/X | NO CODE CHANGES

[Message under 60 lines]

---
```

## Additional Quality Gates
- Pin ALL dependencies with exact versions in requirements.txt (e.g., pydantic==2.7.1, not pydantic>=2.0)
- Never log connection strings containing passwords — sanitize before logging
- Every HTTP endpoint MUST have API key authentication
- All DB queries MUST go through SQLAlchemy parameterized queries — never raw string SQL
- If you add a new file, update MEMORY.md code map immediately
- Before pushing, verify git status is clean — no untracked secrets or temp files
