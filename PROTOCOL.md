# DualMind Protocol — DataConnect Build

## Rules
1. Turn-based. Check STATUS.json before acting.
2. Max 1 module per turn. Don't try to build everything.
3. Write tests FIRST, then implementation.
4. Run `cd workspace && python -m pytest -x -q` before EVERY push.
5. NEVER push failing tests.
6. Read MEMORY.md every turn.
7. Max 2 debate rounds per topic. Then decide and code.
8. Keep responses under 60 lines.
9. Update MEMORY.md with decisions and code map changes.

## Message Format
```
## Turn [N] — [Mind A (Kiran) / Mind B (Arjun)] — [timestamp]
**Phase:** PLANNING | CODING | REVIEWING | TESTING
**Tests:** PASSED X/X | NO CHANGES

[Message]

---
```
