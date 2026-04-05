# DataConnect Spec Reference (from original DataBridge PDF)

## Performance Targets
- Execution accuracy (clean DB): 88-93%
- Execution accuracy (messy DB): 70-80%
- Confidence calibration: 95% of high-confidence results correct
- Router recall: 90%+
- Hallucination rate (high-conf): <2%
- Median latency (P50): <4 seconds
- P95 latency: <8 seconds
- Graceful failure rate: 80%+
- Initial scan: 5-15 minutes (one-time)
- Per-query AI cost: ~$0.005

## Scanner Details
- SQLAlchemy connection
- O(1) sampling via TABLESAMPLE BERNOULLI
- Statistical profiling: nulls, uniques, distributions, patterns
- Relationship discovery: declared FKs, fuzzy name match, value overlap, AI inference
- Output: Summary Index in SQLite (~3-6K tokens for 50-table DB)

## Router Details
- Semantic embeddings: sentence-transformers/all-MiniLM-L6-v2 (local, free)
- Cosine similarity threshold: 0.35
- Graph walking: NetworkX, 2 levels deep from matched tables
- AI cross-check as final validation
- Any table selected by ANY method is included (maximize recall)

## Verifier Details (6 checks, 100% deterministic)
1. Schema Conformity — tables/columns exist
2. Join Validation — columns exist, types match, relationships known
3. Aggregation Validation — GROUP BY correct, function-to-type mapping
4. Filter Validation — WHERE values in known ranges/enums
5. Result Plausibility — row counts, value ranges, null percentages
6. Completeness Audit — flags relevant tables not used

## Confidence Scoring
- 90-100%: all checks passed
- 70-89%: minor warnings
- 50-69%: significant concerns (displayed prominently)
- <50%: marked unverified
- Principle: under-state confidence on correct > overstate on wrong

## Fix-and-Retry
- Max 3 attempts
- Targeted error feedback to AI after each failure
- If all 3 fail, return best attempt with low confidence + explanation
