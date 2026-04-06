"""Router layer — table selection via embeddings, graph, and LLM cross-check.

The router's job: given a natural-language query and a ScanResult, identify
which 3-8 tables are needed to answer the question. Three methods run and
their results are merged (union — any table matched by ANY method is included
to maximize recall).
"""

from __future__ import annotations

import json
import logging
from typing import Any

from dataconnect.config import MAX_RELEVANT_TABLES
from dataconnect.exceptions import LLMError, RoutingError
from dataconnect.models import (
    MatchMethod,
    RouteResult,
    ScanResult,
    TableMatch,
)
from dataconnect.router.embeddings import EmbeddingIndex
from dataconnect.router.graph import RelationshipGraph

logger = logging.getLogger(__name__)


def _merge_matches(
    embedding_matches: list[TableMatch],
    graph_matches: list[TableMatch],
    llm_matches: list[TableMatch],
) -> list[TableMatch]:
    """Merge matches from all methods into a single deduplicated list.

    Tables matched by multiple methods get all methods listed and keep
    the highest relevance score across methods.

    Args:
        embedding_matches: Tables from semantic embedding search.
        graph_matches: Tables from relationship graph walk.
        llm_matches: Tables from LLM cross-check.

    Returns:
        Merged, deduplicated list sorted by relevance score descending.
    """
    merged: dict[str, TableMatch] = {}

    for match in [*embedding_matches, *graph_matches, *llm_matches]:
        if match.table_name in merged:
            existing = merged[match.table_name]
            # Merge methods (deduplicate)
            new_methods = list(set(existing.methods) | set(match.methods))
            # Keep highest score
            best_score = max(existing.relevance_score, match.relevance_score)
            # Combine reasoning
            reasons = {existing.reasoning, match.reasoning}
            merged[match.table_name] = TableMatch(
                table_name=match.table_name,
                methods=new_methods,
                relevance_score=best_score,
                reasoning=" | ".join(sorted(reasons)),
            )
        else:
            merged[match.table_name] = match

    return sorted(merged.values(), key=lambda m: -m.relevance_score)


def _build_llm_prompt(
    query: str,
    table_names: list[str],
    candidate_names: list[str],
) -> str:
    """Build prompt for LLM cross-check.

    Asks the LLM to select which tables from the full list are needed,
    given the candidates already identified by embedding + graph methods.

    Args:
        query: The user's natural-language question.
        table_names: All table names in the database.
        candidate_names: Tables already selected by embedding + graph.

    Returns:
        Prompt string for the LLM.
    """
    return (
        "You are a database expert. Given a natural-language question and a "
        "list of database tables, select which tables are needed to answer "
        "the question.\n\n"
        f"Question: {query}\n\n"
        f"All tables in database: {', '.join(table_names)}\n\n"
        f"Already selected by other methods: {', '.join(candidate_names)}\n\n"
        "Return ONLY a JSON object with:\n"
        '- "tables": list of table names needed (include any from the '
        "already-selected list that are correct, and add any missing ones)\n"
        '- "reasoning": brief explanation of why each table is needed\n\n'
        "Respond with ONLY valid JSON, no other text."
    )


def _parse_llm_response(
    response_text: str,
    valid_tables: set[str],
) -> list[TableMatch]:
    """Parse LLM response into TableMatch list.

    Tolerant of minor formatting issues. Only includes tables that
    actually exist in the database.

    Args:
        response_text: Raw text from LLM completion.
        valid_tables: Set of table names that exist in the database.

    Returns:
        List of TableMatch from LLM cross-check.

    Raises:
        LLMError: If response cannot be parsed.
    """
    text = response_text.strip()
    # Strip markdown code fences if present
    if text.startswith("```"):
        lines = text.split("\n")
        lines = [l for l in lines if not l.startswith("```")]
        text = "\n".join(lines)

    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        raise LLMError(f"LLM returned invalid JSON: {exc}") from exc

    if not isinstance(data, dict) or "tables" not in data:
        raise LLMError("LLM response missing 'tables' key")

    raw_tables = data["tables"]
    if not isinstance(raw_tables, list):
        raise LLMError("LLM 'tables' field is not a list")

    reasoning = data.get("reasoning", "Selected by LLM cross-check")

    matches: list[TableMatch] = []
    for name in raw_tables:
        if not isinstance(name, str):
            continue
        if name in valid_tables:
            matches.append(TableMatch(
                table_name=name,
                methods=[MatchMethod.LLM_CROSSCHECK],
                relevance_score=0.8,
                reasoning=str(reasoning) if isinstance(reasoning, str)
                else "LLM cross-check",
            ))

    return matches


def _call_llm(
    prompt: str,
    model: str,
    api_key: str,
) -> str:
    """Call LLM via litellm for cross-check.

    Args:
        prompt: The cross-check prompt.
        model: litellm model identifier (e.g. "gpt-4o", "claude-sonnet-4-20250514").
        api_key: User's API key for the provider.

    Returns:
        Raw text response from the LLM.

    Raises:
        LLMError: If litellm is not installed or call fails.
    """
    try:
        import litellm  # noqa: F811
    except ImportError as exc:
        raise LLMError(
            "litellm required for LLM cross-check. "
            "Install with: pip install litellm"
        ) from exc

    try:
        response = litellm.completion(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            api_key=api_key,
            temperature=0.0,
            max_tokens=1024,
        )
        return response.choices[0].message.content  # type: ignore[union-attr]
    except Exception as exc:
        raise LLMError(f"LLM call failed: {exc}") from exc


def route_query(
    query: str,
    scan_result: ScanResult,
    *,
    embedding_index: EmbeddingIndex | None = None,
    llm_model: str | None = None,
    llm_api_key: str | None = None,
    top_k: int = MAX_RELEVANT_TABLES,
) -> RouteResult:
    """Route a natural-language query to relevant tables.

    Runs three methods and merges results (union strategy):
    1. Semantic embedding search (always runs)
    2. Relationship graph walk from embedding seeds (always runs)
    3. LLM cross-check (only if llm_model and llm_api_key provided)

    Args:
        query: Natural-language question about the database.
        scan_result: Output from scanner (tables + relationships).
        embedding_index: Pre-built index (built fresh if None).
        llm_model: litellm model ID for cross-check (optional).
        llm_api_key: API key for LLM provider (optional).
        top_k: Max tables from embedding search.

    Returns:
        RouteResult with matched tables and metadata.

    Raises:
        RoutingError: If scan_result has no tables.
    """
    if not query or not query.strip():
        raise RoutingError("Query must not be empty")

    if not scan_result.tables:
        raise RoutingError("Cannot route query: scan result has no tables")

    total_tables = len(scan_result.tables)

    # Step 1: Embedding search
    if embedding_index is None:
        embedding_index = EmbeddingIndex()
    if not embedding_index.is_built:
        embedding_index.build(scan_result.tables)

    embedding_matches = embedding_index.search(query, top_k=top_k)
    logger.info(
        "Embedding search: %d matches for query", len(embedding_matches),
    )

    # Step 2: Graph walk from embedding seeds
    seed_tables = [m.table_name for m in embedding_matches]
    graph = RelationshipGraph()
    graph.build(scan_result.relationships)
    graph_matches = graph.walk(seed_tables)
    logger.info("Graph walk: %d additional tables", len(graph_matches))

    # Step 3: LLM cross-check (optional)
    llm_matches: list[TableMatch] = []
    if llm_model and llm_api_key:
        all_table_names = [t.name for t in scan_result.tables]
        # Candidates so far (before LLM)
        pre_llm = _merge_matches(embedding_matches, graph_matches, [])
        candidate_names = [m.table_name for m in pre_llm]

        prompt = _build_llm_prompt(query, all_table_names, candidate_names)
        try:
            response_text = _call_llm(prompt, llm_model, llm_api_key)
            valid_tables = {t.name for t in scan_result.tables}
            llm_matches = _parse_llm_response(response_text, valid_tables)
            logger.info("LLM cross-check: %d tables", len(llm_matches))
        except LLMError:
            logger.warning(
                "LLM cross-check failed, continuing with embedding + graph",
                exc_info=True,
            )

    # Merge all results
    final_matches = _merge_matches(
        embedding_matches, graph_matches, llm_matches,
    )

    return RouteResult(
        query=query,
        matched_tables=final_matches,
        total_candidates=total_tables,
    )
