"""SQL generation — LLM-based SQL query generation from natural language.

Takes a natural-language question plus router-selected tables and generates
a SQL SELECT query using the user's LLM via litellm.
"""

from __future__ import annotations

import logging
import re

from dataconnect.exceptions import GenerationError, LLMError
from dataconnect.models import RouteResult, ScanResult

logger = logging.getLogger(__name__)


def _build_table_context(
    scan_result: ScanResult,
    route_result: RouteResult,
) -> str:
    """Build table schema context for the generation prompt.

    Only includes tables selected by the router. Shows column names,
    types, keys, and relationships. Never includes sample data values
    (CODING_RULES rule 8).

    Args:
        scan_result: Full database schema from scanner.
        route_result: Tables selected by router.

    Returns:
        Formatted schema context string.
    """
    selected_names = {m.table_name for m in route_result.matched_tables}
    selected_tables = [
        t for t in scan_result.tables if t.name in selected_names
    ]

    lines: list[str] = []

    for table in selected_tables:
        lines.append(f"Table: {table.name}")
        if table.description:
            lines.append(f"  Description: {table.description}")
        lines.append("  Columns:")

        for col in table.columns:
            parts = [f"{col.name} {col.data_type}"]
            if col.is_primary_key:
                parts.append("PRIMARY KEY")
            if col.is_foreign_key and col.foreign_key_target:
                parts.append(f"FK -> {col.foreign_key_target}")
            if not col.nullable:
                parts.append("NOT NULL")
            lines.append("    " + " ".join(parts))

        lines.append("")

    # Relationship context for JOIN hints
    rel_lines: list[str] = []
    for rel in scan_result.relationships:
        if rel.source_table in selected_names or rel.target_table in selected_names:
            rel_lines.append(
                f"  {rel.source_table}.{rel.source_column} -> "
                f"{rel.target_table}.{rel.target_column} "
                f"({rel.relationship_type.value})"
            )

    if rel_lines:
        lines.append("Relationships:")
        lines.extend(rel_lines)

    return "\n".join(lines)


def _build_generation_prompt(question: str, table_context: str) -> str:
    """Build the LLM prompt for SQL generation.

    Args:
        question: Natural-language question.
        table_context: Formatted schema context.

    Returns:
        Complete prompt string.
    """
    return (
        "You are a SQL expert. Generate a SQL SELECT query to answer the "
        "following question using the database schema provided.\n\n"
        f"Question: {question}\n\n"
        f"Database schema:\n{table_context}\n\n"
        "Rules:\n"
        "- Use only SELECT statements (read-only)\n"
        "- Only reference tables and columns from the schema above\n"
        "- Use proper JOINs based on the relationships shown\n"
        "- Return ONLY the SQL query, no explanation, no markdown fences\n"
    )


def _extract_sql(response_text: str) -> str:
    """Extract SQL from LLM response, stripping markdown fences.

    Args:
        response_text: Raw LLM response.

    Returns:
        Cleaned SQL string.

    Raises:
        GenerationError: If response is empty or contains no SQL.
    """
    text = response_text.strip()
    if not text:
        raise GenerationError("LLM returned empty response for SQL generation")

    # Strip markdown code fences
    fence_match = re.search(
        r"```(?:sql)?\s*\n?(.*?)```",
        text,
        re.DOTALL | re.IGNORECASE,
    )
    if fence_match:
        text = fence_match.group(1).strip()

    if not text:
        raise GenerationError("No SQL found in LLM response")

    return text


def generate_sql(
    question: str,
    scan_result: ScanResult,
    route_result: RouteResult,
    *,
    model: str,
    api_key: str,
) -> str:
    """Generate a SQL query from a natural-language question.

    Uses the router's selected tables to build schema context,
    then calls the LLM to generate a SELECT query.

    Args:
        question: Natural-language question about the database.
        scan_result: Full schema from scanner.
        route_result: Tables selected by router.
        model: litellm model identifier (e.g. "gpt-4o").
        api_key: User's API key for the LLM provider.

    Returns:
        Generated SQL query string.

    Raises:
        GenerationError: If question is empty.
        LLMError: If LLM call fails.
    """
    if not question or not question.strip():
        raise GenerationError("Question must not be empty")

    if not route_result.matched_tables:
        raise GenerationError("No tables selected by router — cannot generate SQL")

    table_context = _build_table_context(scan_result, route_result)
    prompt = _build_generation_prompt(question, table_context)

    logger.info(
        "Generating SQL for question with %d relevant tables",
        len(route_result.matched_tables),
    )

    try:
        import litellm
    except ImportError as exc:
        raise LLMError(
            "litellm required for SQL generation. "
            "Install with: pip install litellm"
        ) from exc

    try:
        response = litellm.completion(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            api_key=api_key,
            temperature=0.0,
            max_tokens=2048,
        )
        raw = response.choices[0].message.content  # type: ignore[union-attr]
    except LLMError:
        raise
    except Exception as exc:
        raise LLMError(f"SQL generation LLM call failed: {exc}") from exc

    sql = _extract_sql(raw)
    logger.info("Generated SQL: %d characters", len(sql))
    return sql
