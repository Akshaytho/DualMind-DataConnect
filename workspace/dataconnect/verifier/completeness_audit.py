"""Verifier Check 6: Completeness Audit.

Flags potentially relevant tables that are NOT used in the SQL query.
Uses relationship graph (FK and discovered relationships) to find
tables one hop from referenced tables that might contain useful data.
"""

from __future__ import annotations

import logging
import re
from typing import Any

from dataconnect.exceptions import VerificationError
from dataconnect.models import (
    CheckResult,
    CheckStatus,
    RelationshipInfo,
    ScanResult,
    TableInfo,
)
from dataconnect.verifier.base import CheckProtocol, make_result
from dataconnect.verifier.schema_conformity import extract_table_aliases

logger = logging.getLogger(__name__)

# Only flag tables with at least this many rows (skip empty/tiny lookup tables)
_MIN_ROW_RELEVANCE = 1

# Relationship confidence threshold — only consider strong relationships
_MIN_RELATIONSHIP_CONFIDENCE = 0.5

# Max neighbor tables to report (avoid noise on highly-connected schemas)
_MAX_REPORTED_TABLES = 5

# Pattern to extract FROM clause for table identification
_FROM_RE = re.compile(
    r"\bFROM\s+((?:[^()\n;]|\([^)]*\))+?)"
    r"(?:\bWHERE\b|\bGROUP\b|\bORDER\b|\bLIMIT\b|\bHAVING\b|\bUNION\b|$)",
    re.IGNORECASE | re.DOTALL,
)

# Join pattern
_JOIN_TABLE_RE = re.compile(
    r"\bJOIN\s+([A-Za-z_]\w*)",
    re.IGNORECASE,
)

# SQL keywords (not table names)
_SQL_KEYWORDS = {
    "select", "from", "where", "group", "order", "limit", "having",
    "union", "join", "inner", "left", "right", "full", "cross",
    "outer", "on", "and", "or", "not", "in", "between", "like",
    "is", "null", "as", "distinct", "all", "exists", "case",
    "when", "then", "else", "end", "asc", "desc", "by",
}

# Word pattern for table name extraction
_WORD_RE = re.compile(r"\b([A-Za-z_]\w*)\b")


class CompletenessAuditCheck:
    """Check 6: flag unused but potentially relevant tables.

    Implements CheckProtocol.
    """

    @property
    def name(self) -> str:
        return "completeness_audit"

    def run(self, sql: str, context: dict[str, Any]) -> CheckResult:
        """Audit query completeness by finding unused related tables.

        Identifies tables connected via relationships to query tables
        that are not referenced in the SQL, which may contain relevant
        data the LLM forgot to include.

        Args:
            sql: SQL query to validate.
            context: Must contain 'scan_result' (ScanResult).
                Optional 'route_result' with matched_tables for
                cross-referencing router suggestions.

        Returns:
            CheckResult with PASSED or WARNING status.
        """
        scan_result = _get_scan_result(context)
        table_lookup = _build_table_lookup(scan_result)
        aliases = extract_table_aliases(sql)

        # Get tables actually used in the query
        used_tables = _extract_used_tables(sql, aliases, table_lookup)

        if not used_tables:
            return make_result(
                self.name,
                CheckStatus.PASSED,
                "No recognized tables in query — skipping audit",
            )

        # Build adjacency from relationships
        adjacency = _build_adjacency(scan_result.relationships)

        # Find neighbor tables not used in the query
        missing = _find_missing_neighbors(
            used_tables, adjacency, table_lookup
        )

        # Cross-reference with router suggestions if available
        router_missed = _check_router_suggestions(
            used_tables, context
        )

        warnings: list[str] = []

        if missing:
            table_list = ", ".join(
                f"'{t}'" for t in missing[:_MAX_REPORTED_TABLES]
            )
            warnings.append(
                f"Related table(s) not used in query: {table_list}"
                f" — connected via relationships to tables in your query"
            )

        if router_missed:
            missed_list = ", ".join(
                f"'{t}'" for t in router_missed[:_MAX_REPORTED_TABLES]
            )
            warnings.append(
                f"Router-suggested table(s) not used: {missed_list}"
            )

        if warnings:
            return make_result(
                self.name,
                CheckStatus.WARNING,
                "; ".join(warnings),
                missing_related=missing[:_MAX_REPORTED_TABLES],
                router_missed=router_missed[:_MAX_REPORTED_TABLES],
                used_tables=sorted(used_tables),
            )

        return make_result(
            self.name,
            CheckStatus.PASSED,
            "All related tables accounted for",
            used_tables=sorted(used_tables),
        )


def _get_scan_result(context: dict[str, Any]) -> ScanResult:
    """Extract ScanResult from context dict."""
    scan_result = context.get("scan_result")
    if scan_result is None:
        raise VerificationError("context missing 'scan_result'")
    if not isinstance(scan_result, ScanResult):
        raise VerificationError("context['scan_result'] is not a ScanResult")
    return scan_result


def _build_table_lookup(scan_result: ScanResult) -> dict[str, TableInfo]:
    """Build table_name→TableInfo lookup (lowercase keys)."""
    return {t.name.lower(): t for t in scan_result.tables}


def _extract_used_tables(
    sql: str,
    aliases: dict[str, str],
    table_lookup: dict[str, TableInfo],
) -> set[str]:
    """Get set of real table names used in the SQL query (lowercase)."""
    tables: set[str] = set()

    # From alias mapping
    for real_table in aliases.values():
        tl = real_table.lower()
        if tl in table_lookup:
            tables.add(tl)

    # From FROM clause direct references
    from_match = _FROM_RE.search(sql)
    if from_match:
        from_clause = from_match.group(1)
        for word_match in _WORD_RE.finditer(from_clause):
            word = word_match.group(1).lower()
            if word not in _SQL_KEYWORDS and word in table_lookup:
                tables.add(word)

    # From JOIN clauses
    for join_match in _JOIN_TABLE_RE.finditer(sql):
        tname = join_match.group(1).lower()
        if tname in table_lookup:
            tables.add(tname)

    return tables


def _build_adjacency(
    relationships: list[RelationshipInfo],
) -> dict[str, set[str]]:
    """Build bidirectional adjacency map from relationships.

    Only includes relationships above confidence threshold.
    """
    adj: dict[str, set[str]] = {}
    for rel in relationships:
        if rel.confidence < _MIN_RELATIONSHIP_CONFIDENCE:
            continue
        src = rel.source_table.lower()
        tgt = rel.target_table.lower()
        adj.setdefault(src, set()).add(tgt)
        adj.setdefault(tgt, set()).add(src)
    return adj


def _find_missing_neighbors(
    used_tables: set[str],
    adjacency: dict[str, set[str]],
    table_lookup: dict[str, TableInfo],
) -> list[str]:
    """Find tables one hop from used tables that are not in the query.

    Filters out tiny/empty tables to reduce noise.
    Returns sorted list for deterministic output.
    """
    neighbors: set[str] = set()
    for t in used_tables:
        for neighbor in adjacency.get(t, set()):
            if neighbor not in used_tables:
                # Check the table has meaningful data
                info = table_lookup.get(neighbor)
                if info and info.row_count_estimate >= _MIN_ROW_RELEVANCE:
                    neighbors.add(neighbor)

    return sorted(neighbors)


def _check_router_suggestions(
    used_tables: set[str],
    context: dict[str, Any],
) -> list[str]:
    """Check if router-suggested tables are missing from the SQL.

    Returns list of table names the router suggested but the SQL
    doesn't use. Only runs if 'route_result' is in context.
    """
    route_result = context.get("route_result")
    if route_result is None:
        return []

    # Accept RouteResult or a dict with matched_tables
    matched_tables: list[Any] = []
    if hasattr(route_result, "matched_tables"):
        matched_tables = route_result.matched_tables
    elif isinstance(route_result, dict):
        matched_tables = route_result.get("matched_tables", [])

    missed: list[str] = []
    for match in matched_tables:
        # Support both TableMatch objects and dicts
        if hasattr(match, "table_name"):
            tname = match.table_name.lower()
        elif isinstance(match, dict):
            tname = match.get("table_name", "").lower()
        else:
            continue

        if tname and tname not in used_tables:
            missed.append(tname)

    return sorted(missed)


# Ensure the class satisfies the protocol
assert isinstance(CompletenessAuditCheck, type)
_check: CheckProtocol = CompletenessAuditCheck()  # type: ignore[assignment]
del _check
