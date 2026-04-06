"""Verifier Check 5: Result Plausibility.

Pre-execution plausibility check using profiled metadata.
Flags queries likely to return empty results, unexpectedly large
result sets, or cartesian products — all without running the query.
"""

from __future__ import annotations

import logging
import re
from typing import Any

from dataconnect.exceptions import VerificationError
from dataconnect.models import (
    CheckResult,
    CheckStatus,
    ScanResult,
    TableInfo,
)
from dataconnect.verifier.base import CheckProtocol, make_result
from dataconnect.verifier.schema_conformity import extract_table_aliases

logger = logging.getLogger(__name__)

# Tables with row_count_estimate above this are "large"
_LARGE_TABLE_THRESHOLD = 10_000

# Warn on SELECT * from tables above this row count
_SELECT_STAR_WARN_THRESHOLD = 1_000

# Max reasonable LIMIT value before we stop warning about unbounded queries
_REASONABLE_LIMIT = 10_000

# Pattern to detect SELECT *
_SELECT_STAR_RE = re.compile(
    r"\bSELECT\s+(?:DISTINCT\s+)?(\*)\s",
    re.IGNORECASE,
)

# Pattern to detect LIMIT clause
_LIMIT_RE = re.compile(
    r"\bLIMIT\s+(\d+)",
    re.IGNORECASE,
)

# Pattern to extract FROM clause tables (handles subqueries by skipping parens)
_FROM_RE = re.compile(
    r"\bFROM\s+((?:[^()\n;]|\([^)]*\))+?)(?:\bWHERE\b|\bGROUP\b|\bORDER\b|\bLIMIT\b|\bHAVING\b|\bUNION\b|$)",
    re.IGNORECASE | re.DOTALL,
)

# Pattern for JOIN clauses
_JOIN_RE = re.compile(
    r"\b(?:INNER\s+|LEFT\s+(?:OUTER\s+)?|RIGHT\s+(?:OUTER\s+)?|FULL\s+(?:OUTER\s+)?|CROSS\s+)?JOIN\b",
    re.IGNORECASE,
)

# Pattern to detect WHERE clause presence
_WHERE_RE = re.compile(r"\bWHERE\b", re.IGNORECASE)

# Pattern to detect aggregate functions (no row-level data returned)
_AGGREGATE_RE = re.compile(
    r"\b(?:COUNT|SUM|AVG|MIN|MAX|STDDEV|VARIANCE)\s*\(",
    re.IGNORECASE,
)

# Comma-separated tables in FROM (cartesian product detection)
_COMMA_TABLE_RE = re.compile(
    r"\b([A-Za-z_]\w*)\b",
    re.IGNORECASE,
)

# SQL keywords that are NOT table names in FROM clause
_SQL_KEYWORDS = {
    "select", "from", "where", "group", "order", "limit", "having",
    "union", "join", "inner", "left", "right", "full", "cross",
    "outer", "on", "and", "or", "not", "in", "between", "like",
    "is", "null", "as", "distinct", "all", "exists", "case",
    "when", "then", "else", "end", "asc", "desc", "by",
}


class ResultPlausibilityCheck:
    """Pre-execution plausibility check based on profile metadata.

    Implements CheckProtocol.
    """

    @property
    def name(self) -> str:
        return "result_plausibility"

    def run(self, sql: str, context: dict[str, Any]) -> CheckResult:
        """Check query plausibility against profiled metadata.

        Checks:
        1. Empty table detection — query references tables with 0 rows
        2. Unbounded large result — SELECT without LIMIT on large tables
        3. SELECT * on large tables — may return excessive data
        4. Cartesian product — comma-joined tables without WHERE
        5. High-null column selection — most selected columns are mostly null

        Args:
            sql: SQL query to validate.
            context: Must contain 'scan_result' (ScanResult).

        Returns:
            CheckResult with PASSED, WARNING, or FAILED status.
        """
        scan_result = _get_scan_result(context)
        aliases = extract_table_aliases(sql)
        table_lookup = _build_table_lookup(scan_result)

        warnings: list[str] = []
        failures: list[str] = []

        # Resolve actual table names referenced in the query
        referenced_tables = _resolve_referenced_tables(sql, aliases, table_lookup)

        # Check 1: Empty table detection
        failures.extend(
            _check_empty_tables(referenced_tables, table_lookup)
        )

        # Check 2: Unbounded large results
        warnings.extend(
            _check_unbounded_results(sql, referenced_tables, table_lookup)
        )

        # Check 3: SELECT * on large tables
        warnings.extend(
            _check_select_star(sql, referenced_tables, table_lookup)
        )

        # Check 4: Cartesian product detection
        warnings.extend(
            _check_cartesian_product(sql, aliases, table_lookup)
        )

        # Check 5: High-null column prevalence
        warnings.extend(
            _check_high_null_columns(sql, referenced_tables, table_lookup)
        )

        if failures:
            return make_result(
                self.name,
                CheckStatus.FAILED,
                "; ".join(failures),
                failures=failures,
                warnings=warnings,
            )

        if warnings:
            return make_result(
                self.name,
                CheckStatus.WARNING,
                "; ".join(warnings),
                warnings=warnings,
            )

        return make_result(
            self.name,
            CheckStatus.PASSED,
            "Query results appear plausible based on profiled metadata",
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


def _resolve_referenced_tables(
    sql: str,
    aliases: dict[str, str],
    table_lookup: dict[str, TableInfo],
) -> list[str]:
    """Get list of actual table names referenced in the query.

    Returns lowercase table names that exist in the scan result.
    """
    # Collect all table names from aliases (alias→table mappings)
    tables: set[str] = set()
    for real_table in aliases.values():
        tl = real_table.lower()
        if tl in table_lookup:
            tables.add(tl)

    # Also add any direct table references that might not be aliased
    # by scanning FROM clause for bare table names
    from_match = _FROM_RE.search(sql)
    if from_match:
        from_clause = from_match.group(1)
        for word_match in _COMMA_TABLE_RE.finditer(from_clause):
            word = word_match.group(1).lower()
            if word not in _SQL_KEYWORDS and word in table_lookup:
                tables.add(word)

    return sorted(tables)


def _check_empty_tables(
    referenced_tables: list[str],
    table_lookup: dict[str, TableInfo],
) -> list[str]:
    """Flag queries that reference tables with 0 estimated rows."""
    failures: list[str] = []
    for tname in referenced_tables:
        table = table_lookup.get(tname)
        if table and table.row_count_estimate == 0:
            failures.append(
                f"Table '{tname}' has 0 estimated rows — "
                f"query will return empty results"
            )
    return failures


def _check_unbounded_results(
    sql: str,
    referenced_tables: list[str],
    table_lookup: dict[str, TableInfo],
) -> list[str]:
    """Warn on queries without LIMIT on large tables (non-aggregate)."""
    warnings: list[str] = []

    # Aggregate queries naturally return few rows — skip
    if _AGGREGATE_RE.search(sql):
        return warnings

    has_limit = _LIMIT_RE.search(sql)
    has_where = _WHERE_RE.search(sql)

    if has_limit:
        return warnings

    # Check if any referenced table is large
    large_tables = []
    for tname in referenced_tables:
        table = table_lookup.get(tname)
        if table and table.row_count_estimate > _LARGE_TABLE_THRESHOLD:
            large_tables.append(
                f"{tname} (~{table.row_count_estimate:,} rows)"
            )

    if large_tables and not has_where:
        warnings.append(
            f"No LIMIT or WHERE on large table(s): "
            f"{', '.join(large_tables)}"
        )
    elif large_tables:
        # Has WHERE but no LIMIT — softer warning
        warnings.append(
            f"No LIMIT clause on query involving large table(s): "
            f"{', '.join(large_tables)} — consider adding LIMIT"
        )

    return warnings


def _check_select_star(
    sql: str,
    referenced_tables: list[str],
    table_lookup: dict[str, TableInfo],
) -> list[str]:
    """Warn on SELECT * from large tables."""
    warnings: list[str] = []

    if not _SELECT_STAR_RE.search(sql):
        return warnings

    # Check LIMIT — if present and reasonable, reduce severity
    limit_match = _LIMIT_RE.search(sql)
    if limit_match:
        limit_val = int(limit_match.group(1))
        if limit_val <= _REASONABLE_LIMIT:
            return warnings

    for tname in referenced_tables:
        table = table_lookup.get(tname)
        if table and table.row_count_estimate > _SELECT_STAR_WARN_THRESHOLD:
            col_count = len(table.columns)
            warnings.append(
                f"SELECT * on '{tname}' ({col_count} columns, "
                f"~{table.row_count_estimate:,} rows) — "
                f"consider selecting specific columns"
            )

    return warnings


def _extract_from_tables_raw(sql: str) -> list[str]:
    """Extract raw table names/aliases from FROM clause before JOINs.

    Returns table identifiers from the comma-separated FROM list.
    """
    from_match = _FROM_RE.search(sql)
    if not from_match:
        return []

    from_clause = from_match.group(1).strip()

    # Remove everything after first JOIN keyword
    join_pos = _JOIN_RE.search(from_clause)
    if join_pos:
        from_clause = from_clause[:join_pos.start()].strip()

    # Split by comma and extract table names
    tables: list[str] = []
    for part in from_clause.split(","):
        part = part.strip()
        if not part:
            continue
        # First word is the table name (rest is alias)
        words = part.split()
        if words:
            table_name = words[0].strip()
            if table_name.lower() not in _SQL_KEYWORDS:
                tables.append(table_name)

    return tables


def _check_cartesian_product(
    sql: str,
    aliases: dict[str, str],
    table_lookup: dict[str, TableInfo],
) -> list[str]:
    """Detect implicit cartesian products (comma-joined without WHERE)."""
    warnings: list[str] = []

    from_tables = _extract_from_tables_raw(sql)

    # Only flag if multiple tables in FROM (comma-separated, not JOINed)
    if len(from_tables) < 2:
        return warnings

    has_where = _WHERE_RE.search(sql)

    if not has_where:
        # Resolve to real table names for row count estimation
        real_names = []
        for t in from_tables:
            real = aliases.get(t.lower(), t).lower()
            if real in table_lookup:
                real_names.append(real)

        if len(real_names) >= 2:
            sizes = []
            for rn in real_names:
                tbl = table_lookup[rn]
                sizes.append(f"{rn} (~{tbl.row_count_estimate:,} rows)")

            warnings.append(
                f"Possible cartesian product: {', '.join(sizes)} "
                f"comma-joined without WHERE clause"
            )

    return warnings


def _check_high_null_columns(
    sql: str,
    referenced_tables: list[str],
    table_lookup: dict[str, TableInfo],
) -> list[str]:
    """Warn if query selects from tables where most profiled columns are >90% null."""
    warnings: list[str] = []

    # Skip aggregate-only queries (nulls handled by aggregate functions)
    if _AGGREGATE_RE.search(sql):
        return warnings

    # Only check for SELECT * (specific column selection is intentional)
    if not _SELECT_STAR_RE.search(sql):
        return warnings

    for tname in referenced_tables:
        table = table_lookup.get(tname)
        if not table or not table.profiles:
            continue

        high_null = [
            p.column_name for p in table.profiles
            if p.null_fraction > 0.9
        ]

        if len(high_null) > len(table.profiles) // 2 and len(high_null) >= 2:
            warnings.append(
                f"SELECT * on '{tname}' — {len(high_null)}/{len(table.profiles)} "
                f"profiled columns are >90% null "
                f"({', '.join(high_null[:3])}{'...' if len(high_null) > 3 else ''})"
            )

    return warnings


# Ensure the class satisfies the protocol
assert isinstance(ResultPlausibilityCheck, type)
_check: CheckProtocol = ResultPlausibilityCheck()  # type: ignore[assignment]
del _check
