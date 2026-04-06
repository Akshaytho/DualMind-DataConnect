"""Verifier Check 3: Aggregation Validation.

Validates GROUP BY correctness and aggregate function-to-type mapping.
Checks that non-aggregated SELECT columns appear in GROUP BY, and that
aggregate functions (SUM, AVG, etc.) are applied to compatible types.
"""

from __future__ import annotations

import logging
import re
from typing import Any

from dataconnect.exceptions import VerificationError
from dataconnect.models import CheckResult, CheckStatus, ScanResult
from dataconnect.verifier.base import CheckProtocol, make_result
from dataconnect.verifier.schema_conformity import extract_table_aliases

logger = logging.getLogger(__name__)

# Aggregate functions recognized by the checker
_AGGREGATE_FUNCS = {
    "COUNT", "SUM", "AVG", "MIN", "MAX",
    "ARRAY_AGG", "STRING_AGG", "BOOL_AND", "BOOL_OR",
    "BIT_AND", "BIT_OR", "EVERY",
    "STDDEV", "STDDEV_POP", "STDDEV_SAMP",
    "VARIANCE", "VAR_POP", "VAR_SAMP",
}

# Aggregates that require numeric types
_NUMERIC_ONLY_FUNCS = {"SUM", "AVG", "STDDEV", "STDDEV_POP", "STDDEV_SAMP",
                       "VARIANCE", "VAR_POP", "VAR_SAMP"}

# Numeric SQL types
_NUMERIC_TYPES = {
    "integer", "int", "bigint", "smallint", "serial", "bigserial",
    "int4", "int8", "int2", "numeric", "decimal",
    "float", "double precision", "real", "double", "money",
}

# Pattern to detect aggregate function calls: FUNC(...)
_AGG_CALL_RE = re.compile(
    r"\b(" + "|".join(_AGGREGATE_FUNCS) + r")\s*\(",
    re.IGNORECASE,
)

# Pattern to extract SELECT clause (between SELECT [DISTINCT] and FROM)
_SELECT_CLAUSE_RE = re.compile(
    r"\bSELECT\b\s+(?:DISTINCT\s+)?(.*?)\bFROM\b",
    re.IGNORECASE | re.DOTALL,
)

# Pattern to extract GROUP BY clause
_GROUP_BY_RE = re.compile(
    r"\bGROUP\s+BY\b\s+(.*?)(?:\bHAVING\b|\bORDER\b|\bLIMIT\b|\bUNION\b|$)",
    re.IGNORECASE | re.DOTALL,
)

# Pattern to extract HAVING clause
_HAVING_RE = re.compile(
    r"\bHAVING\b\s+(.*?)(?:\bORDER\b|\bLIMIT\b|\bUNION\b|$)",
    re.IGNORECASE | re.DOTALL,
)

# Pattern for column references: optional_table.column or bare column
_COL_REF_RE = re.compile(r"\b([A-Za-z_]\w*(?:\.[A-Za-z_]\w*)?)\b")


class AggregationValidationCheck:
    """Check GROUP BY correctness and aggregate function type safety.

    Implements CheckProtocol.
    """

    @property
    def name(self) -> str:
        return "aggregation_validation"

    def run(self, sql: str, context: dict[str, Any]) -> CheckResult:
        """Validate aggregation logic in the SQL query.

        Checks:
        1. Non-aggregated SELECT columns must appear in GROUP BY
        2. Aggregate functions applied to compatible types
        3. HAVING references are valid aggregate expressions or GROUP BY cols

        Args:
            sql: SQL query to validate.
            context: Must contain 'scan_result' (ScanResult).

        Returns:
            CheckResult with status and details.
        """
        scan_result = _get_scan_result(context)
        type_lookup = _build_type_lookup(scan_result)
        aliases = extract_table_aliases(sql)

        has_aggregates = bool(_AGG_CALL_RE.search(sql))
        group_by_cols = _extract_group_by_columns(sql)
        has_group_by = len(group_by_cols) > 0

        # No aggregation at all — nothing to check
        if not has_aggregates and not has_group_by:
            return make_result(
                self.name,
                CheckStatus.PASSED,
                "No aggregation in query",
            )

        issues: list[str] = []
        warnings: list[str] = []

        # Check 1: GROUP BY completeness
        if has_aggregates or has_group_by:
            gb_issues = _check_group_by_completeness(sql, group_by_cols)
            issues.extend(gb_issues)

        # Check 2: Aggregate function type compatibility
        type_issues = _check_aggregate_types(
            sql, type_lookup, aliases,
        )
        warnings.extend(type_issues)

        # Check 3: HAVING clause references
        having_issues = _check_having_clause(sql, group_by_cols)
        warnings.extend(having_issues)

        if issues:
            return make_result(
                self.name,
                CheckStatus.FAILED,
                "; ".join(issues),
                issues=issues,
                warnings=warnings,
            )

        if warnings:
            return make_result(
                self.name,
                CheckStatus.WARNING,
                "; ".join(warnings),
                issues=[],
                warnings=warnings,
            )

        return make_result(
            self.name,
            CheckStatus.PASSED,
            "Aggregation logic is valid",
            issues=[],
            warnings=[],
        )


def _get_scan_result(context: dict[str, Any]) -> ScanResult:
    """Extract ScanResult from context dict."""
    scan_result = context.get("scan_result")
    if scan_result is None:
        raise VerificationError("context missing 'scan_result'")
    if not isinstance(scan_result, ScanResult):
        raise VerificationError("context['scan_result'] is not a ScanResult")
    return scan_result


def _build_type_lookup(
    scan_result: ScanResult,
) -> dict[str, dict[str, str]]:
    """Build table→{column→type} lookup (all lowercase keys)."""
    lookup: dict[str, dict[str, str]] = {}
    for table in scan_result.tables:
        cols: dict[str, str] = {}
        for col in table.columns:
            cols[col.name.lower()] = col.data_type.lower()
        lookup[table.name.lower()] = cols
    return lookup


def _extract_group_by_columns(sql: str) -> list[str]:
    """Extract column references from GROUP BY clause.

    Returns lowercase column references (bare or qualified).
    """
    match = _GROUP_BY_RE.search(sql)
    if not match:
        return []

    clause = match.group(1).strip()
    # Split by comma, strip each item
    cols: list[str] = []
    for item in clause.split(","):
        item = item.strip()
        if not item:
            continue
        # Skip numeric positional refs (GROUP BY 1, 2)
        if item.isdigit():
            cols.append(item)
            continue
        # Extract the column reference
        col_match = _COL_REF_RE.match(item)
        if col_match:
            cols.append(col_match.group(1).lower())
    return cols


def _extract_select_expressions(sql: str) -> list[str]:
    """Extract individual expressions from SELECT clause.

    Returns list of raw expression strings.
    """
    match = _SELECT_CLAUSE_RE.search(sql)
    if not match:
        return []

    clause = match.group(1).strip()
    if clause == "*":
        return ["*"]

    # Split by top-level commas (not inside parentheses)
    expressions: list[str] = []
    depth = 0
    current: list[str] = []
    for char in clause:
        if char == "(":
            depth += 1
            current.append(char)
        elif char == ")":
            depth -= 1
            current.append(char)
        elif char == "," and depth == 0:
            expressions.append("".join(current).strip())
            current = []
        else:
            current.append(char)
    if current:
        expressions.append("".join(current).strip())

    return expressions


def _is_aggregate_expression(expr: str) -> bool:
    """Check if expression contains an aggregate function call."""
    return bool(_AGG_CALL_RE.search(expr))


def _strip_alias(expr: str) -> str:
    """Remove trailing alias from expression (e.g., 'col AS alias' -> 'col')."""
    # Match AS alias at end
    alias_match = re.search(r"\bAS\s+\w+\s*$", expr, re.IGNORECASE)
    if alias_match:
        return expr[:alias_match.start()].strip()
    return expr


def _extract_column_ref(expr: str) -> str | None:
    """Extract a simple column reference from an expression.

    Returns lowercase ref or None if expression is complex.
    """
    expr = _strip_alias(expr).strip()
    col_match = re.fullmatch(r"([A-Za-z_]\w*(?:\.[A-Za-z_]\w*)?)", expr)
    if col_match:
        return col_match.group(1).lower()
    return None


def _check_group_by_completeness(
    sql: str,
    group_by_cols: list[str],
) -> list[str]:
    """Check that non-aggregated SELECT columns appear in GROUP BY.

    Returns list of issue descriptions.
    """
    select_exprs = _extract_select_expressions(sql)
    if not select_exprs or select_exprs == ["*"]:
        return []

    group_by_set = set(group_by_cols)
    issues: list[str] = []

    for expr in select_exprs:
        if _is_aggregate_expression(expr):
            continue

        col_ref = _extract_column_ref(expr)
        if col_ref is None:
            # Complex expression — skip (could be a constant, etc.)
            continue

        # Check if this column is in GROUP BY
        # Match both qualified (table.col) and bare (col) forms
        if col_ref not in group_by_set:
            # Also check if the bare column name matches
            bare = col_ref.split(".")[-1] if "." in col_ref else col_ref
            qualified_matches = any(
                g.split(".")[-1] == bare for g in group_by_set
                if not g.isdigit()
            )
            if not qualified_matches and bare not in group_by_set:
                issues.append(
                    f"Column '{col_ref}' in SELECT is not in GROUP BY "
                    f"and not aggregated"
                )

    return issues


def _resolve_column_type(
    col_ref: str,
    type_lookup: dict[str, dict[str, str]],
    aliases: dict[str, str],
) -> str | None:
    """Resolve the SQL type of a column reference.

    Handles both qualified (table.col) and bare column refs.
    Returns lowercase type string or None if not found.
    """
    if "." in col_ref:
        parts = col_ref.split(".", 1)
        table = aliases.get(parts[0].lower(), parts[0]).lower()
        col = parts[1].lower()
        table_cols = type_lookup.get(table)
        if table_cols:
            return table_cols.get(col)
    else:
        # Bare column — search all tables
        col = col_ref.lower()
        for table_cols in type_lookup.values():
            if col in table_cols:
                return table_cols[col]
    return None


# Pattern to extract aggregate calls with their arguments
_AGG_WITH_ARG_RE = re.compile(
    r"\b(" + "|".join(_AGGREGATE_FUNCS) + r")\s*\(\s*"
    r"(?:DISTINCT\s+)?"
    r"([^()]+?)\s*\)",
    re.IGNORECASE,
)


def _check_aggregate_types(
    sql: str,
    type_lookup: dict[str, dict[str, str]],
    aliases: dict[str, str],
) -> list[str]:
    """Check that aggregate functions are applied to compatible types.

    Returns list of warning descriptions.
    """
    warnings: list[str] = []

    for match in _AGG_WITH_ARG_RE.finditer(sql):
        func_name = match.group(1).upper()
        arg = match.group(2).strip()

        # Skip COUNT(*) and COUNT(1)
        if func_name == "COUNT":
            continue
        if arg == "*":
            continue

        # Only check numeric-only aggregates
        if func_name not in _NUMERIC_ONLY_FUNCS:
            continue

        # Resolve argument type
        col_type = _resolve_column_type(arg, type_lookup, aliases)
        if col_type is None:
            # Can't resolve — skip (might be an expression)
            continue

        if col_type not in _NUMERIC_TYPES:
            warnings.append(
                f"{func_name}({arg}) applied to non-numeric type '{col_type}'"
            )

    return warnings


def _check_having_clause(
    sql: str,
    group_by_cols: list[str],
) -> list[str]:
    """Validate HAVING clause references.

    Columns in HAVING must be either aggregated or in GROUP BY.
    Returns list of warning descriptions.
    """
    match = _HAVING_RE.search(sql)
    if not match:
        return []

    having_clause = match.group(1).strip()
    warnings: list[str] = []

    # Strip string literals so their content isn't treated as column refs
    no_strings = re.sub(r"'[^']*'", "''", having_clause)

    # Extract column references from HAVING that aren't inside aggregates
    # First, remove aggregate function calls
    cleaned = _AGG_CALL_RE.sub("AGG_FUNC(", no_strings)

    # Find column refs in what remains (outside aggregates)
    group_by_set = set(group_by_cols)
    for col_match in _COL_REF_RE.finditer(cleaned):
        ref = col_match.group(1).lower()
        # Skip SQL keywords and function names
        if ref.upper() in {"AND", "OR", "NOT", "AGG_FUNC", "BETWEEN",
                           "IN", "IS", "NULL", "LIKE", "TRUE", "FALSE"}:
            continue
        # Skip numeric literals
        if ref.isdigit():
            continue
        # Check against GROUP BY
        bare = ref.split(".")[-1] if "." in ref else ref
        if ref not in group_by_set and bare not in group_by_set:
            qualified_matches = any(
                g.split(".")[-1] == bare for g in group_by_set
                if not g.isdigit()
            )
            if not qualified_matches:
                warnings.append(
                    f"Column '{ref}' in HAVING is not in GROUP BY "
                    f"and not aggregated"
                )

    return warnings


# Ensure the class satisfies the protocol
assert isinstance(AggregationValidationCheck, type)
_check: CheckProtocol = AggregationValidationCheck()  # type: ignore[assignment]
del _check
