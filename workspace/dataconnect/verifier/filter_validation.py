"""Verifier Check 4: Filter Validation.

Validates WHERE clause filter values against column profile data
(sample_values, min/max, null_fraction) from the scanner.
Catches out-of-range numerics, impossible NULL filters, and
suspicious enum mismatches.
"""

from __future__ import annotations

import logging
import re
from typing import Any

from dataconnect.exceptions import VerificationError
from dataconnect.models import (
    CheckResult,
    CheckStatus,
    ColumnProfile,
    ScanResult,
    TableInfo,
)
from dataconnect.verifier.base import CheckProtocol, make_result
from dataconnect.verifier.schema_conformity import extract_table_aliases

logger = logging.getLogger(__name__)

# Pattern to extract WHERE clause (between WHERE and GROUP BY/ORDER BY/LIMIT/HAVING/UNION/end)
_WHERE_RE = re.compile(
    r"\bWHERE\b\s+(.*?)(?:\bGROUP\b|\bORDER\b|\bLIMIT\b|\bHAVING\b|\bUNION\b|$)",
    re.IGNORECASE | re.DOTALL,
)

# Pattern for simple comparisons: column = 'value' or column = 123
# Captures: (table_or_col, operator, value)
_COMPARISON_RE = re.compile(
    r"\b([A-Za-z_]\w*(?:\.[A-Za-z_]\w*)?)\s*"     # column ref
    r"(=|!=|<>|>=|<=|>|<)\s*"                       # operator
    r"('(?:[^'\\]|\\.)*'|\"(?:[^\"\\]|\\.)*\"|-?\d+(?:\.\d+)?)",  # literal value
    re.IGNORECASE,
)

# Pattern for IS NULL / IS NOT NULL
_IS_NULL_RE = re.compile(
    r"\b([A-Za-z_]\w*(?:\.[A-Za-z_]\w*)?)\s+IS\s+(NOT\s+)?NULL\b",
    re.IGNORECASE,
)

# Pattern for IN (...) with literal values
_IN_RE = re.compile(
    r"\b([A-Za-z_]\w*(?:\.[A-Za-z_]\w*)?)\s+(?:NOT\s+)?IN\s*\(\s*"
    r"((?:'(?:[^'\\]|\\.)*'|\"(?:[^\"\\]|\\.)*\"|-?\d+(?:\.\d+)?)"
    r"(?:\s*,\s*(?:'(?:[^'\\]|\\.)*'|\"(?:[^\"\\]|\\.)*\"|-?\d+(?:\.\d+)?))*)\s*\)",
    re.IGNORECASE,
)

# Pattern for BETWEEN: column BETWEEN val1 AND val2
_BETWEEN_RE = re.compile(
    r"\b([A-Za-z_]\w*(?:\.[A-Za-z_]\w*)?)\s+(?:NOT\s+)?BETWEEN\s+"
    r"('(?:[^'\\]|\\.)*'|-?\d+(?:\.\d+)?)\s+AND\s+"
    r"('(?:[^'\\]|\\.)*'|-?\d+(?:\.\d+)?)",
    re.IGNORECASE,
)

# Pattern for LIKE: column LIKE 'pattern'
_LIKE_RE = re.compile(
    r"\b([A-Za-z_]\w*(?:\.[A-Za-z_]\w*)?)\s+(?:NOT\s+)?LIKE\s+"
    r"'([^']*)'",
    re.IGNORECASE,
)

# Numeric types for range checking
_NUMERIC_TYPES = {
    "integer", "int", "bigint", "smallint", "serial", "bigserial",
    "int4", "int8", "int2", "numeric", "decimal",
    "float", "double precision", "real", "double", "money",
}

# Max distinct_count to treat as enum-like column
_ENUM_THRESHOLD = 20


class FilterValidationCheck:
    """Check WHERE filter values against column profile data.

    Implements CheckProtocol.
    """

    @property
    def name(self) -> str:
        return "filter_validation"

    def run(self, sql: str, context: dict[str, Any]) -> CheckResult:
        """Validate WHERE clause filters against column profiles.

        Checks:
        1. Numeric comparisons against min/max from profiles
        2. String equality against sample_values for low-cardinality columns
        3. IS NULL on columns with 0% nulls / IS NOT NULL on 100% nulls
        4. IN values against known sample_values for enum-like columns
        5. BETWEEN range overlap with profiled min/max

        Args:
            sql: SQL query to validate.
            context: Must contain 'scan_result' (ScanResult).

        Returns:
            CheckResult with PASSED or WARNING status.
        """
        scan_result = _get_scan_result(context)

        where_clause = _extract_where_clause(sql)
        if where_clause is None:
            return make_result(
                self.name,
                CheckStatus.PASSED,
                "No WHERE clause in query",
            )

        aliases = extract_table_aliases(sql)
        profile_lookup = _build_profile_lookup(scan_result)
        type_lookup = _build_type_lookup(scan_result)

        warnings: list[str] = []

        # Check comparisons (=, !=, <, >, etc.)
        warnings.extend(
            _check_comparisons(where_clause, profile_lookup, type_lookup, aliases)
        )

        # Check IS NULL / IS NOT NULL
        warnings.extend(
            _check_null_filters(where_clause, profile_lookup, aliases)
        )

        # Check IN lists
        warnings.extend(
            _check_in_filters(where_clause, profile_lookup, type_lookup, aliases)
        )

        # Check BETWEEN ranges
        warnings.extend(
            _check_between_filters(where_clause, profile_lookup, type_lookup, aliases)
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
            "All filter values are consistent with column profiles",
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


def _extract_where_clause(sql: str) -> str | None:
    """Extract the WHERE clause content from SQL."""
    match = _WHERE_RE.search(sql)
    if not match:
        return None
    clause = match.group(1).strip()
    return clause if clause else None


def _build_profile_lookup(
    scan_result: ScanResult,
) -> dict[str, dict[str, ColumnProfile]]:
    """Build table→{column→ColumnProfile} lookup (all lowercase keys)."""
    lookup: dict[str, dict[str, ColumnProfile]] = {}
    for table in scan_result.tables:
        profiles: dict[str, ColumnProfile] = {}
        for profile in table.profiles:
            profiles[profile.column_name.lower()] = profile
        lookup[table.name.lower()] = profiles
    return lookup


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


def _resolve_column(
    col_ref: str,
    aliases: dict[str, str],
) -> tuple[str | None, str]:
    """Resolve a column reference to (table_name, column_name).

    Returns (table_name_or_None, column_name) with lowercase values.
    """
    if "." in col_ref:
        parts = col_ref.split(".", 1)
        table = aliases.get(parts[0].lower(), parts[0]).lower()
        return table, parts[1].lower()
    return None, col_ref.lower()


def _get_profile(
    col_ref: str,
    profile_lookup: dict[str, dict[str, ColumnProfile]],
    aliases: dict[str, str],
) -> ColumnProfile | None:
    """Look up the ColumnProfile for a column reference."""
    table, col = _resolve_column(col_ref, aliases)
    if table:
        table_profiles = profile_lookup.get(table)
        if table_profiles:
            return table_profiles.get(col)
    else:
        # Bare column — search all tables
        for table_profiles in profile_lookup.values():
            if col in table_profiles:
                return table_profiles[col]
    return None


def _get_column_type(
    col_ref: str,
    type_lookup: dict[str, dict[str, str]],
    aliases: dict[str, str],
) -> str | None:
    """Look up the SQL type of a column reference."""
    table, col = _resolve_column(col_ref, aliases)
    if table:
        table_cols = type_lookup.get(table)
        if table_cols:
            return table_cols.get(col)
    else:
        for table_cols in type_lookup.values():
            if col in table_cols:
                return table_cols[col]
    return None


def _parse_literal(value: str) -> str | float | None:
    """Parse a SQL literal value to Python type.

    Returns string (unquoted) or float, or None if unparseable.
    """
    if not value:
        return None
    # String literal
    if (value.startswith("'") and value.endswith("'")) or \
       (value.startswith('"') and value.endswith('"')):
        return value[1:-1]
    # Numeric literal
    try:
        return float(value)
    except ValueError:
        return None


def _is_numeric_value(value: str) -> bool:
    """Check if a raw literal string is numeric."""
    try:
        float(value.strip("'\""))
        return True
    except ValueError:
        return False


def _check_comparisons(
    where_clause: str,
    profile_lookup: dict[str, dict[str, ColumnProfile]],
    type_lookup: dict[str, dict[str, str]],
    aliases: dict[str, str],
) -> list[str]:
    """Check comparison operators against profile data."""
    warnings: list[str] = []

    for match in _COMPARISON_RE.finditer(where_clause):
        col_ref = match.group(1)
        operator = match.group(2)
        raw_value = match.group(3)

        profile = _get_profile(col_ref, profile_lookup, aliases)
        if profile is None:
            continue

        parsed = _parse_literal(raw_value)
        if parsed is None:
            continue

        col_type = _get_column_type(col_ref, type_lookup, aliases)

        # Numeric range check
        if isinstance(parsed, float) and col_type and col_type in _NUMERIC_TYPES:
            if profile.min_value is not None and profile.max_value is not None:
                try:
                    pmin = float(profile.min_value)
                    pmax = float(profile.max_value)
                    if operator in ("=", ">=", ">") and parsed > pmax:
                        warnings.append(
                            f"Filter {col_ref} {operator} {raw_value} "
                            f"exceeds profiled max ({profile.max_value})"
                        )
                    elif operator in ("=", "<=", "<") and parsed < pmin:
                        warnings.append(
                            f"Filter {col_ref} {operator} {raw_value} "
                            f"is below profiled min ({profile.min_value})"
                        )
                except ValueError:
                    pass

        # String enum check for equality on low-cardinality columns
        if isinstance(parsed, str) and operator in ("=", "!=", "<>"):
            if (profile.sample_values
                    and profile.distinct_count > 0
                    and profile.distinct_count <= _ENUM_THRESHOLD):
                sample_lower = {v.lower() for v in profile.sample_values}
                if parsed.lower() not in sample_lower:
                    warnings.append(
                        f"Filter {col_ref} {operator} '{parsed}' — "
                        f"value not in sampled values {profile.sample_values}"
                    )

    return warnings


def _check_null_filters(
    where_clause: str,
    profile_lookup: dict[str, dict[str, ColumnProfile]],
    aliases: dict[str, str],
) -> list[str]:
    """Check IS NULL / IS NOT NULL against null_fraction profiles."""
    warnings: list[str] = []

    for match in _IS_NULL_RE.finditer(where_clause):
        col_ref = match.group(1)
        is_not_null = match.group(2) is not None

        profile = _get_profile(col_ref, profile_lookup, aliases)
        if profile is None:
            continue

        if is_not_null and profile.null_fraction >= 1.0:
            warnings.append(
                f"Filter {col_ref} IS NOT NULL but column is 100% null"
            )
        elif not is_not_null and profile.null_fraction == 0.0:
            warnings.append(
                f"Filter {col_ref} IS NULL but column has 0% nulls"
            )

    return warnings


def _check_in_filters(
    where_clause: str,
    profile_lookup: dict[str, dict[str, ColumnProfile]],
    type_lookup: dict[str, dict[str, str]],
    aliases: dict[str, str],
) -> list[str]:
    """Check IN list values against profile data."""
    warnings: list[str] = []

    for match in _IN_RE.finditer(where_clause):
        col_ref = match.group(1)
        values_str = match.group(2)

        profile = _get_profile(col_ref, profile_lookup, aliases)
        if profile is None:
            continue

        col_type = _get_column_type(col_ref, type_lookup, aliases)

        # Parse individual values from the IN list
        in_values: list[str | float] = []
        for val_match in re.finditer(
            r"'([^'\\]*(?:\\.[^'\\]*)*)'|\"([^\"\\]*(?:\\.[^\"\\]*)*)\"|(-?\d+(?:\.\d+)?)",
            values_str,
        ):
            if val_match.group(1) is not None:
                in_values.append(val_match.group(1))
            elif val_match.group(2) is not None:
                in_values.append(val_match.group(2))
            elif val_match.group(3) is not None:
                in_values.append(float(val_match.group(3)))

        # Numeric range check for IN values
        if col_type and col_type in _NUMERIC_TYPES:
            if profile.min_value is not None and profile.max_value is not None:
                try:
                    pmin = float(profile.min_value)
                    pmax = float(profile.max_value)
                    out_of_range = [
                        v for v in in_values
                        if isinstance(v, float) and (v < pmin or v > pmax)
                    ]
                    if out_of_range:
                        warnings.append(
                            f"IN filter on {col_ref} has values {out_of_range} "
                            f"outside profiled range [{profile.min_value}, {profile.max_value}]"
                        )
                except ValueError:
                    pass

        # String enum check for IN values on low-cardinality columns
        if (profile.sample_values
                and profile.distinct_count > 0
                and profile.distinct_count <= _ENUM_THRESHOLD):
            sample_lower = {v.lower() for v in profile.sample_values}
            unknown_strings = [
                v for v in in_values
                if isinstance(v, str) and v.lower() not in sample_lower
            ]
            if unknown_strings:
                warnings.append(
                    f"IN filter on {col_ref} has values {unknown_strings} "
                    f"not in sampled values {profile.sample_values}"
                )

    return warnings


def _check_between_filters(
    where_clause: str,
    profile_lookup: dict[str, dict[str, ColumnProfile]],
    type_lookup: dict[str, dict[str, str]],
    aliases: dict[str, str],
) -> list[str]:
    """Check BETWEEN ranges against profile min/max."""
    warnings: list[str] = []

    for match in _BETWEEN_RE.finditer(where_clause):
        col_ref = match.group(1)
        low_raw = match.group(2)
        high_raw = match.group(3)

        profile = _get_profile(col_ref, profile_lookup, aliases)
        if profile is None:
            continue

        col_type = _get_column_type(col_ref, type_lookup, aliases)

        # Numeric range overlap check
        if col_type and col_type in _NUMERIC_TYPES:
            if profile.min_value is not None and profile.max_value is not None:
                try:
                    pmin = float(profile.min_value)
                    pmax = float(profile.max_value)
                    low = float(low_raw)
                    high = float(high_raw)
                    # No overlap: filter range entirely above or below profiled range
                    if low > pmax or high < pmin:
                        warnings.append(
                            f"BETWEEN filter on {col_ref} [{low_raw}, {high_raw}] "
                            f"has no overlap with profiled range "
                            f"[{profile.min_value}, {profile.max_value}]"
                        )
                except ValueError:
                    pass

    return warnings


# Ensure the class satisfies the protocol
assert isinstance(FilterValidationCheck, type)
_check: CheckProtocol = FilterValidationCheck()  # type: ignore[assignment]
del _check
