"""Verifier Check 2: Join Validation.

Validates that JOIN operations reference existing columns, use type-compatible
columns, and involve known relationships from the scan result.
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
)
from dataconnect.verifier.base import make_result
from dataconnect.verifier.schema_conformity import extract_table_aliases

logger = logging.getLogger(__name__)

# Matches: JOIN <table> [alias] ON <left> = <right>
# Captures join conditions as table.col = table.col pairs
_JOIN_ON_RE = re.compile(
    r"\bJOIN\b\s+"
    r"(\w+)"              # table name
    r"(?:\s+(?:AS\s+)?(\w+))?"  # optional alias
    r"\s+ON\s+"
    r"(\w+\.\w+)"         # left side: table.col
    r"\s*=\s*"
    r"(\w+\.\w+)",        # right side: table.col
    re.IGNORECASE,
)

# For multi-condition joins (AND), capture additional conditions
_JOIN_COND_RE = re.compile(
    r"(\w+\.\w+)\s*=\s*(\w+\.\w+)",
    re.IGNORECASE,
)

# Captures full JOIN ... ON ... (until next JOIN or WHERE or ORDER or GROUP
# or end of string)
_JOIN_BLOCK_RE = re.compile(
    r"\bJOIN\b\s+(\w+)(?:\s+(?:AS\s+)?(\w+))?\s+ON\s+"
    r"(.*?)(?=\b(?:JOIN|WHERE|ORDER|GROUP|HAVING|LIMIT|UNION)\b|$)",
    re.IGNORECASE | re.DOTALL,
)


class JoinValidationCheck:
    """Check that joins use valid columns with compatible types.

    Implements CheckProtocol.
    """

    @property
    def name(self) -> str:
        return "join_validation"

    def run(self, sql: str, context: dict[str, Any]) -> CheckResult:
        """Validate all JOIN conditions in the SQL.

        Checks:
        1. Join columns exist in their respective tables
        2. Joined column types are compatible
        3. The join relationship is known (from scan relationships)

        Args:
            sql: SQL query to validate.
            context: Must contain 'scan_result' (ScanResult).

        Returns:
            CheckResult with status and details.
        """
        scan_result = _get_scan_result(context)
        schema = _build_type_lookup(scan_result)
        relationships = _build_relationship_set(scan_result)
        aliases = extract_table_aliases(sql)

        joins = parse_join_conditions(sql)

        if not joins:
            return make_result(
                self.name,
                CheckStatus.PASSED,
                "No JOIN clauses found",
            )

        issues: list[str] = []
        warnings: list[str] = []

        for left_ref, right_ref in joins:
            left_table, left_col = left_ref
            right_table, right_col = right_ref

            # Resolve aliases
            real_left = aliases.get(left_table.lower(), left_table)
            real_right = aliases.get(right_table.lower(), right_table)

            # Check columns exist
            left_type = _get_column_type(real_left, left_col, schema)
            right_type = _get_column_type(real_right, right_col, schema)

            if left_type is None:
                issues.append(
                    f"Column {real_left}.{left_col} not found in schema"
                )
            if right_type is None:
                issues.append(
                    f"Column {real_right}.{right_col} not found in schema"
                )

            # Type compatibility (only if both exist)
            if left_type is not None and right_type is not None:
                if not _types_compatible(left_type, right_type):
                    issues.append(
                        f"Type mismatch: {real_left}.{left_col} "
                        f"({left_type}) vs {real_right}.{right_col} "
                        f"({right_type})"
                    )

            # Known relationship check
            if not _is_known_relationship(
                real_left, left_col, real_right, right_col, relationships,
            ):
                warnings.append(
                    f"No known relationship: {real_left}.{left_col} "
                    f"↔ {real_right}.{right_col}"
                )

        if issues:
            return make_result(
                self.name,
                CheckStatus.FAILED,
                "; ".join(issues),
                issues=issues,
                warnings=warnings,
                join_count=len(joins),
            )

        if warnings:
            return make_result(
                self.name,
                CheckStatus.WARNING,
                "; ".join(warnings),
                issues=[],
                warnings=warnings,
                join_count=len(joins),
            )

        return make_result(
            self.name,
            CheckStatus.PASSED,
            f"All {len(joins)} join condition(s) validated",
            issues=[],
            warnings=[],
            join_count=len(joins),
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


def _build_relationship_set(
    scan_result: ScanResult,
) -> set[tuple[str, str, str, str]]:
    """Build set of known relationships as (src_table, src_col, tgt_table, tgt_col).

    All lowercase. Both directions stored for bidirectional lookup.
    """
    rels: set[tuple[str, str, str, str]] = set()
    for rel in scan_result.relationships:
        forward = (
            rel.source_table.lower(),
            rel.source_column.lower(),
            rel.target_table.lower(),
            rel.target_column.lower(),
        )
        reverse = (forward[2], forward[3], forward[0], forward[1])
        rels.add(forward)
        rels.add(reverse)
    return rels


def _get_column_type(
    table: str,
    column: str,
    schema: dict[str, dict[str, str]],
) -> str | None:
    """Look up column type. Returns None if table or column not found."""
    table_cols = schema.get(table.lower())
    if table_cols is None:
        return None
    return table_cols.get(column.lower())


# Type compatibility groups — types within the same group can be joined
_TYPE_GROUPS: list[set[str]] = [
    {"integer", "int", "bigint", "smallint", "serial", "bigserial", "int4",
     "int8", "int2", "numeric", "decimal", "float", "double precision",
     "real", "double"},
    {"text", "varchar", "character varying", "char", "character", "name",
     "citext", "bpchar"},
    {"timestamp", "timestamptz", "timestamp without time zone",
     "timestamp with time zone", "date", "datetime"},
    {"boolean", "bool"},
    {"uuid"},
    {"json", "jsonb"},
]


def _types_compatible(type_a: str, type_b: str) -> bool:
    """Check if two SQL types are compatible for joining.

    Types are compatible if they belong to the same type group.
    Unknown types are treated as compatible (benefit of the doubt).
    """
    a = type_a.lower().strip()
    b = type_b.lower().strip()

    if a == b:
        return True

    for group in _TYPE_GROUPS:
        a_in = a in group
        b_in = b in group
        if a_in and b_in:
            return True
        if a_in != b_in:
            # One is in a group, the other is in a different group or unknown
            # If the other is unknown, allow it
            if a_in and b not in _all_known_types():
                return True
            if b_in and a not in _all_known_types():
                return True
            if a_in and b_in is False and b in _all_known_types():
                return False

    # Both unknown — allow
    if a not in _all_known_types() or b not in _all_known_types():
        return True

    return False


def _all_known_types() -> set[str]:
    """Flatten all type groups into a single set."""
    result: set[str] = set()
    for group in _TYPE_GROUPS:
        result |= group
    return result


def _is_known_relationship(
    table_a: str,
    col_a: str,
    table_b: str,
    col_b: str,
    relationships: set[tuple[str, str, str, str]],
) -> bool:
    """Check if join columns correspond to a known relationship."""
    key = (table_a.lower(), col_a.lower(), table_b.lower(), col_b.lower())
    return key in relationships


def parse_join_conditions(
    sql: str,
) -> list[tuple[tuple[str, str], tuple[str, str]]]:
    """Parse JOIN ... ON conditions from SQL.

    Args:
        sql: SQL query string.

    Returns:
        List of ((left_table, left_col), (right_table, right_col)) pairs.
    """
    results: list[tuple[tuple[str, str], tuple[str, str]]] = []

    for match in _JOIN_BLOCK_RE.finditer(sql):
        on_clause = match.group(3)
        # Find all table.col = table.col conditions in this ON clause
        for cond in _JOIN_COND_RE.finditer(on_clause):
            left = cond.group(1)
            right = cond.group(2)
            left_parts = left.split(".", 1)
            right_parts = right.split(".", 1)
            if len(left_parts) == 2 and len(right_parts) == 2:
                results.append(
                    ((left_parts[0], left_parts[1]),
                     (right_parts[0], right_parts[1]))
                )

    return results
