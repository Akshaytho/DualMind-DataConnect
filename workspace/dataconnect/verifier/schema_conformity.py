"""Verifier Check 1: Schema Conformity.

Validates that every table and column referenced in a SQL query
actually exists in the scanned database schema.
"""

from __future__ import annotations

import logging
import re
from typing import Any

import sqlparse
from sqlparse.sql import Identifier, IdentifierList, Where
from sqlparse.tokens import Keyword, DML

from dataconnect.exceptions import VerificationError
from dataconnect.models import CheckResult, CheckStatus, ScanResult
from dataconnect.verifier.base import CheckProtocol, make_result

logger = logging.getLogger(__name__)

# SQL keywords that precede table references
_TABLE_KEYWORDS = {"FROM", "JOIN", "INNER JOIN", "LEFT JOIN", "RIGHT JOIN",
                   "FULL JOIN", "CROSS JOIN", "LEFT OUTER JOIN",
                   "RIGHT OUTER JOIN", "FULL OUTER JOIN", "INTO"}

# Pattern for qualified column refs: table.column or table.column alias
_QUALIFIED_COL_RE = re.compile(
    r"\b([A-Za-z_]\w*)\s*\.\s*([A-Za-z_]\w*)\b"
)


class SchemaConformityCheck:
    """Check that all tables and columns in SQL exist in the schema.

    Implements CheckProtocol.
    """

    @property
    def name(self) -> str:
        return "schema_conformity"

    def run(self, sql: str, context: dict[str, Any]) -> CheckResult:
        """Verify all table/column references against scan_result schema.

        Args:
            sql: SQL query to validate.
            context: Must contain 'scan_result' (ScanResult).

        Returns:
            CheckResult with PASSED, WARNING, or FAILED status.
        """
        scan_result = _get_scan_result(context)
        schema = _build_schema_lookup(scan_result)

        # Extract references from SQL
        table_refs = extract_table_references(sql)
        table_aliases = extract_table_aliases(sql)
        qualified_cols = extract_qualified_columns(sql)

        missing_tables: list[str] = []
        missing_columns: list[str] = []

        # Check tables exist
        for table_name in table_refs:
            if table_name.lower() not in schema:
                missing_tables.append(table_name)

        # Check qualified column references (table.column)
        for table_ref, col_name in qualified_cols:
            # Resolve alias to real table name
            real_table = table_aliases.get(table_ref.lower(), table_ref)
            table_lower = real_table.lower()

            if table_lower not in schema:
                # Table already flagged as missing, skip column check
                continue

            if col_name.lower() not in schema[table_lower]:
                # Check if it could be * (SELECT t.*)
                if col_name != "*":
                    missing_columns.append(f"{real_table}.{col_name}")

        if missing_tables and missing_columns:
            return make_result(
                self.name,
                CheckStatus.FAILED,
                f"Unknown tables: {missing_tables}; "
                f"unknown columns: {missing_columns}",
                missing_tables=missing_tables,
                missing_columns=missing_columns,
            )
        if missing_tables:
            return make_result(
                self.name,
                CheckStatus.FAILED,
                f"Unknown tables: {missing_tables}",
                missing_tables=missing_tables,
                missing_columns=[],
            )
        if missing_columns:
            return make_result(
                self.name,
                CheckStatus.WARNING,
                f"Unknown columns: {missing_columns}",
                missing_tables=[],
                missing_columns=missing_columns,
            )

        return make_result(
            self.name,
            CheckStatus.PASSED,
            "All referenced tables and columns exist in schema",
            missing_tables=[],
            missing_columns=[],
        )


def _get_scan_result(context: dict[str, Any]) -> ScanResult:
    """Extract ScanResult from context dict."""
    scan_result = context.get("scan_result")
    if scan_result is None:
        raise VerificationError("context missing 'scan_result'")
    if not isinstance(scan_result, ScanResult):
        raise VerificationError("context['scan_result'] is not a ScanResult")
    return scan_result


def _build_schema_lookup(
    scan_result: ScanResult,
) -> dict[str, set[str]]:
    """Build table→{columns} lookup from ScanResult (all lowercase)."""
    schema: dict[str, set[str]] = {}
    for table in scan_result.tables:
        cols = {c.name.lower() for c in table.columns}
        schema[table.name.lower()] = cols
    return schema


def extract_table_references(sql: str) -> list[str]:
    """Extract table names referenced in FROM/JOIN clauses.

    Args:
        sql: SQL query string.

    Returns:
        List of table names (without aliases, without schema prefix).
    """
    parsed = sqlparse.parse(sql)
    if not parsed:
        return []

    tables: list[str] = []
    for statement in parsed:
        _walk_for_tables(statement.tokens, tables)
    return tables


def _walk_for_tables(
    tokens: list[Any],
    tables: list[str],
) -> None:
    """Walk token list and collect table names after FROM/JOIN keywords."""
    expect_table = False

    for token in tokens:
        # Skip whitespace and comments
        if token.ttype in (sqlparse.tokens.Whitespace,
                           sqlparse.tokens.Newline,
                           sqlparse.tokens.Comment.Single,
                           sqlparse.tokens.Comment.Multiline):
            continue

        # Check for FROM/JOIN keywords
        if token.ttype is Keyword or token.ttype is Keyword.DML:
            upper = token.normalized.upper()
            if upper in _TABLE_KEYWORDS or upper.endswith("JOIN"):
                expect_table = True
                continue
            expect_table = False
            continue

        if expect_table:
            if isinstance(token, IdentifierList):
                for identifier in token.get_identifiers():
                    name = _extract_table_name(identifier)
                    if name:
                        tables.append(name)
            elif isinstance(token, Identifier):
                name = _extract_table_name(token)
                if name:
                    tables.append(name)
            expect_table = False
            continue

        # Recurse into subgroups (but not WHERE — no tables there for us)
        if hasattr(token, "tokens") and not isinstance(token, Where):
            _walk_for_tables(token.tokens, tables)


def _extract_table_name(identifier: Any) -> str | None:
    """Get the real table name from a sqlparse Identifier.

    Handles: 'orders', 'orders o', 'public.orders', 'public.orders o'
    Returns: 'orders' (the table part, without schema or alias).
    """
    real_name = identifier.get_real_name()
    if real_name and not real_name.startswith("("):
        return real_name
    return None


def extract_table_aliases(sql: str) -> dict[str, str]:
    """Build alias→real_table_name mapping from SQL (all lowercase keys).

    Args:
        sql: SQL query string.

    Returns:
        Dict mapping lowercase alias to real table name.
    """
    parsed = sqlparse.parse(sql)
    if not parsed:
        return {}

    aliases: dict[str, str] = {}
    for statement in parsed:
        _walk_for_aliases(statement.tokens, aliases)
    return aliases


def _walk_for_aliases(
    tokens: list[Any],
    aliases: dict[str, str],
) -> None:
    """Walk tokens collecting table aliases."""
    expect_table = False

    for token in tokens:
        if token.ttype in (sqlparse.tokens.Whitespace,
                           sqlparse.tokens.Newline):
            continue

        if token.ttype is Keyword or token.ttype is Keyword.DML:
            upper = token.normalized.upper()
            if upper in _TABLE_KEYWORDS or upper.endswith("JOIN"):
                expect_table = True
                continue
            expect_table = False
            continue

        if expect_table:
            if isinstance(token, IdentifierList):
                for identifier in token.get_identifiers():
                    _collect_alias(identifier, aliases)
            elif isinstance(token, Identifier):
                _collect_alias(token, aliases)
            expect_table = False
            continue

        if hasattr(token, "tokens") and not isinstance(token, Where):
            _walk_for_aliases(token.tokens, aliases)


def _collect_alias(identifier: Any, aliases: dict[str, str]) -> None:
    """If identifier has an alias, add it to the dict."""
    real = identifier.get_real_name()
    alias = identifier.get_alias()
    if real and alias and alias != real:
        aliases[alias.lower()] = real


def extract_qualified_columns(sql: str) -> list[tuple[str, str]]:
    """Extract all table.column references from SQL.

    Args:
        sql: SQL query string.

    Returns:
        List of (table_ref, column_name) tuples.
    """
    return _QUALIFIED_COL_RE.findall(sql)


# Ensure the class satisfies the protocol
assert isinstance(SchemaConformityCheck, type)
_check: CheckProtocol = SchemaConformityCheck()  # type: ignore[assignment]
del _check
