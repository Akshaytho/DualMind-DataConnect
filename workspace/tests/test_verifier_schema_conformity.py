"""Tests for verifier.schema_conformity — Check 1."""

import pytest

from dataconnect.exceptions import VerificationError
from dataconnect.models import (
    CheckStatus,
    ColumnInfo,
    ScanResult,
    TableInfo,
)
from dataconnect.verifier.base import CheckProtocol
from dataconnect.verifier.schema_conformity import (
    SchemaConformityCheck,
    extract_qualified_columns,
    extract_table_aliases,
    extract_table_references,
)


# ── Fixtures ────────────────────────────────────────────────────────


@pytest.fixture
def scan_result() -> ScanResult:
    """ScanResult with orders/customers/products tables."""
    return ScanResult(
        database_name="test_db",
        tables=[
            TableInfo(
                name="orders",
                columns=[
                    ColumnInfo(name="id", data_type="integer"),
                    ColumnInfo(name="customer_id", data_type="integer"),
                    ColumnInfo(name="total", data_type="numeric"),
                    ColumnInfo(name="created_at", data_type="timestamp"),
                ],
            ),
            TableInfo(
                name="customers",
                columns=[
                    ColumnInfo(name="id", data_type="integer"),
                    ColumnInfo(name="name", data_type="varchar"),
                    ColumnInfo(name="email", data_type="varchar"),
                ],
            ),
            TableInfo(
                name="products",
                columns=[
                    ColumnInfo(name="id", data_type="integer"),
                    ColumnInfo(name="name", data_type="varchar"),
                    ColumnInfo(name="price", data_type="numeric"),
                ],
            ),
        ],
    )


@pytest.fixture
def context(scan_result: ScanResult) -> dict:
    return {"scan_result": scan_result}


@pytest.fixture
def check() -> SchemaConformityCheck:
    return SchemaConformityCheck()


# ── Protocol Compliance ─────────────────────────────────────────────


def test_implements_check_protocol():
    check = SchemaConformityCheck()
    assert isinstance(check, CheckProtocol)


def test_name_property():
    check = SchemaConformityCheck()
    assert check.name == "schema_conformity"


# ── extract_table_references ────────────────────────────────────────


def test_extract_simple_from():
    sql = "SELECT * FROM orders"
    assert extract_table_references(sql) == ["orders"]


def test_extract_multiple_from():
    sql = "SELECT * FROM orders, customers"
    refs = extract_table_references(sql)
    assert "orders" in refs
    assert "customers" in refs


def test_extract_join():
    sql = "SELECT * FROM orders JOIN customers ON orders.id = customers.id"
    refs = extract_table_references(sql)
    assert "orders" in refs
    assert "customers" in refs


def test_extract_left_join():
    sql = ("SELECT * FROM orders LEFT JOIN customers "
           "ON orders.customer_id = customers.id")
    refs = extract_table_references(sql)
    assert "orders" in refs
    assert "customers" in refs


def test_extract_with_alias():
    sql = "SELECT o.id FROM orders o JOIN customers c ON o.id = c.id"
    refs = extract_table_references(sql)
    assert "orders" in refs
    assert "customers" in refs


def test_extract_empty_sql():
    assert extract_table_references("") == []


def test_extract_no_from():
    sql = "SELECT 1"
    assert extract_table_references(sql) == []


# ── extract_table_aliases ──────────────────────────────────────────


def test_aliases_simple():
    sql = "SELECT o.id FROM orders o"
    aliases = extract_table_aliases(sql)
    assert aliases.get("o") == "orders"


def test_aliases_multiple():
    sql = ("SELECT o.id, c.name FROM orders o "
           "JOIN customers c ON o.customer_id = c.id")
    aliases = extract_table_aliases(sql)
    assert aliases.get("o") == "orders"
    assert aliases.get("c") == "customers"


def test_aliases_none():
    sql = "SELECT * FROM orders"
    aliases = extract_table_aliases(sql)
    # No alias — 'orders' shouldn't appear as both key and value
    assert "orders" not in aliases


def test_aliases_as_keyword():
    sql = "SELECT * FROM orders AS o"
    aliases = extract_table_aliases(sql)
    assert aliases.get("o") == "orders"


# ── extract_qualified_columns ──────────────────────────────────────


def test_qualified_columns_simple():
    sql = "SELECT orders.id, orders.total FROM orders"
    cols = extract_qualified_columns(sql)
    assert ("orders", "id") in cols
    assert ("orders", "total") in cols


def test_qualified_columns_with_alias():
    sql = "SELECT o.id FROM orders o WHERE o.total > 100"
    cols = extract_qualified_columns(sql)
    assert ("o", "id") in cols
    assert ("o", "total") in cols


def test_qualified_columns_join_condition():
    sql = ("SELECT * FROM orders o "
           "JOIN customers c ON o.customer_id = c.id")
    cols = extract_qualified_columns(sql)
    assert ("o", "customer_id") in cols
    assert ("c", "id") in cols


def test_qualified_columns_none():
    sql = "SELECT id, name FROM orders"
    cols = extract_qualified_columns(sql)
    assert cols == []


# ── SchemaConformityCheck.run — PASSED ──────────────────────────────


def test_all_tables_and_columns_exist(check, context):
    sql = "SELECT orders.id, orders.total FROM orders"
    result = check.run(sql, context)
    assert result.status == CheckStatus.PASSED
    assert result.check_name == "schema_conformity"


def test_join_all_valid(check, context):
    sql = ("SELECT o.id, c.name FROM orders o "
           "JOIN customers c ON o.customer_id = c.id")
    result = check.run(sql, context)
    assert result.status == CheckStatus.PASSED


def test_multiple_tables_all_valid(check, context):
    sql = ("SELECT orders.id, customers.name, products.price "
           "FROM orders "
           "JOIN customers ON orders.customer_id = customers.id "
           "JOIN products ON products.id = orders.id")
    result = check.run(sql, context)
    assert result.status == CheckStatus.PASSED


# ── SchemaConformityCheck.run — FAILED (missing tables) ────────────


def test_unknown_table(check, context):
    sql = "SELECT * FROM nonexistent"
    result = check.run(sql, context)
    assert result.status == CheckStatus.FAILED
    assert "nonexistent" in result.details["missing_tables"]


def test_unknown_table_in_join(check, context):
    sql = ("SELECT * FROM orders "
           "JOIN invoices ON orders.id = invoices.order_id")
    result = check.run(sql, context)
    assert result.status == CheckStatus.FAILED
    assert "invoices" in result.details["missing_tables"]


# ── SchemaConformityCheck.run — WARNING (missing columns) ──────────


def test_unknown_column(check, context):
    sql = "SELECT orders.nonexistent_col FROM orders"
    result = check.run(sql, context)
    assert result.status == CheckStatus.WARNING
    assert any("nonexistent_col" in c
               for c in result.details["missing_columns"])


def test_unknown_column_with_alias(check, context):
    sql = "SELECT o.bogus FROM orders o"
    result = check.run(sql, context)
    assert result.status == CheckStatus.WARNING
    assert any("bogus" in c for c in result.details["missing_columns"])


# ── SchemaConformityCheck.run — FAILED (both missing) ──────────────


def test_both_missing_table_and_column(check, context):
    sql = ("SELECT orders.bogus_col FROM orders "
           "JOIN ghost_table ON orders.id = ghost_table.id")
    result = check.run(sql, context)
    assert result.status == CheckStatus.FAILED
    assert "ghost_table" in result.details["missing_tables"]
    assert any("bogus_col" in c for c in result.details["missing_columns"])


# ── Context validation ─────────────────────────────────────────────


def test_missing_context_raises(check):
    with pytest.raises(VerificationError, match="missing 'scan_result'"):
        check.run("SELECT 1", {})


def test_wrong_type_context_raises(check):
    with pytest.raises(VerificationError, match="not a ScanResult"):
        check.run("SELECT 1", {"scan_result": "not_a_scan_result"})


# ── Edge cases ─────────────────────────────────────────────────────


def test_case_insensitive_table_match(check, context):
    sql = "SELECT ORDERS.id FROM ORDERS"
    result = check.run(sql, context)
    assert result.status == CheckStatus.PASSED


def test_star_column_not_flagged(check, context):
    sql = "SELECT o.* FROM orders o"
    result = check.run(sql, context)
    assert result.status == CheckStatus.PASSED


def test_select_without_table_ref(check, context):
    sql = "SELECT 1"
    result = check.run(sql, context)
    assert result.status == CheckStatus.PASSED
