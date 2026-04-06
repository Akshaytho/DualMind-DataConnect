"""Tests for verifier.join_validation — Check 2."""

import pytest

from dataconnect.exceptions import VerificationError
from dataconnect.models import (
    CheckStatus,
    ColumnInfo,
    RelationshipInfo,
    RelationshipType,
    ScanResult,
    TableInfo,
)
from dataconnect.verifier.join_validation import (
    JoinValidationCheck,
    parse_join_conditions,
)


# ── Fixtures ────────────────────────────────────────────────────────


@pytest.fixture
def scan_result() -> ScanResult:
    """ScanResult with orders/customers/products and FK relationships."""
    return ScanResult(
        database_name="test_db",
        tables=[
            TableInfo(
                name="orders",
                columns=[
                    ColumnInfo(name="id", data_type="integer"),
                    ColumnInfo(name="customer_id", data_type="integer"),
                    ColumnInfo(name="product_id", data_type="integer"),
                    ColumnInfo(name="total", data_type="numeric"),
                    ColumnInfo(name="notes", data_type="text"),
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
                    ColumnInfo(name="category_id", data_type="uuid"),
                ],
            ),
        ],
        relationships=[
            RelationshipInfo(
                source_table="orders",
                source_column="customer_id",
                target_table="customers",
                target_column="id",
                relationship_type=RelationshipType.DECLARED_FK,
                confidence=1.0,
            ),
            RelationshipInfo(
                source_table="orders",
                source_column="product_id",
                target_table="products",
                target_column="id",
                relationship_type=RelationshipType.DECLARED_FK,
                confidence=1.0,
            ),
        ],
    )


@pytest.fixture
def context(scan_result: ScanResult) -> dict:
    return {"scan_result": scan_result}


@pytest.fixture
def check() -> JoinValidationCheck:
    return JoinValidationCheck()


# ── parse_join_conditions ──────────────────────────────────────────


def test_parse_simple_join():
    sql = ("SELECT * FROM orders "
           "JOIN customers ON orders.customer_id = customers.id")
    joins = parse_join_conditions(sql)
    assert len(joins) == 1
    assert joins[0] == (("orders", "customer_id"), ("customers", "id"))


def test_parse_multiple_joins():
    sql = ("SELECT * FROM orders "
           "JOIN customers ON orders.customer_id = customers.id "
           "JOIN products ON orders.product_id = products.id")
    joins = parse_join_conditions(sql)
    assert len(joins) == 2


def test_parse_left_join():
    sql = ("SELECT * FROM orders "
           "LEFT JOIN customers ON orders.customer_id = customers.id")
    joins = parse_join_conditions(sql)
    assert len(joins) == 1


def test_parse_no_joins():
    sql = "SELECT * FROM orders WHERE orders.id = 1"
    joins = parse_join_conditions(sql)
    assert len(joins) == 0


def test_parse_multi_condition_join():
    sql = ("SELECT * FROM orders "
           "JOIN customers ON orders.customer_id = customers.id "
           "AND orders.total = customers.id")
    joins = parse_join_conditions(sql)
    assert len(joins) == 2


def test_parse_aliased_join():
    sql = ("SELECT * FROM orders o "
           "JOIN customers c ON o.customer_id = c.id")
    joins = parse_join_conditions(sql)
    assert len(joins) == 1
    assert joins[0] == (("o", "customer_id"), ("c", "id"))


# ── JoinValidationCheck.run — PASSED ───────────────────────────────


def test_valid_join_passes(check, context):
    sql = ("SELECT * FROM orders "
           "JOIN customers ON orders.customer_id = customers.id")
    result = check.run(sql, context)
    assert result.status == CheckStatus.PASSED
    assert "1 join" in result.message


def test_valid_multi_join_passes(check, context):
    sql = ("SELECT * FROM orders "
           "JOIN customers ON orders.customer_id = customers.id "
           "JOIN products ON orders.product_id = products.id")
    result = check.run(sql, context)
    assert result.status == CheckStatus.PASSED
    assert "2 join" in result.message


def test_no_joins_passes(check, context):
    sql = "SELECT * FROM orders"
    result = check.run(sql, context)
    assert result.status == CheckStatus.PASSED
    assert "No JOIN" in result.message


# ── JoinValidationCheck.run — WARNING (unknown relationship) ───────


def test_unknown_relationship_warns(check, context):
    # Joining on columns that exist but have no known relationship
    sql = ("SELECT * FROM orders "
           "JOIN customers ON orders.total = customers.id")
    result = check.run(sql, context)
    assert result.status == CheckStatus.WARNING
    assert len(result.details["warnings"]) > 0
    assert "No known relationship" in result.details["warnings"][0]


def test_reverse_direction_still_known(check, context):
    # The FK is orders.customer_id → customers.id
    # Joining in reverse should still be recognized
    sql = ("SELECT * FROM customers "
           "JOIN orders ON customers.id = orders.customer_id")
    result = check.run(sql, context)
    assert result.status == CheckStatus.PASSED


# ── JoinValidationCheck.run — FAILED (missing columns) ────────────


def test_nonexistent_column_fails(check, context):
    sql = ("SELECT * FROM orders "
           "JOIN customers ON orders.bogus = customers.id")
    result = check.run(sql, context)
    assert result.status == CheckStatus.FAILED
    assert any("bogus" in i for i in result.details["issues"])


def test_nonexistent_column_both_sides(check, context):
    sql = ("SELECT * FROM orders "
           "JOIN customers ON orders.bogus = customers.fake")
    result = check.run(sql, context)
    assert result.status == CheckStatus.FAILED
    assert len(result.details["issues"]) >= 2


# ── JoinValidationCheck.run — FAILED (type mismatch) ──────────────


def test_type_mismatch_fails(check, context):
    # orders.notes is text, customers.id is integer
    sql = ("SELECT * FROM orders "
           "JOIN customers ON orders.notes = customers.id")
    result = check.run(sql, context)
    assert result.status == CheckStatus.FAILED
    assert any("Type mismatch" in i for i in result.details["issues"])


def test_compatible_types_pass(check, context):
    # orders.total is numeric, products.price is numeric — compatible
    sql = ("SELECT * FROM orders "
           "JOIN products ON orders.total = products.price")
    result = check.run(sql, context)
    # Should warn (no known relationship) but not fail (types match)
    assert result.status in (CheckStatus.PASSED, CheckStatus.WARNING)
    if result.details.get("issues"):
        assert not any("Type mismatch" in i
                       for i in result.details["issues"])


def test_uuid_vs_integer_fails(check, context):
    # products.category_id is uuid, orders.id is integer
    sql = ("SELECT * FROM orders "
           "JOIN products ON orders.id = products.category_id")
    result = check.run(sql, context)
    assert result.status == CheckStatus.FAILED
    assert any("Type mismatch" in i for i in result.details["issues"])


# ── Alias resolution ───────────────────────────────────────────────


def test_alias_resolution(check, context):
    sql = ("SELECT * FROM orders o "
           "JOIN customers c ON o.customer_id = c.id")
    result = check.run(sql, context)
    assert result.status == CheckStatus.PASSED


# ── Context validation ─────────────────────────────────────────────


def test_missing_context(check):
    with pytest.raises(VerificationError, match="missing 'scan_result'"):
        check.run("SELECT 1", {})


def test_wrong_type_context(check):
    with pytest.raises(VerificationError, match="not a ScanResult"):
        check.run("SELECT 1", {"scan_result": 42})
