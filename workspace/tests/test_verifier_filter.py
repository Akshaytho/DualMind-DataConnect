"""Tests for verifier Check 4: Filter Validation."""

from __future__ import annotations

import pytest

from dataconnect.models import (
    CheckStatus,
    ColumnInfo,
    ColumnProfile,
    ScanResult,
    TableInfo,
)
from dataconnect.verifier.base import CheckProtocol
from dataconnect.verifier.filter_validation import (
    FilterValidationCheck,
    _check_between_filters,
    _check_comparisons,
    _check_in_filters,
    _check_null_filters,
    _extract_where_clause,
)


# ── Fixtures ────────────────────────────────────────────────────────


@pytest.fixture
def rich_scan_result() -> ScanResult:
    """ScanResult with profiles including sample_values, min/max, null_fraction."""
    return ScanResult(
        database_name="test_db",
        tables=[
            TableInfo(
                name="orders",
                columns=[
                    ColumnInfo(name="id", data_type="INTEGER", is_primary_key=True, nullable=False),
                    ColumnInfo(name="user_id", data_type="INTEGER", nullable=False),
                    ColumnInfo(name="amount", data_type="INTEGER", nullable=False),
                    ColumnInfo(name="status", data_type="VARCHAR(50)", nullable=False),
                    ColumnInfo(name="notes", data_type="TEXT", nullable=True),
                ],
                row_count_estimate=1000,
                profiles=[
                    ColumnProfile(column_name="id", distinct_count=1000, null_fraction=0.0,
                                  min_value="1", max_value="1000"),
                    ColumnProfile(column_name="user_id", distinct_count=200, null_fraction=0.0,
                                  min_value="1", max_value="500"),
                    ColumnProfile(column_name="amount", distinct_count=50, null_fraction=0.0,
                                  min_value="10", max_value="5000",
                                  sample_values=["100", "250", "500", "1000"]),
                    ColumnProfile(column_name="status", distinct_count=4, null_fraction=0.0,
                                  sample_values=["pending", "shipped", "delivered", "cancelled"]),
                    ColumnProfile(column_name="notes", distinct_count=800, null_fraction=0.7),
                ],
            ),
            TableInfo(
                name="users",
                columns=[
                    ColumnInfo(name="id", data_type="INTEGER", is_primary_key=True, nullable=False),
                    ColumnInfo(name="name", data_type="VARCHAR(100)", nullable=False),
                    ColumnInfo(name="role", data_type="VARCHAR(20)", nullable=False),
                    ColumnInfo(name="age", data_type="INTEGER", nullable=True),
                    ColumnInfo(name="deleted_at", data_type="TIMESTAMP", nullable=True),
                ],
                row_count_estimate=500,
                profiles=[
                    ColumnProfile(column_name="id", distinct_count=500, null_fraction=0.0,
                                  min_value="1", max_value="500"),
                    ColumnProfile(column_name="name", distinct_count=480, null_fraction=0.0),
                    ColumnProfile(column_name="role", distinct_count=3, null_fraction=0.0,
                                  sample_values=["admin", "editor", "viewer"]),
                    ColumnProfile(column_name="age", distinct_count=60, null_fraction=0.05,
                                  min_value="18", max_value="85"),
                    ColumnProfile(column_name="deleted_at", distinct_count=0, null_fraction=1.0),
                ],
            ),
        ],
        relationships=[],
        token_estimate=500,
    )


@pytest.fixture
def context(rich_scan_result: ScanResult) -> dict:
    return {"scan_result": rich_scan_result}


# ── Protocol Compliance ────────────────────────────────────────────


class TestProtocol:
    def test_implements_protocol(self) -> None:
        check = FilterValidationCheck()
        assert isinstance(check, CheckProtocol)

    def test_has_name(self) -> None:
        assert FilterValidationCheck().name == "filter_validation"


# ── WHERE Clause Extraction ────────────────────────────────────────


class TestExtractWhereClause:
    def test_simple_where(self) -> None:
        sql = "SELECT * FROM orders WHERE status = 'pending'"
        assert _extract_where_clause(sql) == "status = 'pending'"

    def test_no_where(self) -> None:
        assert _extract_where_clause("SELECT * FROM orders") is None

    def test_where_with_group_by(self) -> None:
        sql = "SELECT * FROM orders WHERE amount > 100 GROUP BY status"
        assert _extract_where_clause(sql) == "amount > 100"

    def test_where_with_order_by(self) -> None:
        sql = "SELECT * FROM orders WHERE id = 1 ORDER BY amount"
        assert _extract_where_clause(sql) == "id = 1"

    def test_complex_where(self) -> None:
        sql = "SELECT * FROM orders WHERE status = 'pending' AND amount > 100"
        clause = _extract_where_clause(sql)
        assert "status = 'pending'" in clause
        assert "amount > 100" in clause


# ── Comparison Checks ──────────────────────────────────────────────


class TestComparisons:
    def test_numeric_exceeds_max(self, context: dict) -> None:
        check = FilterValidationCheck()
        sql = "SELECT * FROM orders WHERE amount = 99999"
        result = check.run(sql, context)
        assert result.status == CheckStatus.WARNING
        assert "exceeds profiled max" in result.message

    def test_numeric_below_min(self, context: dict) -> None:
        check = FilterValidationCheck()
        sql = "SELECT * FROM orders WHERE amount = 1"
        result = check.run(sql, context)
        assert result.status == CheckStatus.WARNING
        assert "below profiled min" in result.message

    def test_numeric_in_range(self, context: dict) -> None:
        check = FilterValidationCheck()
        sql = "SELECT * FROM orders WHERE amount = 500"
        result = check.run(sql, context)
        assert result.status == CheckStatus.PASSED

    def test_string_enum_unknown_value(self, context: dict) -> None:
        check = FilterValidationCheck()
        sql = "SELECT * FROM orders WHERE status = 'refunded'"
        result = check.run(sql, context)
        assert result.status == CheckStatus.WARNING
        assert "not in sampled values" in result.message

    def test_string_enum_known_value(self, context: dict) -> None:
        check = FilterValidationCheck()
        sql = "SELECT * FROM orders WHERE status = 'pending'"
        result = check.run(sql, context)
        assert result.status == CheckStatus.PASSED

    def test_string_enum_case_insensitive(self, context: dict) -> None:
        check = FilterValidationCheck()
        sql = "SELECT * FROM orders WHERE status = 'Pending'"
        result = check.run(sql, context)
        assert result.status == CheckStatus.PASSED

    def test_high_cardinality_no_enum_check(self, context: dict) -> None:
        """High-cardinality columns (name) should not trigger enum warnings."""
        check = FilterValidationCheck()
        sql = "SELECT * FROM users WHERE name = 'UnknownPerson'"
        result = check.run(sql, context)
        assert result.status == CheckStatus.PASSED

    def test_greater_than_above_max(self, context: dict) -> None:
        check = FilterValidationCheck()
        sql = "SELECT * FROM users WHERE age > 100"
        result = check.run(sql, context)
        assert result.status == CheckStatus.WARNING
        assert "exceeds profiled max" in result.message

    def test_less_than_below_min(self, context: dict) -> None:
        check = FilterValidationCheck()
        sql = "SELECT * FROM users WHERE age < 10"
        result = check.run(sql, context)
        assert result.status == CheckStatus.WARNING
        assert "below profiled min" in result.message

    def test_not_equal_enum_unknown(self, context: dict) -> None:
        check = FilterValidationCheck()
        sql = "SELECT * FROM users WHERE role != 'superadmin'"
        result = check.run(sql, context)
        assert result.status == CheckStatus.WARNING
        assert "not in sampled values" in result.message


# ── NULL Filter Checks ─────────────────────────────────────────────


class TestNullFilters:
    def test_is_null_on_zero_null_column(self, context: dict) -> None:
        check = FilterValidationCheck()
        sql = "SELECT * FROM orders WHERE status IS NULL"
        result = check.run(sql, context)
        assert result.status == CheckStatus.WARNING
        assert "0% nulls" in result.message

    def test_is_not_null_on_all_null_column(self, context: dict) -> None:
        check = FilterValidationCheck()
        sql = "SELECT * FROM users WHERE deleted_at IS NOT NULL"
        result = check.run(sql, context)
        assert result.status == CheckStatus.WARNING
        assert "100% null" in result.message

    def test_is_null_on_nullable_column(self, context: dict) -> None:
        check = FilterValidationCheck()
        sql = "SELECT * FROM orders WHERE notes IS NULL"
        result = check.run(sql, context)
        assert result.status == CheckStatus.PASSED

    def test_is_not_null_on_partial_null_column(self, context: dict) -> None:
        check = FilterValidationCheck()
        sql = "SELECT * FROM orders WHERE notes IS NOT NULL"
        result = check.run(sql, context)
        assert result.status == CheckStatus.PASSED


# ── IN Filter Checks ──────────────────────────────────────────────


class TestInFilters:
    def test_in_with_known_enum_values(self, context: dict) -> None:
        check = FilterValidationCheck()
        sql = "SELECT * FROM orders WHERE status IN ('pending', 'shipped')"
        result = check.run(sql, context)
        assert result.status == CheckStatus.PASSED

    def test_in_with_unknown_enum_value(self, context: dict) -> None:
        check = FilterValidationCheck()
        sql = "SELECT * FROM orders WHERE status IN ('pending', 'returned')"
        result = check.run(sql, context)
        assert result.status == CheckStatus.WARNING
        assert "returned" in result.message

    def test_in_numeric_out_of_range(self, context: dict) -> None:
        check = FilterValidationCheck()
        sql = "SELECT * FROM orders WHERE amount IN (100, 99999)"
        result = check.run(sql, context)
        assert result.status == CheckStatus.WARNING
        assert "outside profiled range" in result.message

    def test_in_numeric_in_range(self, context: dict) -> None:
        check = FilterValidationCheck()
        sql = "SELECT * FROM orders WHERE amount IN (100, 500)"
        result = check.run(sql, context)
        assert result.status == CheckStatus.PASSED

    def test_in_with_role_known(self, context: dict) -> None:
        check = FilterValidationCheck()
        sql = "SELECT * FROM users WHERE role IN ('admin', 'editor')"
        result = check.run(sql, context)
        assert result.status == CheckStatus.PASSED


# ── BETWEEN Filter Checks ─────────────────────────────────────────


class TestBetweenFilters:
    def test_between_overlaps_profile(self, context: dict) -> None:
        check = FilterValidationCheck()
        sql = "SELECT * FROM orders WHERE amount BETWEEN 100 AND 3000"
        result = check.run(sql, context)
        assert result.status == CheckStatus.PASSED

    def test_between_entirely_above_max(self, context: dict) -> None:
        check = FilterValidationCheck()
        sql = "SELECT * FROM orders WHERE amount BETWEEN 6000 AND 9000"
        result = check.run(sql, context)
        assert result.status == CheckStatus.WARNING
        assert "no overlap" in result.message

    def test_between_entirely_below_min(self, context: dict) -> None:
        check = FilterValidationCheck()
        sql = "SELECT * FROM orders WHERE amount BETWEEN 1 AND 5"
        result = check.run(sql, context)
        assert result.status == CheckStatus.WARNING
        assert "no overlap" in result.message

    def test_between_partial_overlap(self, context: dict) -> None:
        check = FilterValidationCheck()
        sql = "SELECT * FROM orders WHERE amount BETWEEN 4000 AND 8000"
        result = check.run(sql, context)
        assert result.status == CheckStatus.PASSED


# ── Alias Resolution ──────────────────────────────────────────────


class TestAliasResolution:
    def test_alias_qualified_column(self, context: dict) -> None:
        check = FilterValidationCheck()
        sql = "SELECT * FROM orders o WHERE o.status = 'refunded'"
        result = check.run(sql, context)
        assert result.status == CheckStatus.WARNING
        assert "not in sampled values" in result.message

    def test_alias_numeric_range(self, context: dict) -> None:
        check = FilterValidationCheck()
        sql = "SELECT * FROM users u WHERE u.age = 200"
        result = check.run(sql, context)
        assert result.status == CheckStatus.WARNING
        assert "exceeds profiled max" in result.message


# ── No WHERE Clause ───────────────────────────────────────────────


class TestNoWhere:
    def test_no_where_passes(self, context: dict) -> None:
        check = FilterValidationCheck()
        sql = "SELECT * FROM orders"
        result = check.run(sql, context)
        assert result.status == CheckStatus.PASSED
        assert "No WHERE clause" in result.message


# ── Context Validation ────────────────────────────────────────────


class TestContextValidation:
    def test_missing_scan_result(self) -> None:
        check = FilterValidationCheck()
        with pytest.raises(Exception, match="scan_result"):
            check.run("SELECT 1", {})

    def test_invalid_scan_result(self) -> None:
        check = FilterValidationCheck()
        with pytest.raises(Exception, match="scan_result"):
            check.run("SELECT 1", {"scan_result": "not_a_scan_result"})


# ── Edge Cases ────────────────────────────────────────────────────


class TestEdgeCases:
    def test_unknown_column_skipped(self, context: dict) -> None:
        """Columns not in any profile are gracefully skipped."""
        check = FilterValidationCheck()
        sql = "SELECT * FROM orders WHERE unknown_col = 'foo'"
        result = check.run(sql, context)
        assert result.status == CheckStatus.PASSED

    def test_no_profile_data_skipped(self) -> None:
        """Tables without profiles don't cause errors."""
        scan = ScanResult(
            database_name="test",
            tables=[
                TableInfo(
                    name="items",
                    columns=[ColumnInfo(name="id", data_type="INTEGER")],
                    profiles=[],
                )
            ],
        )
        check = FilterValidationCheck()
        result = check.run(
            "SELECT * FROM items WHERE id = 999",
            {"scan_result": scan},
        )
        assert result.status == CheckStatus.PASSED

    def test_multiple_warnings_combined(self, context: dict) -> None:
        """Multiple issues in one query produce combined warnings."""
        check = FilterValidationCheck()
        sql = "SELECT * FROM orders WHERE status = 'bogus' AND amount = 99999"
        result = check.run(sql, context)
        assert result.status == CheckStatus.WARNING
        assert len(result.details["warnings"]) >= 2

    def test_like_not_flagged(self, context: dict) -> None:
        """LIKE patterns should not trigger enum warnings."""
        check = FilterValidationCheck()
        sql = "SELECT * FROM users WHERE name LIKE '%alice%'"
        result = check.run(sql, context)
        assert result.status == CheckStatus.PASSED

    def test_subquery_in_where_graceful(self, context: dict) -> None:
        """Subqueries in WHERE don't crash the parser."""
        check = FilterValidationCheck()
        sql = "SELECT * FROM orders WHERE user_id IN (SELECT id FROM users WHERE role = 'admin')"
        result = check.run(sql, context)
        # Should not crash — may or may not produce warnings
        assert result.status in (CheckStatus.PASSED, CheckStatus.WARNING)
