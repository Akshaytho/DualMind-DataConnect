"""Tests for verifier Check 3: Aggregation Validation."""

from __future__ import annotations

import pytest

from dataconnect.exceptions import VerificationError
from dataconnect.models import (
    CheckStatus,
    ColumnInfo,
    ScanResult,
    TableInfo,
)
from dataconnect.verifier.aggregation_validation import (
    AggregationValidationCheck,
    _check_aggregate_types,
    _check_group_by_completeness,
    _check_having_clause,
    _extract_group_by_columns,
    _extract_select_expressions,
    _is_aggregate_expression,
    _strip_alias,
)
from dataconnect.verifier.base import CheckProtocol


# ── Fixtures ───────────────────────────────────────────────────────

@pytest.fixture
def checker() -> AggregationValidationCheck:
    return AggregationValidationCheck()


@pytest.fixture
def scan_result() -> ScanResult:
    """ScanResult with numeric and text columns for type checking."""
    return ScanResult(
        database_name="test_db",
        tables=[
            TableInfo(
                name="orders",
                columns=[
                    ColumnInfo(name="id", data_type="integer"),
                    ColumnInfo(name="user_id", data_type="integer"),
                    ColumnInfo(name="amount", data_type="numeric"),
                    ColumnInfo(name="status", data_type="varchar"),
                    ColumnInfo(name="created_at", data_type="timestamp"),
                ],
            ),
            TableInfo(
                name="users",
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
                    ColumnInfo(name="category", data_type="varchar"),
                ],
            ),
        ],
    )


@pytest.fixture
def context(scan_result: ScanResult) -> dict:
    return {"scan_result": scan_result}


# ── Protocol compliance ────────────────────────────────────────────

class TestProtocol:
    def test_satisfies_check_protocol(self, checker: AggregationValidationCheck) -> None:
        assert isinstance(checker, CheckProtocol)

    def test_name_property(self, checker: AggregationValidationCheck) -> None:
        assert checker.name == "aggregation_validation"


# ── _extract_group_by_columns ──────────────────────────────────────

class TestExtractGroupBy:
    def test_no_group_by(self) -> None:
        assert _extract_group_by_columns("SELECT id FROM orders") == []

    def test_single_column(self) -> None:
        sql = "SELECT status, COUNT(*) FROM orders GROUP BY status"
        assert _extract_group_by_columns(sql) == ["status"]

    def test_multiple_columns(self) -> None:
        sql = "SELECT user_id, status, COUNT(*) FROM orders GROUP BY user_id, status"
        cols = _extract_group_by_columns(sql)
        assert cols == ["user_id", "status"]

    def test_qualified_column(self) -> None:
        sql = "SELECT o.user_id, COUNT(*) FROM orders o GROUP BY o.user_id"
        assert _extract_group_by_columns(sql) == ["o.user_id"]

    def test_positional_reference(self) -> None:
        sql = "SELECT status, COUNT(*) FROM orders GROUP BY 1"
        assert _extract_group_by_columns(sql) == ["1"]

    def test_with_having(self) -> None:
        sql = "SELECT status, COUNT(*) FROM orders GROUP BY status HAVING COUNT(*) > 5"
        assert _extract_group_by_columns(sql) == ["status"]

    def test_with_order_by(self) -> None:
        sql = "SELECT status, COUNT(*) FROM orders GROUP BY status ORDER BY status"
        assert _extract_group_by_columns(sql) == ["status"]


# ── _extract_select_expressions ────────────────────────────────────

class TestExtractSelectExpressions:
    def test_simple_columns(self) -> None:
        sql = "SELECT id, name FROM users"
        assert _extract_select_expressions(sql) == ["id", "name"]

    def test_star(self) -> None:
        sql = "SELECT * FROM users"
        assert _extract_select_expressions(sql) == ["*"]

    def test_with_aggregates(self) -> None:
        sql = "SELECT status, COUNT(*), SUM(amount) FROM orders"
        exprs = _extract_select_expressions(sql)
        assert len(exprs) == 3
        assert exprs[0] == "status"
        assert "COUNT" in exprs[1]
        assert "SUM" in exprs[2]

    def test_nested_parens(self) -> None:
        sql = "SELECT COALESCE(SUM(amount), 0), status FROM orders"
        exprs = _extract_select_expressions(sql)
        assert len(exprs) == 2

    def test_with_alias(self) -> None:
        sql = "SELECT status AS s, COUNT(*) AS cnt FROM orders"
        exprs = _extract_select_expressions(sql)
        assert len(exprs) == 2


# ── _is_aggregate_expression ──────────────────────────────────────

class TestIsAggregateExpression:
    def test_count_star(self) -> None:
        assert _is_aggregate_expression("COUNT(*)")

    def test_sum(self) -> None:
        assert _is_aggregate_expression("SUM(amount)")

    def test_bare_column(self) -> None:
        assert not _is_aggregate_expression("status")

    def test_case_insensitive(self) -> None:
        assert _is_aggregate_expression("count(*)")
        assert _is_aggregate_expression("Sum(amount)")


# ── _strip_alias ──────────────────────────────────────────────────

class TestStripAlias:
    def test_no_alias(self) -> None:
        assert _strip_alias("status") == "status"

    def test_as_alias(self) -> None:
        assert _strip_alias("status AS s") == "status"

    def test_complex_expr(self) -> None:
        assert _strip_alias("COUNT(*) AS cnt") == "COUNT(*)"


# ── _check_group_by_completeness ──────────────────────────────────

class TestGroupByCompleteness:
    def test_all_columns_grouped(self) -> None:
        sql = "SELECT status, COUNT(*) FROM orders GROUP BY status"
        group_by = _extract_group_by_columns(sql)
        issues = _check_group_by_completeness(sql, group_by)
        assert issues == []

    def test_missing_column_in_group_by(self) -> None:
        sql = "SELECT status, user_id, COUNT(*) FROM orders GROUP BY status"
        group_by = _extract_group_by_columns(sql)
        issues = _check_group_by_completeness(sql, group_by)
        assert len(issues) == 1
        assert "user_id" in issues[0]

    def test_all_aggregated(self) -> None:
        sql = "SELECT COUNT(*), SUM(amount) FROM orders"
        issues = _check_group_by_completeness(sql, [])
        assert issues == []

    def test_select_star_no_issues(self) -> None:
        sql = "SELECT * FROM orders GROUP BY id"
        group_by = _extract_group_by_columns(sql)
        issues = _check_group_by_completeness(sql, group_by)
        assert issues == []

    def test_qualified_column_matches_bare_group_by(self) -> None:
        sql = "SELECT o.status, COUNT(*) FROM orders o GROUP BY status"
        group_by = _extract_group_by_columns(sql)
        issues = _check_group_by_completeness(sql, group_by)
        assert issues == []


# ── _check_aggregate_types ─────────────────────────────────────────

class TestAggregateTypes:
    def test_sum_numeric_ok(self) -> None:
        type_lookup = {"orders": {"amount": "numeric"}}
        warnings = _check_aggregate_types(
            "SELECT SUM(amount) FROM orders", type_lookup, {},
        )
        assert warnings == []

    def test_sum_varchar_warns(self) -> None:
        type_lookup = {"orders": {"status": "varchar"}}
        warnings = _check_aggregate_types(
            "SELECT SUM(status) FROM orders", type_lookup, {},
        )
        assert len(warnings) == 1
        assert "non-numeric" in warnings[0]

    def test_avg_text_warns(self) -> None:
        type_lookup = {"users": {"name": "varchar"}}
        warnings = _check_aggregate_types(
            "SELECT AVG(name) FROM users", type_lookup, {},
        )
        assert len(warnings) == 1
        assert "AVG" in warnings[0]

    def test_count_anything_ok(self) -> None:
        type_lookup = {"orders": {"status": "varchar"}}
        warnings = _check_aggregate_types(
            "SELECT COUNT(status) FROM orders", type_lookup, {},
        )
        assert warnings == []

    def test_min_max_any_type_ok(self) -> None:
        type_lookup = {"orders": {"status": "varchar"}}
        warnings = _check_aggregate_types(
            "SELECT MIN(status), MAX(status) FROM orders", type_lookup, {},
        )
        assert warnings == []

    def test_qualified_column_with_alias(self) -> None:
        type_lookup = {"orders": {"amount": "numeric"}}
        aliases = {"o": "orders"}
        warnings = _check_aggregate_types(
            "SELECT SUM(o.amount) FROM orders o", type_lookup, aliases,
        )
        assert warnings == []

    def test_unresolvable_skipped(self) -> None:
        warnings = _check_aggregate_types(
            "SELECT SUM(x.y) FROM foo", {}, {},
        )
        assert warnings == []


# ── _check_having_clause ──────────────────────────────────────────

class TestHavingClause:
    def test_no_having(self) -> None:
        sql = "SELECT status, COUNT(*) FROM orders GROUP BY status"
        warnings = _check_having_clause(sql, ["status"])
        assert warnings == []

    def test_having_with_aggregate(self) -> None:
        sql = "SELECT status, COUNT(*) FROM orders GROUP BY status HAVING COUNT(*) > 5"
        warnings = _check_having_clause(sql, ["status"])
        assert warnings == []

    def test_having_with_group_by_col(self) -> None:
        sql = "SELECT status, COUNT(*) FROM orders GROUP BY status HAVING status = 'active'"
        warnings = _check_having_clause(sql, ["status"])
        assert warnings == []

    def test_having_with_ungrouped_col(self) -> None:
        sql = "SELECT status, COUNT(*) FROM orders GROUP BY status HAVING user_id > 10"
        warnings = _check_having_clause(sql, ["status"])
        assert len(warnings) == 1
        assert "user_id" in warnings[0]


# ── Full run() integration ─────────────────────────────────────────

class TestRunIntegration:
    def test_no_aggregation_passes(
        self, checker: AggregationValidationCheck, context: dict,
    ) -> None:
        result = checker.run("SELECT id, name FROM users", context)
        assert result.status == CheckStatus.PASSED
        assert "No aggregation" in result.message

    def test_correct_group_by_passes(
        self, checker: AggregationValidationCheck, context: dict,
    ) -> None:
        sql = "SELECT status, COUNT(*) FROM orders GROUP BY status"
        result = checker.run(sql, context)
        assert result.status == CheckStatus.PASSED

    def test_missing_group_by_column_fails(
        self, checker: AggregationValidationCheck, context: dict,
    ) -> None:
        sql = "SELECT status, user_id, COUNT(*) FROM orders GROUP BY status"
        result = checker.run(sql, context)
        assert result.status == CheckStatus.FAILED
        assert "user_id" in result.message

    def test_sum_on_text_warns(
        self, checker: AggregationValidationCheck, context: dict,
    ) -> None:
        sql = "SELECT SUM(status) FROM orders GROUP BY user_id"
        result = checker.run(sql, context)
        assert result.status == CheckStatus.WARNING
        assert "non-numeric" in result.message

    def test_count_star_no_group_by(
        self, checker: AggregationValidationCheck, context: dict,
    ) -> None:
        sql = "SELECT COUNT(*) FROM orders"
        result = checker.run(sql, context)
        assert result.status == CheckStatus.PASSED

    def test_group_by_with_having_passes(
        self, checker: AggregationValidationCheck, context: dict,
    ) -> None:
        sql = ("SELECT status, COUNT(*) FROM orders "
               "GROUP BY status HAVING COUNT(*) > 1")
        result = checker.run(sql, context)
        assert result.status == CheckStatus.PASSED

    def test_missing_context_raises(
        self, checker: AggregationValidationCheck,
    ) -> None:
        with pytest.raises(VerificationError, match="missing"):
            checker.run("SELECT 1", {})

    def test_wrong_context_type_raises(
        self, checker: AggregationValidationCheck,
    ) -> None:
        with pytest.raises(VerificationError, match="not a ScanResult"):
            checker.run("SELECT 1", {"scan_result": "not_a_scan_result"})

    def test_aggregate_only_no_group_by_passes(
        self, checker: AggregationValidationCheck, context: dict,
    ) -> None:
        sql = "SELECT COUNT(*), SUM(amount), AVG(amount) FROM orders"
        result = checker.run(sql, context)
        assert result.status == CheckStatus.PASSED

    def test_complex_query_with_join(
        self, checker: AggregationValidationCheck, context: dict,
    ) -> None:
        sql = ("SELECT u.name, COUNT(o.id) FROM users u "
               "JOIN orders o ON u.id = o.user_id "
               "GROUP BY u.name")
        result = checker.run(sql, context)
        assert result.status == CheckStatus.PASSED

    def test_multiple_issues_reported(
        self, checker: AggregationValidationCheck, context: dict,
    ) -> None:
        sql = "SELECT status, user_id, SUM(status) FROM orders GROUP BY user_id"
        result = checker.run(sql, context)
        # status not in GROUP BY → FAILED
        assert result.status == CheckStatus.FAILED
        assert "status" in result.message
