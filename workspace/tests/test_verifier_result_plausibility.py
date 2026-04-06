"""Tests for verifier Check 5: Result Plausibility."""

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
from dataconnect.verifier.result_plausibility import (
    ResultPlausibilityCheck,
    _build_table_lookup,
    _check_cartesian_product,
    _check_empty_tables,
    _check_high_null_columns,
    _check_select_star,
    _check_unbounded_results,
    _extract_from_tables_raw,
    _resolve_referenced_tables,
)


# ── Fixtures ────────────────────────────────────────────────────────


@pytest.fixture
def large_scan_result() -> ScanResult:
    """ScanResult with large and small tables for plausibility testing."""
    return ScanResult(
        database_name="test_db",
        tables=[
            TableInfo(
                name="orders",
                columns=[
                    ColumnInfo(name="id", data_type="INTEGER", is_primary_key=True),
                    ColumnInfo(name="user_id", data_type="INTEGER"),
                    ColumnInfo(name="amount", data_type="NUMERIC"),
                    ColumnInfo(name="status", data_type="VARCHAR(50)"),
                    ColumnInfo(name="notes", data_type="TEXT"),
                ],
                row_count_estimate=50_000,
                profiles=[
                    ColumnProfile(column_name="id", distinct_count=50000, null_fraction=0.0),
                    ColumnProfile(column_name="user_id", distinct_count=5000, null_fraction=0.0),
                    ColumnProfile(column_name="amount", distinct_count=1000, null_fraction=0.05),
                    ColumnProfile(column_name="status", distinct_count=4, null_fraction=0.0),
                    ColumnProfile(column_name="notes", distinct_count=100, null_fraction=0.95),
                ],
            ),
            TableInfo(
                name="users",
                columns=[
                    ColumnInfo(name="id", data_type="INTEGER", is_primary_key=True),
                    ColumnInfo(name="name", data_type="VARCHAR(100)"),
                    ColumnInfo(name="email", data_type="VARCHAR(200)"),
                ],
                row_count_estimate=5_000,
                profiles=[
                    ColumnProfile(column_name="id", distinct_count=5000, null_fraction=0.0),
                    ColumnProfile(column_name="name", distinct_count=4800, null_fraction=0.0),
                    ColumnProfile(column_name="email", distinct_count=5000, null_fraction=0.01),
                ],
            ),
            TableInfo(
                name="categories",
                columns=[
                    ColumnInfo(name="id", data_type="INTEGER", is_primary_key=True),
                    ColumnInfo(name="name", data_type="VARCHAR(50)"),
                ],
                row_count_estimate=10,
                profiles=[
                    ColumnProfile(column_name="id", distinct_count=10, null_fraction=0.0),
                    ColumnProfile(column_name="name", distinct_count=10, null_fraction=0.0),
                ],
            ),
            TableInfo(
                name="empty_table",
                columns=[
                    ColumnInfo(name="id", data_type="INTEGER", is_primary_key=True),
                    ColumnInfo(name="value", data_type="TEXT"),
                ],
                row_count_estimate=0,
                profiles=[],
            ),
        ],
    )


@pytest.fixture
def high_null_scan_result() -> ScanResult:
    """ScanResult where most columns are heavily null."""
    return ScanResult(
        database_name="test_db",
        tables=[
            TableInfo(
                name="sparse_data",
                columns=[
                    ColumnInfo(name="id", data_type="INTEGER"),
                    ColumnInfo(name="col_a", data_type="TEXT"),
                    ColumnInfo(name="col_b", data_type="TEXT"),
                    ColumnInfo(name="col_c", data_type="TEXT"),
                    ColumnInfo(name="col_d", data_type="TEXT"),
                ],
                row_count_estimate=1000,
                profiles=[
                    ColumnProfile(column_name="id", null_fraction=0.0, distinct_count=1000),
                    ColumnProfile(column_name="col_a", null_fraction=0.95, distinct_count=10),
                    ColumnProfile(column_name="col_b", null_fraction=0.98, distinct_count=5),
                    ColumnProfile(column_name="col_c", null_fraction=0.92, distinct_count=8),
                    ColumnProfile(column_name="col_d", null_fraction=0.1, distinct_count=500),
                ],
            ),
        ],
    )


@pytest.fixture
def check() -> ResultPlausibilityCheck:
    return ResultPlausibilityCheck()


def _ctx(scan_result: ScanResult) -> dict:
    return {"scan_result": scan_result}


# ── Protocol compliance ─────────────────────────────────────────────


class TestProtocol:
    def test_implements_protocol(self) -> None:
        assert isinstance(ResultPlausibilityCheck(), CheckProtocol)

    def test_name_property(self, check: ResultPlausibilityCheck) -> None:
        assert check.name == "result_plausibility"


# ── Context validation ──────────────────────────────────────────────


class TestContextValidation:
    def test_missing_scan_result_raises(self, check: ResultPlausibilityCheck) -> None:
        with pytest.raises(Exception, match="scan_result"):
            check.run("SELECT 1", {})

    def test_wrong_type_scan_result_raises(self, check: ResultPlausibilityCheck) -> None:
        with pytest.raises(Exception, match="scan_result"):
            check.run("SELECT 1", {"scan_result": "not_a_scan_result"})


# ── Helper: _build_table_lookup ─────────────────────────────────────


class TestBuildTableLookup:
    def test_lookup_keys_are_lowercase(self, large_scan_result: ScanResult) -> None:
        lookup = _build_table_lookup(large_scan_result)
        assert "orders" in lookup
        assert "users" in lookup
        assert "categories" in lookup

    def test_lookup_values_are_table_info(self, large_scan_result: ScanResult) -> None:
        lookup = _build_table_lookup(large_scan_result)
        assert lookup["orders"].row_count_estimate == 50_000


# ── Helper: _extract_from_tables_raw ────────────────────────────────


class TestExtractFromTablesRaw:
    def test_single_table(self) -> None:
        result = _extract_from_tables_raw("SELECT * FROM orders")
        assert result == ["orders"]

    def test_aliased_table(self) -> None:
        result = _extract_from_tables_raw("SELECT * FROM orders o")
        assert result == ["orders"]

    def test_comma_separated_tables(self) -> None:
        result = _extract_from_tables_raw("SELECT * FROM orders, users")
        assert len(result) == 2
        assert "orders" in result
        assert "users" in result

    def test_join_not_included(self) -> None:
        result = _extract_from_tables_raw(
            "SELECT * FROM orders o JOIN users u ON o.user_id = u.id"
        )
        assert result == ["orders"]

    def test_empty_query(self) -> None:
        result = _extract_from_tables_raw("SELECT 1")
        assert result == []


# ── Helper: _resolve_referenced_tables ──────────────────────────────


class TestResolveReferencedTables:
    def test_resolves_aliases(self, large_scan_result: ScanResult) -> None:
        lookup = _build_table_lookup(large_scan_result)
        aliases = {"o": "orders", "u": "users"}
        result = _resolve_referenced_tables(
            "SELECT * FROM orders o JOIN users u ON o.user_id = u.id",
            aliases, lookup,
        )
        assert "orders" in result
        assert "users" in result

    def test_ignores_unknown_tables(self, large_scan_result: ScanResult) -> None:
        lookup = _build_table_lookup(large_scan_result)
        aliases = {"x": "nonexistent"}
        result = _resolve_referenced_tables(
            "SELECT * FROM nonexistent x", aliases, lookup,
        )
        assert result == []


# ── Check: empty tables ─────────────────────────────────────────────


class TestEmptyTables:
    def test_empty_table_fails(self, large_scan_result: ScanResult) -> None:
        lookup = _build_table_lookup(large_scan_result)
        failures = _check_empty_tables(["empty_table"], lookup)
        assert len(failures) == 1
        assert "0 estimated rows" in failures[0]

    def test_nonempty_table_passes(self, large_scan_result: ScanResult) -> None:
        lookup = _build_table_lookup(large_scan_result)
        failures = _check_empty_tables(["orders"], lookup)
        assert failures == []

    def test_full_run_empty_table(
        self, check: ResultPlausibilityCheck, large_scan_result: ScanResult,
    ) -> None:
        result = check.run(
            "SELECT * FROM empty_table", _ctx(large_scan_result),
        )
        assert result.status == CheckStatus.FAILED
        assert "empty_table" in result.message


# ── Check: unbounded results ────────────────────────────────────────


class TestUnboundedResults:
    def test_large_table_no_limit_no_where_warns(
        self, large_scan_result: ScanResult,
    ) -> None:
        lookup = _build_table_lookup(large_scan_result)
        warnings = _check_unbounded_results(
            "SELECT id FROM orders", ["orders"], lookup,
        )
        assert len(warnings) == 1
        assert "No LIMIT or WHERE" in warnings[0]

    def test_large_table_with_limit_ok(
        self, large_scan_result: ScanResult,
    ) -> None:
        lookup = _build_table_lookup(large_scan_result)
        warnings = _check_unbounded_results(
            "SELECT id FROM orders LIMIT 100", ["orders"], lookup,
        )
        assert warnings == []

    def test_large_table_with_where_no_limit_warns(
        self, large_scan_result: ScanResult,
    ) -> None:
        lookup = _build_table_lookup(large_scan_result)
        warnings = _check_unbounded_results(
            "SELECT id FROM orders WHERE status = 'active'", ["orders"], lookup,
        )
        assert len(warnings) == 1
        assert "consider adding LIMIT" in warnings[0]

    def test_small_table_no_limit_ok(
        self, large_scan_result: ScanResult,
    ) -> None:
        lookup = _build_table_lookup(large_scan_result)
        warnings = _check_unbounded_results(
            "SELECT * FROM categories", ["categories"], lookup,
        )
        assert warnings == []

    def test_aggregate_query_skips_check(
        self, large_scan_result: ScanResult,
    ) -> None:
        lookup = _build_table_lookup(large_scan_result)
        warnings = _check_unbounded_results(
            "SELECT COUNT(*) FROM orders", ["orders"], lookup,
        )
        assert warnings == []


# ── Check: SELECT * ─────────────────────────────────────────────────


class TestSelectStar:
    def test_select_star_large_table_warns(
        self, large_scan_result: ScanResult,
    ) -> None:
        lookup = _build_table_lookup(large_scan_result)
        warnings = _check_select_star(
            "SELECT * FROM orders", ["orders"], lookup,
        )
        assert len(warnings) == 1
        assert "SELECT *" in warnings[0]
        assert "5 columns" in warnings[0]

    def test_select_star_small_table_ok(
        self, large_scan_result: ScanResult,
    ) -> None:
        lookup = _build_table_lookup(large_scan_result)
        warnings = _check_select_star(
            "SELECT * FROM categories", ["categories"], lookup,
        )
        assert warnings == []

    def test_select_star_with_reasonable_limit_ok(
        self, large_scan_result: ScanResult,
    ) -> None:
        lookup = _build_table_lookup(large_scan_result)
        warnings = _check_select_star(
            "SELECT * FROM orders LIMIT 10", ["orders"], lookup,
        )
        assert warnings == []

    def test_specific_columns_ok(
        self, large_scan_result: ScanResult,
    ) -> None:
        lookup = _build_table_lookup(large_scan_result)
        warnings = _check_select_star(
            "SELECT id, status FROM orders", ["orders"], lookup,
        )
        assert warnings == []

    def test_select_distinct_star_warns(
        self, large_scan_result: ScanResult,
    ) -> None:
        lookup = _build_table_lookup(large_scan_result)
        warnings = _check_select_star(
            "SELECT DISTINCT * FROM orders", ["orders"], lookup,
        )
        assert len(warnings) == 1


# ── Check: cartesian product ────────────────────────────────────────


class TestCartesianProduct:
    def test_comma_join_no_where_warns(
        self, large_scan_result: ScanResult,
    ) -> None:
        lookup = _build_table_lookup(large_scan_result)
        aliases = {"orders": "orders", "users": "users"}
        warnings = _check_cartesian_product(
            "SELECT * FROM orders, users", aliases, lookup,
        )
        assert len(warnings) == 1
        assert "cartesian product" in warnings[0].lower()

    def test_comma_join_with_where_ok(
        self, large_scan_result: ScanResult,
    ) -> None:
        lookup = _build_table_lookup(large_scan_result)
        aliases = {"orders": "orders", "users": "users"}
        warnings = _check_cartesian_product(
            "SELECT * FROM orders, users WHERE orders.user_id = users.id",
            aliases, lookup,
        )
        assert warnings == []

    def test_single_table_ok(
        self, large_scan_result: ScanResult,
    ) -> None:
        lookup = _build_table_lookup(large_scan_result)
        aliases = {"orders": "orders"}
        warnings = _check_cartesian_product(
            "SELECT * FROM orders", aliases, lookup,
        )
        assert warnings == []

    def test_proper_join_ok(
        self, large_scan_result: ScanResult,
    ) -> None:
        lookup = _build_table_lookup(large_scan_result)
        aliases = {"o": "orders", "u": "users"}
        warnings = _check_cartesian_product(
            "SELECT * FROM orders o JOIN users u ON o.user_id = u.id",
            aliases, lookup,
        )
        assert warnings == []


# ── Check: high null columns ────────────────────────────────────────


class TestHighNullColumns:
    def test_mostly_null_table_select_star_warns(
        self, high_null_scan_result: ScanResult,
    ) -> None:
        lookup = _build_table_lookup(high_null_scan_result)
        warnings = _check_high_null_columns(
            "SELECT * FROM sparse_data", ["sparse_data"], lookup,
        )
        assert len(warnings) == 1
        assert ">90% null" in warnings[0]

    def test_specific_columns_skip(
        self, high_null_scan_result: ScanResult,
    ) -> None:
        lookup = _build_table_lookup(high_null_scan_result)
        warnings = _check_high_null_columns(
            "SELECT id FROM sparse_data", ["sparse_data"], lookup,
        )
        assert warnings == []

    def test_aggregate_query_skips(
        self, high_null_scan_result: ScanResult,
    ) -> None:
        lookup = _build_table_lookup(high_null_scan_result)
        warnings = _check_high_null_columns(
            "SELECT COUNT(*) FROM sparse_data", ["sparse_data"], lookup,
        )
        assert warnings == []

    def test_healthy_table_ok(
        self, large_scan_result: ScanResult,
    ) -> None:
        lookup = _build_table_lookup(large_scan_result)
        warnings = _check_high_null_columns(
            "SELECT * FROM users", ["users"], lookup,
        )
        assert warnings == []


# ── Full integration tests ──────────────────────────────────────────


class TestFullIntegration:
    def test_simple_query_passes(
        self, check: ResultPlausibilityCheck, large_scan_result: ScanResult,
    ) -> None:
        result = check.run(
            "SELECT id, status FROM orders WHERE status = 'active' LIMIT 100",
            _ctx(large_scan_result),
        )
        assert result.status == CheckStatus.PASSED

    def test_aggregate_query_passes(
        self, check: ResultPlausibilityCheck, large_scan_result: ScanResult,
    ) -> None:
        result = check.run(
            "SELECT COUNT(*), status FROM orders GROUP BY status",
            _ctx(large_scan_result),
        )
        assert result.status == CheckStatus.PASSED

    def test_empty_table_query_fails(
        self, check: ResultPlausibilityCheck, large_scan_result: ScanResult,
    ) -> None:
        result = check.run(
            "SELECT * FROM empty_table", _ctx(large_scan_result),
        )
        assert result.status == CheckStatus.FAILED

    def test_select_star_large_warns(
        self, check: ResultPlausibilityCheck, large_scan_result: ScanResult,
    ) -> None:
        result = check.run(
            "SELECT * FROM orders", _ctx(large_scan_result),
        )
        assert result.status == CheckStatus.WARNING

    def test_cartesian_product_warns(
        self, check: ResultPlausibilityCheck, large_scan_result: ScanResult,
    ) -> None:
        result = check.run(
            "SELECT * FROM orders, users", _ctx(large_scan_result),
        )
        assert result.status == CheckStatus.WARNING
        assert "cartesian" in result.message.lower()

    def test_join_query_passes(
        self, check: ResultPlausibilityCheck, large_scan_result: ScanResult,
    ) -> None:
        result = check.run(
            "SELECT o.id, u.name FROM orders o JOIN users u ON o.user_id = u.id LIMIT 50",
            _ctx(large_scan_result),
        )
        assert result.status == CheckStatus.PASSED

    def test_small_table_select_star_passes(
        self, check: ResultPlausibilityCheck, large_scan_result: ScanResult,
    ) -> None:
        result = check.run(
            "SELECT * FROM categories", _ctx(large_scan_result),
        )
        assert result.status == CheckStatus.PASSED

    def test_high_null_select_star_warns(
        self, check: ResultPlausibilityCheck, high_null_scan_result: ScanResult,
    ) -> None:
        result = check.run(
            "SELECT * FROM sparse_data", _ctx(high_null_scan_result),
        )
        assert result.status == CheckStatus.WARNING
        assert ">90% null" in result.message

    def test_empty_table_with_join_fails(
        self, check: ResultPlausibilityCheck, large_scan_result: ScanResult,
    ) -> None:
        result = check.run(
            "SELECT * FROM empty_table e JOIN orders o ON e.id = o.id",
            _ctx(large_scan_result),
        )
        assert result.status == CheckStatus.FAILED

    def test_result_details_contain_warnings(
        self, check: ResultPlausibilityCheck, large_scan_result: ScanResult,
    ) -> None:
        result = check.run(
            "SELECT * FROM orders", _ctx(large_scan_result),
        )
        assert "warnings" in result.details

    def test_passed_result_has_message(
        self, check: ResultPlausibilityCheck, large_scan_result: ScanResult,
    ) -> None:
        result = check.run(
            "SELECT id FROM categories LIMIT 5", _ctx(large_scan_result),
        )
        assert result.status == CheckStatus.PASSED
        assert result.message != ""
