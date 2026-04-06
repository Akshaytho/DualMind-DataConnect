"""Tests for verifier Check 6: Completeness Audit."""

from __future__ import annotations

import pytest

from dataconnect.models import (
    CheckStatus,
    ColumnInfo,
    MatchMethod,
    RelationshipInfo,
    RelationshipType,
    RouteResult,
    ScanResult,
    TableInfo,
    TableMatch,
)
from dataconnect.verifier.base import CheckProtocol
from dataconnect.verifier.completeness_audit import (
    CompletenessAuditCheck,
    _build_adjacency,
    _check_router_suggestions,
    _extract_used_tables,
    _find_missing_neighbors,
)


# ── Fixtures ────────────────────────────────────────────────────────


def _make_table(name: str, row_count: int = 100) -> TableInfo:
    """Helper to build minimal TableInfo."""
    return TableInfo(
        name=name,
        columns=[
            ColumnInfo(name="id", data_type="INTEGER", is_primary_key=True),
            ColumnInfo(name="name", data_type="VARCHAR(100)"),
        ],
        row_count_estimate=row_count,
    )


@pytest.fixture
def scan_result() -> ScanResult:
    """ScanResult with related tables for completeness testing."""
    return ScanResult(
        database_name="test_db",
        tables=[
            _make_table("orders", 5000),
            _make_table("users", 1000),
            _make_table("products", 2000),
            _make_table("order_items", 15000),
            _make_table("categories", 50),
            _make_table("reviews", 3000),
            _make_table("empty_table", 0),
        ],
        relationships=[
            RelationshipInfo(
                source_table="orders",
                source_column="user_id",
                target_table="users",
                target_column="id",
                relationship_type=RelationshipType.DECLARED_FK,
                confidence=1.0,
            ),
            RelationshipInfo(
                source_table="order_items",
                source_column="order_id",
                target_table="orders",
                target_column="id",
                relationship_type=RelationshipType.DECLARED_FK,
                confidence=1.0,
            ),
            RelationshipInfo(
                source_table="order_items",
                source_column="product_id",
                target_table="products",
                target_column="id",
                relationship_type=RelationshipType.DECLARED_FK,
                confidence=1.0,
            ),
            RelationshipInfo(
                source_table="products",
                source_column="category_id",
                target_table="categories",
                target_column="id",
                relationship_type=RelationshipType.DECLARED_FK,
                confidence=0.9,
            ),
            RelationshipInfo(
                source_table="reviews",
                source_column="product_id",
                target_table="products",
                target_column="id",
                relationship_type=RelationshipType.NAME_MATCH,
                confidence=0.7,
            ),
            # Low confidence — should be ignored
            RelationshipInfo(
                source_table="empty_table",
                source_column="x",
                target_table="orders",
                target_column="id",
                relationship_type=RelationshipType.VALUE_OVERLAP,
                confidence=0.3,
            ),
        ],
    )


@pytest.fixture
def check() -> CompletenessAuditCheck:
    return CompletenessAuditCheck()


def _ctx(scan_result: ScanResult, **extra: object) -> dict:
    return {"scan_result": scan_result, **extra}


# ── Protocol Compliance ─────────────────────────────────────────────


class TestProtocol:
    def test_implements_check_protocol(self, check: CompletenessAuditCheck) -> None:
        assert isinstance(check, CheckProtocol)

    def test_name(self, check: CompletenessAuditCheck) -> None:
        assert check.name == "completeness_audit"


# ── Context Validation ──────────────────────────────────────────────


class TestContextValidation:
    def test_missing_scan_result_raises(self, check: CompletenessAuditCheck) -> None:
        with pytest.raises(Exception, match="scan_result"):
            check.run("SELECT 1", {})

    def test_wrong_type_scan_result_raises(
        self, check: CompletenessAuditCheck
    ) -> None:
        with pytest.raises(Exception, match="scan_result"):
            check.run("SELECT 1", {"scan_result": "not_a_scan_result"})


# ── Extract Used Tables ─────────────────────────────────────────────


class TestExtractUsedTables:
    def test_simple_from(self, scan_result: ScanResult) -> None:
        lookup = {t.name.lower(): t for t in scan_result.tables}
        aliases = {"orders": "orders"}
        result = _extract_used_tables(
            "SELECT * FROM orders", aliases, lookup
        )
        assert result == {"orders"}

    def test_aliased_table(self, scan_result: ScanResult) -> None:
        lookup = {t.name.lower(): t for t in scan_result.tables}
        aliases = {"o": "orders", "orders": "orders"}
        result = _extract_used_tables(
            "SELECT o.id FROM orders o", aliases, lookup
        )
        assert result == {"orders"}

    def test_join_tables(self, scan_result: ScanResult) -> None:
        lookup = {t.name.lower(): t for t in scan_result.tables}
        aliases = {"orders": "orders", "users": "users"}
        result = _extract_used_tables(
            "SELECT * FROM orders JOIN users ON orders.user_id = users.id",
            aliases,
            lookup,
        )
        assert result == {"orders", "users"}

    def test_multiple_joins(self, scan_result: ScanResult) -> None:
        lookup = {t.name.lower(): t for t in scan_result.tables}
        aliases = {
            "o": "orders",
            "orders": "orders",
            "u": "users",
            "users": "users",
            "oi": "order_items",
            "order_items": "order_items",
        }
        sql = (
            "SELECT o.id FROM orders o "
            "JOIN users u ON o.user_id = u.id "
            "JOIN order_items oi ON o.id = oi.order_id"
        )
        result = _extract_used_tables(sql, aliases, lookup)
        assert result == {"orders", "users", "order_items"}

    def test_unknown_table_ignored(self, scan_result: ScanResult) -> None:
        lookup = {t.name.lower(): t for t in scan_result.tables}
        aliases = {"nonexistent": "nonexistent"}
        result = _extract_used_tables(
            "SELECT * FROM nonexistent", aliases, lookup
        )
        assert result == set()


# ── Build Adjacency ─────────────────────────────────────────────────


class TestBuildAdjacency:
    def test_bidirectional(self, scan_result: ScanResult) -> None:
        adj = _build_adjacency(scan_result.relationships)
        assert "users" in adj.get("orders", set())
        assert "orders" in adj.get("users", set())

    def test_low_confidence_excluded(self, scan_result: ScanResult) -> None:
        adj = _build_adjacency(scan_result.relationships)
        # empty_table has confidence=0.3 — below threshold
        assert "empty_table" not in adj.get("orders", set())

    def test_empty_relationships(self) -> None:
        adj = _build_adjacency([])
        assert adj == {}

    def test_threshold_boundary(self) -> None:
        rels = [
            RelationshipInfo(
                source_table="a",
                source_column="id",
                target_table="b",
                target_column="a_id",
                relationship_type=RelationshipType.NAME_MATCH,
                confidence=0.5,
            ),
        ]
        adj = _build_adjacency(rels)
        assert "b" in adj.get("a", set())

    def test_below_threshold(self) -> None:
        rels = [
            RelationshipInfo(
                source_table="a",
                source_column="id",
                target_table="b",
                target_column="a_id",
                relationship_type=RelationshipType.NAME_MATCH,
                confidence=0.49,
            ),
        ]
        adj = _build_adjacency(rels)
        assert adj == {}


# ── Find Missing Neighbors ──────────────────────────────────────────


class TestFindMissingNeighbors:
    def test_finds_related_unused_table(self, scan_result: ScanResult) -> None:
        adj = _build_adjacency(scan_result.relationships)
        lookup = {t.name.lower(): t for t in scan_result.tables}
        # Query uses orders but not users (which is related)
        missing = _find_missing_neighbors({"orders"}, adj, lookup)
        assert "users" in missing
        assert "order_items" in missing

    def test_all_neighbors_used(self, scan_result: ScanResult) -> None:
        adj = _build_adjacency(scan_result.relationships)
        lookup = {t.name.lower(): t for t in scan_result.tables}
        # Query uses orders, users, and order_items — all neighbors of orders
        missing = _find_missing_neighbors(
            {"orders", "users", "order_items"}, adj, lookup
        )
        # products is a neighbor of order_items but not of orders directly
        assert "orders" not in missing
        assert "users" not in missing
        assert "order_items" not in missing

    def test_empty_table_excluded(self, scan_result: ScanResult) -> None:
        """Tables with 0 rows are not flagged as missing."""
        adj = _build_adjacency(scan_result.relationships)
        # Force empty_table into adjacency for this test
        adj.setdefault("orders", set()).add("empty_table")
        lookup = {t.name.lower(): t for t in scan_result.tables}
        missing = _find_missing_neighbors({"orders"}, adj, lookup)
        assert "empty_table" not in missing

    def test_no_relationships(self) -> None:
        adj: dict[str, set[str]] = {}
        lookup = {"orders": _make_table("orders")}
        missing = _find_missing_neighbors({"orders"}, adj, lookup)
        assert missing == []

    def test_sorted_output(self, scan_result: ScanResult) -> None:
        adj = _build_adjacency(scan_result.relationships)
        lookup = {t.name.lower(): t for t in scan_result.tables}
        missing = _find_missing_neighbors({"orders"}, adj, lookup)
        assert missing == sorted(missing)


# ── Router Suggestions ──────────────────────────────────────────────


class TestRouterSuggestions:
    def test_no_route_result(self) -> None:
        result = _check_router_suggestions({"orders"}, {})
        assert result == []

    def test_all_suggestions_used(self) -> None:
        route = RouteResult(
            query="test",
            matched_tables=[
                TableMatch(table_name="orders", methods=[MatchMethod.EMBEDDING]),
            ],
        )
        result = _check_router_suggestions(
            {"orders"}, {"route_result": route}
        )
        assert result == []

    def test_unused_suggestion_flagged(self) -> None:
        route = RouteResult(
            query="test",
            matched_tables=[
                TableMatch(table_name="orders", methods=[MatchMethod.EMBEDDING]),
                TableMatch(table_name="users", methods=[MatchMethod.GRAPH_WALK]),
            ],
        )
        result = _check_router_suggestions(
            {"orders"}, {"route_result": route}
        )
        assert result == ["users"]

    def test_dict_route_result(self) -> None:
        route = {
            "matched_tables": [
                {"table_name": "orders"},
                {"table_name": "products"},
            ],
        }
        result = _check_router_suggestions(
            {"orders"}, {"route_result": route}
        )
        assert result == ["products"]

    def test_case_insensitive(self) -> None:
        route = RouteResult(
            query="test",
            matched_tables=[
                TableMatch(table_name="Orders", methods=[MatchMethod.EMBEDDING]),
            ],
        )
        result = _check_router_suggestions(
            {"orders"}, {"route_result": route}
        )
        assert result == []


# ── Full Integration ────────────────────────────────────────────────


class TestFullIntegration:
    def test_passes_when_all_related_used(
        self, check: CompletenessAuditCheck, scan_result: ScanResult
    ) -> None:
        """Query uses orders + users + order_items — all neighbors covered."""
        sql = (
            "SELECT o.id, u.name, oi.product_id "
            "FROM orders o "
            "JOIN users u ON o.user_id = u.id "
            "JOIN order_items oi ON o.id = oi.order_id"
        )
        result = check.run(sql, _ctx(scan_result))
        # products is neighbor of order_items — still flagged as warning
        assert result.status in (CheckStatus.PASSED, CheckStatus.WARNING)

    def test_warns_on_missing_related_table(
        self, check: CompletenessAuditCheck, scan_result: ScanResult
    ) -> None:
        """Query uses orders but not users/order_items."""
        sql = "SELECT * FROM orders WHERE id = 1"
        result = check.run(sql, _ctx(scan_result))
        assert result.status == CheckStatus.WARNING
        assert "missing_related" in result.details
        missing = result.details["missing_related"]
        assert "users" in missing
        assert "order_items" in missing

    def test_warns_on_router_missed(
        self, check: CompletenessAuditCheck, scan_result: ScanResult
    ) -> None:
        """Router suggested tables that SQL doesn't use."""
        sql = "SELECT * FROM orders WHERE id = 1"
        route = RouteResult(
            query="test",
            matched_tables=[
                TableMatch(table_name="orders", methods=[MatchMethod.EMBEDDING]),
                TableMatch(table_name="reviews", methods=[MatchMethod.LLM_CROSSCHECK]),
            ],
        )
        result = check.run(sql, _ctx(scan_result, route_result=route))
        assert result.status == CheckStatus.WARNING
        assert "reviews" in result.details.get("router_missed", [])

    def test_no_tables_recognized(
        self, check: CompletenessAuditCheck, scan_result: ScanResult
    ) -> None:
        """Query references no known tables — passes (nothing to audit)."""
        sql = "SELECT 1"
        result = check.run(sql, _ctx(scan_result))
        assert result.status == CheckStatus.PASSED

    def test_single_isolated_table(
        self, check: CompletenessAuditCheck
    ) -> None:
        """Table with no relationships — passes."""
        sr = ScanResult(
            database_name="test_db",
            tables=[_make_table("standalone", 500)],
            relationships=[],
        )
        sql = "SELECT * FROM standalone"
        result = check.run(sql, _ctx(sr))
        assert result.status == CheckStatus.PASSED

    def test_never_fails(
        self, check: CompletenessAuditCheck, scan_result: ScanResult
    ) -> None:
        """Completeness audit should never FAIL — only WARN or PASS."""
        sql = "SELECT * FROM orders"
        result = check.run(sql, _ctx(scan_result))
        assert result.status != CheckStatus.FAILED

    def test_used_tables_in_details(
        self, check: CompletenessAuditCheck, scan_result: ScanResult
    ) -> None:
        """Details always include which tables were detected as used."""
        sql = "SELECT * FROM orders JOIN users ON orders.user_id = users.id"
        result = check.run(sql, _ctx(scan_result))
        assert "used_tables" in result.details

    def test_low_confidence_relationships_ignored(
        self, check: CompletenessAuditCheck
    ) -> None:
        """Low-confidence relationships should not trigger warnings."""
        sr = ScanResult(
            database_name="test_db",
            tables=[_make_table("a", 100), _make_table("b", 100)],
            relationships=[
                RelationshipInfo(
                    source_table="a",
                    source_column="id",
                    target_table="b",
                    target_column="a_id",
                    relationship_type=RelationshipType.VALUE_OVERLAP,
                    confidence=0.3,
                ),
            ],
        )
        sql = "SELECT * FROM a"
        result = check.run(sql, _ctx(sr))
        assert result.status == CheckStatus.PASSED

    def test_message_contains_table_names(
        self, check: CompletenessAuditCheck, scan_result: ScanResult
    ) -> None:
        """Warning message should mention the missing table names."""
        sql = "SELECT * FROM orders WHERE id = 1"
        result = check.run(sql, _ctx(scan_result))
        assert result.status == CheckStatus.WARNING
        assert "users" in result.message or "order_items" in result.message

    def test_chain_relationship_depth_one(
        self, check: CompletenessAuditCheck, scan_result: ScanResult
    ) -> None:
        """Only one-hop neighbors are flagged, not two-hop."""
        # categories is related to products, not directly to orders
        sql = "SELECT * FROM orders WHERE id = 1"
        result = check.run(sql, _ctx(scan_result))
        missing = result.details.get("missing_related", [])
        # categories should NOT appear — it's two hops from orders
        assert "categories" not in missing

    def test_multiple_from_tables(
        self, check: CompletenessAuditCheck, scan_result: ScanResult
    ) -> None:
        """Query using comma-separated tables."""
        sql = "SELECT * FROM orders, users WHERE orders.user_id = users.id"
        result = check.run(sql, _ctx(scan_result))
        # order_items is neighbor of orders but used_tables has both
        used = result.details.get("used_tables", [])
        assert "orders" in used
        assert "users" in used
