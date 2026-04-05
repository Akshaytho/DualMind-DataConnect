"""Tests for router graph-based relationship walking."""

from __future__ import annotations

import pytest

from dataconnect.models import (
    MatchMethod,
    RelationshipInfo,
    RelationshipType,
    TableMatch,
)
from dataconnect.router.graph import RelationshipGraph


# ── Fixtures ─────────────────────────────────────────────────────


def _fk(src_table: str, src_col: str, tgt_table: str, tgt_col: str,
        confidence: float = 1.0,
        rel_type: RelationshipType = RelationshipType.DECLARED_FK) -> RelationshipInfo:
    """Helper to create a RelationshipInfo."""
    return RelationshipInfo(
        source_table=src_table, source_column=src_col,
        target_table=tgt_table, target_column=tgt_col,
        relationship_type=rel_type, confidence=confidence,
    )


SAMPLE_RELS = [
    _fk("orders", "user_id", "users", "id"),
    _fk("orders", "product_id", "products", "id"),
    _fk("order_items", "order_id", "orders", "id"),
    _fk("reviews", "product_id", "products", "id"),
    _fk("reviews", "user_id", "users", "id"),
    _fk("categories", "parent_id", "categories", "id"),  # self-ref
]


@pytest.fixture
def graph() -> RelationshipGraph:
    """Graph built from sample relationships."""
    g = RelationshipGraph()
    g.build(SAMPLE_RELS)
    return g


# ── Build tests ──────────────────────────────────────────────────


class TestBuild:
    """Tests for graph construction."""

    def test_empty_relationships(self) -> None:
        """Empty relationships produce empty graph."""
        g = RelationshipGraph()
        g.build([])
        assert g.node_count == 0
        assert g.edge_count == 0

    def test_node_count(self, graph: RelationshipGraph) -> None:
        """All tables appear as nodes."""
        # users, orders, products, order_items, reviews, categories
        assert graph.node_count == 6

    def test_edge_count(self, graph: RelationshipGraph) -> None:
        """Edges are created (deduplicated by table pair)."""
        # orders-users, orders-products, order_items-orders,
        # reviews-products, reviews-users, categories-categories (self)
        assert graph.edge_count == 6

    def test_duplicate_edges_keep_highest_confidence(self) -> None:
        """When two relationships exist between same tables, keep highest confidence."""
        rels = [
            _fk("a", "c1", "b", "c2", confidence=0.5,
                 rel_type=RelationshipType.NAME_MATCH),
            _fk("a", "c3", "b", "c4", confidence=0.9,
                 rel_type=RelationshipType.DECLARED_FK),
        ]
        g = RelationshipGraph()
        g.build(rels)
        assert g.edge_count == 1
        assert g.node_count == 2

    def test_rebuild_clears_previous(self) -> None:
        """Rebuilding the graph clears previous state."""
        g = RelationshipGraph()
        g.build(SAMPLE_RELS)
        assert g.node_count == 6
        g.build([_fk("x", "a", "y", "b")])
        assert g.node_count == 2
        assert g.edge_count == 1


# ── Walk tests ───────────────────────────────────────────────────


class TestWalk:
    """Tests for graph walking from seed tables."""

    def test_empty_seeds(self, graph: RelationshipGraph) -> None:
        """Empty seed list returns no matches."""
        assert graph.walk([]) == []

    def test_unknown_seed(self, graph: RelationshipGraph) -> None:
        """Seed table not in graph returns no matches."""
        assert graph.walk(["nonexistent"]) == []

    def test_direct_neighbors_found(self, graph: RelationshipGraph) -> None:
        """Direct neighbors of seed are returned."""
        matches = graph.walk(["orders"], max_depth=1)
        found = {m.table_name for m in matches}
        # orders connects to: users, products, order_items
        assert "users" in found
        assert "products" in found
        assert "order_items" in found

    def test_seeds_excluded(self, graph: RelationshipGraph) -> None:
        """Seed tables are not in the results."""
        matches = graph.walk(["orders"], max_depth=2)
        found = {m.table_name for m in matches}
        assert "orders" not in found

    def test_depth_limits_walk(self, graph: RelationshipGraph) -> None:
        """Depth=1 only finds direct neighbors."""
        matches_d1 = graph.walk(["order_items"], max_depth=1)
        found_d1 = {m.table_name for m in matches_d1}
        # order_items -> orders (direct)
        assert "orders" in found_d1
        # users is 2 hops away (order_items -> orders -> users)
        assert "users" not in found_d1

    def test_depth_two_reaches_further(self, graph: RelationshipGraph) -> None:
        """Depth=2 reaches tables 2 hops away."""
        matches = graph.walk(["order_items"], max_depth=2)
        found = {m.table_name for m in matches}
        assert "users" in found
        assert "products" in found

    def test_match_method_is_graph_walk(self, graph: RelationshipGraph) -> None:
        """All matches use GRAPH_WALK method."""
        matches = graph.walk(["orders"], max_depth=1)
        for m in matches:
            assert MatchMethod.GRAPH_WALK in m.methods

    def test_scores_clamped(self, graph: RelationshipGraph) -> None:
        """Relevance scores are in [0, 1]."""
        matches = graph.walk(["orders"], max_depth=2)
        for m in matches:
            assert 0.0 <= m.relevance_score <= 1.0

    def test_closer_tables_score_higher(self, graph: RelationshipGraph) -> None:
        """Tables at depth 1 score higher than depth 2."""
        matches = graph.walk(["order_items"], max_depth=2)
        scores = {m.table_name: m.relevance_score for m in matches}
        # orders is depth 1, users is depth 2
        if "orders" in scores and "users" in scores:
            assert scores["orders"] >= scores["users"]

    def test_multiple_seeds(self, graph: RelationshipGraph) -> None:
        """Multiple seeds expand the discovered set."""
        single = graph.walk(["users"], max_depth=1)
        multi = graph.walk(["users", "products"], max_depth=1)
        assert len(multi) >= len(single)

    def test_has_reasoning(self, graph: RelationshipGraph) -> None:
        """Matches include reasoning text."""
        matches = graph.walk(["orders"], max_depth=1)
        for m in matches:
            assert "relationship graph" in m.reasoning

    def test_self_referencing_table(self) -> None:
        """Self-referencing relationships don't cause infinite loops."""
        rels = [_fk("categories", "parent_id", "categories", "id")]
        g = RelationshipGraph()
        g.build(rels)
        matches = g.walk(["categories"], max_depth=3)
        # categories is the seed, self-ref doesn't produce new tables
        assert len(matches) == 0

    def test_disconnected_graph(self) -> None:
        """Tables in disconnected components aren't found."""
        rels = [
            _fk("a", "x", "b", "y"),
            _fk("c", "x", "d", "y"),  # disconnected from a-b
        ]
        g = RelationshipGraph()
        g.build(rels)
        matches = g.walk(["a"], max_depth=5)
        found = {m.table_name for m in matches}
        assert "b" in found
        assert "c" not in found
        assert "d" not in found

    def test_low_confidence_edges(self) -> None:
        """Low confidence edges produce lower scores."""
        rels = [
            _fk("a", "x", "b", "y", confidence=0.3,
                 rel_type=RelationshipType.VALUE_OVERLAP),
        ]
        g = RelationshipGraph()
        g.build(rels)
        matches = g.walk(["a"], max_depth=1)
        assert len(matches) == 1
        assert matches[0].relevance_score <= 0.3
