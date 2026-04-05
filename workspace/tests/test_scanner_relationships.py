"""Tests for scanner/relationships.py — name matching + value overlap."""

from __future__ import annotations

import pytest

from dataconnect.models import (
    ColumnInfo,
    ColumnProfile,
    RelationshipInfo,
    RelationshipType,
    TableInfo,
)
from dataconnect.scanner.relationships import (
    _jaccard_similarity,
    _singularize,
    discover_name_matches,
    discover_relationships,
    discover_value_overlaps,
)


# ── Helpers ──────────────────────────────────────────────────────────


def _make_table(
    name: str,
    columns: list[ColumnInfo],
    profiles: list[ColumnProfile] | None = None,
) -> TableInfo:
    """Build a TableInfo with optional profiles."""
    return TableInfo(
        name=name,
        columns=columns,
        profiles=profiles or [],
        row_count_estimate=100,
    )


def _users_table() -> TableInfo:
    return _make_table(
        "users",
        [
            ColumnInfo(name="id", data_type="INTEGER", is_primary_key=True, nullable=False),
            ColumnInfo(name="name", data_type="VARCHAR", nullable=False),
        ],
        [
            ColumnProfile(column_name="id", sample_values=["1", "2", "3", "4", "5"]),
            ColumnProfile(column_name="name", sample_values=["Alice", "Bob"]),
        ],
    )


def _orders_table() -> TableInfo:
    return _make_table(
        "orders",
        [
            ColumnInfo(name="id", data_type="INTEGER", is_primary_key=True, nullable=False),
            ColumnInfo(name="user_id", data_type="INTEGER", nullable=False),
            ColumnInfo(name="amount", data_type="INTEGER", nullable=False),
        ],
        [
            ColumnProfile(column_name="id", sample_values=["10", "20", "30"]),
            ColumnProfile(column_name="user_id", sample_values=["1", "2", "3"]),
            ColumnProfile(column_name="amount", sample_values=["100", "200", "50"]),
        ],
    )


def _products_table() -> TableInfo:
    return _make_table(
        "products",
        [
            ColumnInfo(name="id", data_type="INTEGER", is_primary_key=True, nullable=False),
            ColumnInfo(name="category_id", data_type="INTEGER", nullable=False),
        ],
        [
            ColumnProfile(column_name="id", sample_values=["100", "200", "300"]),
            ColumnProfile(column_name="category_id", sample_values=["10", "20"]),
        ],
    )


def _categories_table() -> TableInfo:
    return _make_table(
        "categories",
        [
            ColumnInfo(name="id", data_type="INTEGER", is_primary_key=True, nullable=False),
            ColumnInfo(name="name", data_type="VARCHAR", nullable=False),
        ],
        [
            ColumnProfile(column_name="id", sample_values=["10", "20", "30"]),
            ColumnProfile(column_name="name", sample_values=["Electronics", "Books"]),
        ],
    )


# ── Singularize tests ───────────────────────────────────────────────


class TestSingularize:
    def test_plural_s(self) -> None:
        assert _singularize("users") == "user"

    def test_plural_es(self) -> None:
        assert _singularize("boxes") == "box"

    def test_plural_ies(self) -> None:
        assert _singularize("categories") == "category"

    def test_already_singular(self) -> None:
        assert _singularize("address") == "address"

    def test_short_word(self) -> None:
        assert _singularize("as") == "a"

    def test_empty(self) -> None:
        assert _singularize("") == ""


# ── Jaccard similarity tests ────────────────────────────────────────


class TestJaccardSimilarity:
    def test_identical_lists(self) -> None:
        assert _jaccard_similarity(["1", "2", "3"], ["1", "2", "3"]) == 1.0

    def test_disjoint_lists(self) -> None:
        assert _jaccard_similarity(["1", "2"], ["3", "4"]) == 0.0

    def test_partial_overlap(self) -> None:
        # {1,2,3} & {2,3,4} = {2,3}, union = {1,2,3,4} → 2/4 = 0.5
        assert _jaccard_similarity(["1", "2", "3"], ["2", "3", "4"]) == 0.5

    def test_empty_first(self) -> None:
        assert _jaccard_similarity([], ["1", "2"]) == 0.0

    def test_empty_second(self) -> None:
        assert _jaccard_similarity(["1", "2"], []) == 0.0


# ── Name matching tests ─────────────────────────────────────────────


class TestNameMatches:
    def test_user_id_matches_users(self) -> None:
        tables = [_users_table(), _orders_table()]
        rels = discover_name_matches(tables, [])
        assert len(rels) == 1
        rel = rels[0]
        assert rel.source_table == "orders"
        assert rel.source_column == "user_id"
        assert rel.target_table == "users"
        assert rel.target_column == "id"
        assert rel.relationship_type == RelationshipType.NAME_MATCH
        assert rel.confidence == 0.75

    def test_category_id_matches_categories(self) -> None:
        tables = [_products_table(), _categories_table()]
        rels = discover_name_matches(tables, [])
        assert len(rels) == 1
        assert rels[0].source_column == "category_id"
        assert rels[0].target_table == "categories"

    def test_skips_existing_fk(self) -> None:
        tables = [_users_table(), _orders_table()]
        existing = [RelationshipInfo(
            source_table="orders",
            source_column="user_id",
            target_table="users",
            target_column="id",
            relationship_type=RelationshipType.DECLARED_FK,
            confidence=1.0,
        )]
        rels = discover_name_matches(tables, existing)
        assert len(rels) == 0

    def test_skips_pk_columns(self) -> None:
        """PK columns should not be matched as FK sources."""
        tables = [_users_table()]
        rels = discover_name_matches(tables, [])
        assert len(rels) == 0

    def test_no_self_reference(self) -> None:
        """A column named 'order_id' in table 'orders' should not self-ref."""
        table = _make_table(
            "orders",
            [
                ColumnInfo(name="id", data_type="INTEGER", is_primary_key=True, nullable=False),
                ColumnInfo(name="order_id", data_type="INTEGER", nullable=True),
            ],
        )
        rels = discover_name_matches([table], [])
        assert len(rels) == 0

    def test_no_match_for_random_column(self) -> None:
        tables = [_users_table(), _orders_table()]
        # 'amount' should not match anything
        amount_matches = [r for r in discover_name_matches(tables, []) if r.source_column == "amount"]
        assert len(amount_matches) == 0


# ── Value overlap tests ─────────────────────────────────────────────


class TestValueOverlap:
    def test_overlapping_values_detected(self) -> None:
        """user_id sample [1,2,3] overlaps with users.id [1,2,3,4,5]."""
        tables = [_users_table(), _orders_table()]
        rels = discover_value_overlaps(tables, [])
        user_id_rels = [r for r in rels if r.source_column == "user_id"]
        assert len(user_id_rels) == 1
        assert user_id_rels[0].target_table == "users"
        assert user_id_rels[0].relationship_type == RelationshipType.VALUE_OVERLAP

    def test_no_overlap_below_threshold(self) -> None:
        """amount [100,200,50] vs users.id [1,2,3,4,5] — no overlap."""
        tables = [_users_table(), _orders_table()]
        rels = discover_value_overlaps(tables, [])
        amount_rels = [r for r in rels if r.source_column == "amount"]
        assert len(amount_rels) == 0

    def test_skips_existing(self) -> None:
        tables = [_users_table(), _orders_table()]
        existing = [RelationshipInfo(
            source_table="orders",
            source_column="user_id",
            target_table="users",
            target_column="id",
            relationship_type=RelationshipType.NAME_MATCH,
            confidence=0.75,
        )]
        rels = discover_value_overlaps(tables, existing)
        user_id_rels = [r for r in rels if r.source_column == "user_id"]
        assert len(user_id_rels) == 0

    def test_empty_profiles_no_crash(self) -> None:
        table = _make_table(
            "empty",
            [ColumnInfo(name="id", data_type="INTEGER", is_primary_key=True, nullable=False)],
            [],
        )
        rels = discover_value_overlaps([table], [])
        assert rels == []

    def test_high_confidence_on_strong_overlap(self) -> None:
        """Jaccard >= 0.6 should give high confidence."""
        t1 = _make_table(
            "source",
            [ColumnInfo(name="ref_col", data_type="INTEGER", nullable=False)],
            [ColumnProfile(column_name="ref_col", sample_values=["1", "2", "3", "4"])],
        )
        t2 = _make_table(
            "target",
            [ColumnInfo(name="id", data_type="INTEGER", is_primary_key=True, nullable=False)],
            [ColumnProfile(column_name="id", sample_values=["1", "2", "3", "4", "5"])],
        )
        rels = discover_value_overlaps([t1, t2], [])
        assert len(rels) == 1
        assert rels[0].confidence == 0.85


# ── Integration: discover_relationships ──────────────────────────────


class TestDiscoverRelationships:
    def test_combines_name_and_value(self) -> None:
        tables = [_users_table(), _orders_table()]
        rels = discover_relationships(tables)
        # Name match finds user_id -> users.id
        # Value overlap should NOT duplicate it (dedup)
        assert len(rels) >= 1
        types = {r.relationship_type for r in rels}
        assert RelationshipType.NAME_MATCH in types

    def test_deduplicates_across_strategies(self) -> None:
        tables = [_users_table(), _orders_table()]
        rels = discover_relationships(tables)
        # user_id -> users.id should appear only once (name match wins, value overlap deduped)
        user_id_rels = [r for r in rels if r.source_column == "user_id" and r.target_table == "users"]
        assert len(user_id_rels) == 1

    def test_with_existing_fks(self) -> None:
        tables = [_users_table(), _orders_table()]
        existing = [RelationshipInfo(
            source_table="orders",
            source_column="user_id",
            target_table="users",
            target_column="id",
            relationship_type=RelationshipType.DECLARED_FK,
            confidence=1.0,
        )]
        rels = discover_relationships(tables, existing)
        user_id_rels = [r for r in rels if r.source_column == "user_id" and r.target_table == "users"]
        assert len(user_id_rels) == 0

    def test_empty_tables(self) -> None:
        rels = discover_relationships([])
        assert rels == []

    def test_no_profiles_still_finds_name_matches(self) -> None:
        """Name matching works even without profiles."""
        users = _make_table(
            "users",
            [ColumnInfo(name="id", data_type="INTEGER", is_primary_key=True, nullable=False)],
        )
        orders = _make_table(
            "orders",
            [
                ColumnInfo(name="id", data_type="INTEGER", is_primary_key=True, nullable=False),
                ColumnInfo(name="user_id", data_type="INTEGER", nullable=False),
            ],
        )
        rels = discover_relationships([users, orders])
        assert len(rels) == 1
        assert rels[0].relationship_type == RelationshipType.NAME_MATCH
