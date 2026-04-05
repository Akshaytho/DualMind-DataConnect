"""Tests for scanner/profiler.py — data sampling and column profiling."""

from __future__ import annotations

import pytest
from sqlalchemy import Column, Integer, String, create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, Session

from dataconnect.models import ColumnInfo, ColumnProfile, TableInfo
from dataconnect.scanner.profiler import (
    _build_sample_query,
    _get_row_count,
    _profile_column,
    profile_table,
    profile_tables,
)
from dataconnect.exceptions import ProfilingError


# ── Fixtures ──────────────────────────────────────────────────────────


class _Base(DeclarativeBase):
    pass


class _Products(_Base):
    __tablename__ = "products"
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    price = Column(Integer, nullable=True)


@pytest.fixture
def profiler_engine() -> Engine:
    """Engine with products table — includes NULLs for profiling."""
    engine = create_engine("sqlite:///:memory:")
    _Base.metadata.create_all(engine)

    with Session(engine) as session:
        session.add_all([
            _Products(id=1, name="Widget", price=100),
            _Products(id=2, name="Gadget", price=200),
            _Products(id=3, name="Widget", price=None),
            _Products(id=4, name="Doohickey", price=50),
            _Products(id=5, name="Thingamajig", price=None),
        ])
        session.commit()
    return engine


@pytest.fixture
def products_table() -> TableInfo:
    """TableInfo with columns matching the products fixture."""
    return TableInfo(
        name="products",
        columns=[
            ColumnInfo(name="id", data_type="INTEGER", is_primary_key=True, nullable=False),
            ColumnInfo(name="name", data_type="VARCHAR(100)", nullable=False),
            ColumnInfo(name="price", data_type="INTEGER", nullable=True),
        ],
    )


@pytest.fixture
def empty_engine() -> Engine:
    """Engine with an empty table."""
    engine = create_engine("sqlite:///:memory:")
    _Base.metadata.create_all(engine)
    return engine


# ── Row count ─────────────────────────────────────────────────────────


def test_get_row_count(profiler_engine: Engine) -> None:
    count = _get_row_count(profiler_engine, "products")
    assert count == 5


def test_get_row_count_empty(empty_engine: Engine) -> None:
    count = _get_row_count(empty_engine, "products")
    assert count == 0


# ── Sample query building ────────────────────────────────────────────


def test_build_sample_query_sqlite(profiler_engine: Engine) -> None:
    query = _build_sample_query(profiler_engine, "products", 100, 5.0, 10000)
    assert "RANDOM()" in query
    assert "LIMIT" in query


def test_build_sample_query_limit_capped() -> None:
    """With a large table, limit should not exceed max_rows."""
    engine = create_engine("sqlite:///:memory:")
    query = _build_sample_query(engine, "big_table", 1_000_000, 5.0, 500)
    assert "LIMIT 500" in query


# ── Column profiling ─────────────────────────────────────────────────


def test_profile_column_basic() -> None:
    values = [1, 2, 3, None, 5]
    profile = _profile_column("col", values, 5)
    assert profile.column_name == "col"
    assert profile.null_fraction == 0.2
    assert profile.distinct_count == 4  # 1, 2, 3, 5
    assert profile.min_value is not None
    assert profile.max_value is not None


def test_profile_column_all_null() -> None:
    values = [None, None, None]
    profile = _profile_column("col", values, 3)
    assert profile.null_fraction == 1.0
    assert profile.distinct_count == 0
    assert profile.sample_values == []
    assert profile.min_value is None
    assert profile.max_value is None


def test_profile_column_empty() -> None:
    profile = _profile_column("col", [], 0)
    assert profile.null_fraction == 0.0
    assert profile.distinct_count == 0


def test_profile_column_sample_values_capped() -> None:
    """sample_values should have at most 10 entries."""
    values = list(range(50))
    profile = _profile_column("col", values, 50)
    assert len(profile.sample_values) <= 10


# ── Table profiling ──────────────────────────────────────────────────


def test_profile_table_row_count(profiler_engine: Engine, products_table: TableInfo) -> None:
    profile_table(profiler_engine, products_table, sample_pct=100.0)
    assert products_table.row_count_estimate == 5


def test_profile_table_profiles_created(profiler_engine: Engine, products_table: TableInfo) -> None:
    profile_table(profiler_engine, products_table, sample_pct=100.0)
    assert len(products_table.profiles) == 3
    names = [p.column_name for p in products_table.profiles]
    assert names == ["id", "name", "price"]


def test_profile_table_null_fraction(profiler_engine: Engine, products_table: TableInfo) -> None:
    profile_table(profiler_engine, products_table, sample_pct=100.0)
    price_profile = next(p for p in products_table.profiles if p.column_name == "price")
    assert price_profile.null_fraction == 0.4  # 2 nulls out of 5


def test_profile_table_distinct_count(profiler_engine: Engine, products_table: TableInfo) -> None:
    profile_table(profiler_engine, products_table, sample_pct=100.0)
    name_profile = next(p for p in products_table.profiles if p.column_name == "name")
    # "Widget" (x2), "Gadget", "Doohickey", "Thingamajig" = 4 distinct
    assert name_profile.distinct_count == 4


def test_profile_table_empty(empty_engine: Engine, products_table: TableInfo) -> None:
    profile_table(empty_engine, products_table, sample_pct=100.0)
    assert products_table.row_count_estimate == 0
    assert len(products_table.profiles) == 3
    for p in products_table.profiles:
        assert p.null_fraction == 0.0
        assert p.distinct_count == 0


def test_profile_table_min_max(profiler_engine: Engine, products_table: TableInfo) -> None:
    profile_table(profiler_engine, products_table, sample_pct=100.0)
    id_profile = next(p for p in products_table.profiles if p.column_name == "id")
    assert id_profile.min_value == "1"
    assert id_profile.max_value == "5"


# ── Batch profiling ──────────────────────────────────────────────────


def test_profile_tables_batch(profiler_engine: Engine, products_table: TableInfo) -> None:
    result = profile_tables(profiler_engine, [products_table])
    assert len(result) == 1
    assert result[0].row_count_estimate == 5
    assert len(result[0].profiles) == 3


def test_profile_tables_skips_bad_table(profiler_engine: Engine) -> None:
    """Bad table should be skipped, not crash the batch."""
    bad_table = TableInfo(
        name="nonexistent_table",
        columns=[ColumnInfo(name="x", data_type="INT")],
    )
    good_table = TableInfo(
        name="products",
        columns=[
            ColumnInfo(name="id", data_type="INTEGER"),
            ColumnInfo(name="name", data_type="VARCHAR(100)"),
            ColumnInfo(name="price", data_type="INTEGER"),
        ],
    )
    result = profile_tables(profiler_engine, [bad_table, good_table])
    # good_table should still be profiled
    assert good_table.row_count_estimate == 5


# ── Integration with sample_engine fixture ────────────────────────────


def test_profile_with_sample_engine(sample_engine: Engine) -> None:
    """Use the shared conftest sample_engine."""
    table = TableInfo(
        name="users",
        columns=[
            ColumnInfo(name="id", data_type="INTEGER"),
            ColumnInfo(name="name", data_type="VARCHAR(100)"),
            ColumnInfo(name="email", data_type="VARCHAR(255)"),
        ],
    )
    profile_table(sample_engine, table, sample_pct=100.0)
    assert table.row_count_estimate == 2
    email_profile = next(p for p in table.profiles if p.column_name == "email")
    assert email_profile.null_fraction == 0.5  # Bob has no email
