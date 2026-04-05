"""Tests for scanner orchestration — scan_database() pipeline."""

from __future__ import annotations

from unittest.mock import patch

import pytest
from sqlalchemy import Column, ForeignKey, Integer, String, create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, Session

from dataconnect.exceptions import ScanError
from dataconnect.models import (
    ColumnInfo,
    RelationshipInfo,
    RelationshipType,
    ScanResult,
    TableInfo,
)
from dataconnect.scanner import (
    _estimate_tokens,
    _extract_database_name,
    scan_database,
)


# ── Fixtures ──────────────────────────────────────────────────────


class FKBase(DeclarativeBase):
    """Base with foreign keys for integration tests."""


class FKUsers(FKBase):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)


class FKOrders(FKBase):
    __tablename__ = "orders"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    amount = Column(Integer, nullable=False)


class FKProducts(FKBase):
    __tablename__ = "products"
    id = Column(Integer, primary_key=True)
    title = Column(String(200), nullable=False)
    price = Column(Integer, nullable=False)


@pytest.fixture
def fk_engine() -> Engine:
    """Engine with FK relationships and data for full pipeline test."""
    engine = create_engine("sqlite:///:memory:")
    FKBase.metadata.create_all(engine)

    with Session(engine) as session:
        session.add_all([
            FKUsers(id=1, name="Alice"),
            FKUsers(id=2, name="Bob"),
            FKUsers(id=3, name="Charlie"),
        ])
        session.add_all([
            FKOrders(id=1, user_id=1, amount=100),
            FKOrders(id=2, user_id=1, amount=200),
            FKOrders(id=3, user_id=2, amount=50),
        ])
        session.add_all([
            FKProducts(id=1, title="Widget", price=25),
            FKProducts(id=2, title="Gadget", price=75),
        ])
        session.commit()

    return engine


@pytest.fixture
def empty_engine() -> Engine:
    """Engine with no tables."""
    return create_engine("sqlite:///:memory:")


# ── Token estimation tests ────────────────────────────────────────


class TestEstimateTokens:
    """Tests for _estimate_tokens helper."""

    def test_empty_input(self) -> None:
        """Empty tables and relationships produce minimal tokens."""
        assert _estimate_tokens([], []) == 1

    def test_single_table(self) -> None:
        """Single table with columns produces reasonable estimate."""
        tables = [
            TableInfo(
                name="users",
                columns=[
                    ColumnInfo(name="id", data_type="INTEGER"),
                    ColumnInfo(name="name", data_type="VARCHAR(100)"),
                ],
            ),
        ]
        tokens = _estimate_tokens(tables, [])
        assert tokens > 0
        assert tokens < 500  # sanity cap for 2-column table

    def test_relationships_add_tokens(self) -> None:
        """Relationships increase the token estimate."""
        tables = [TableInfo(name="t", columns=[])]
        rels = [
            RelationshipInfo(
                source_table="orders",
                source_column="user_id",
                target_table="users",
                target_column="id",
                relationship_type=RelationshipType.DECLARED_FK,
                confidence=1.0,
            ),
        ]
        tokens_without = _estimate_tokens(tables, [])
        tokens_with = _estimate_tokens(tables, rels)
        assert tokens_with > tokens_without

    def test_descriptions_add_tokens(self) -> None:
        """Table/column descriptions increase token count."""
        tables_no_desc = [
            TableInfo(name="t", columns=[
                ColumnInfo(name="c", data_type="INT"),
            ]),
        ]
        tables_with_desc = [
            TableInfo(name="t", description="A very important table", columns=[
                ColumnInfo(name="c", data_type="INT", description="The key column"),
            ]),
        ]
        assert _estimate_tokens(tables_with_desc, []) > _estimate_tokens(tables_no_desc, [])


# ── Database name extraction tests ────────────────────────────────


class TestExtractDatabaseName:
    """Tests for _extract_database_name helper."""

    def test_memory_sqlite(self) -> None:
        """In-memory SQLite returns dialect name."""
        engine = create_engine("sqlite:///:memory:")
        name = _extract_database_name(engine)
        # :memory: or sqlite — both are acceptable
        assert name in (":memory:", "sqlite")

    def test_file_sqlite(self, tmp_path) -> None:
        """File-based SQLite extracts filename without .db."""
        db_path = tmp_path / "mydata.db"
        engine = create_engine(f"sqlite:///{db_path}")
        name = _extract_database_name(engine)
        assert name == "mydata"

    def test_file_sqlite_no_extension(self, tmp_path) -> None:
        """File without .db extension keeps full name."""
        db_path = tmp_path / "mydata"
        engine = create_engine(f"sqlite:///{db_path}")
        name = _extract_database_name(engine)
        assert name == "mydata"


# ── Full pipeline tests ───────────────────────────────────────────


class TestScanDatabase:
    """Integration tests for scan_database() pipeline."""

    def test_returns_scan_result(self, fk_engine: Engine) -> None:
        """scan_database returns a valid ScanResult."""
        result = scan_database(fk_engine, database_name="test_db")
        assert isinstance(result, ScanResult)
        assert result.database_name == "test_db"

    def test_discovers_all_tables(self, fk_engine: Engine) -> None:
        """All tables are discovered."""
        result = scan_database(fk_engine, database_name="test_db")
        table_names = {t.name for t in result.tables}
        assert "users" in table_names
        assert "orders" in table_names
        assert "products" in table_names
        assert len(result.tables) == 3

    def test_profiles_populated(self, fk_engine: Engine) -> None:
        """Tables have profiling data after scan."""
        result = scan_database(fk_engine, database_name="test_db")
        for table in result.tables:
            assert len(table.profiles) > 0
            assert table.row_count_estimate > 0

    def test_row_counts_correct(self, fk_engine: Engine) -> None:
        """Row count estimates match actual data."""
        result = scan_database(fk_engine, database_name="test_db")
        counts = {t.name: t.row_count_estimate for t in result.tables}
        assert counts["users"] == 3
        assert counts["orders"] == 3
        assert counts["products"] == 2

    def test_fk_relationships_found(self, fk_engine: Engine) -> None:
        """Declared FK relationships are captured."""
        result = scan_database(fk_engine, database_name="test_db")
        fk_rels = [
            r for r in result.relationships
            if r.relationship_type == RelationshipType.DECLARED_FK
        ]
        assert len(fk_rels) >= 1
        # orders.user_id -> users.id
        fk_sources = {(r.source_table, r.source_column) for r in fk_rels}
        assert ("orders", "user_id") in fk_sources

    def test_discovered_relationships_included(self, fk_engine: Engine) -> None:
        """Name-match or value-overlap relationships may be found."""
        result = scan_database(fk_engine, database_name="test_db")
        # At least the declared FK should be present
        assert len(result.relationships) >= 1

    def test_token_estimate_positive(self, fk_engine: Engine) -> None:
        """Token estimate is positive for non-empty database."""
        result = scan_database(fk_engine, database_name="test_db")
        assert result.token_estimate > 0

    def test_token_estimate_reasonable(self, fk_engine: Engine) -> None:
        """Token estimate is within a reasonable range for small DB."""
        result = scan_database(fk_engine, database_name="test_db")
        # 3 tables, ~10 columns total → should be under 1000 tokens
        assert result.token_estimate < 1000

    def test_scanned_at_set(self, fk_engine: Engine) -> None:
        """scanned_at timestamp is set."""
        result = scan_database(fk_engine, database_name="test_db")
        assert result.scanned_at is not None

    def test_empty_database(self, empty_engine: Engine) -> None:
        """Empty database produces empty ScanResult."""
        result = scan_database(empty_engine, database_name="empty")
        assert result.database_name == "empty"
        assert result.tables == []
        assert result.relationships == []
        assert result.token_estimate == 0

    def test_auto_database_name(self, fk_engine: Engine) -> None:
        """Database name is auto-detected when not provided."""
        result = scan_database(fk_engine)
        assert result.database_name  # not empty
        assert isinstance(result.database_name, str)

    def test_custom_sample_params(self, fk_engine: Engine) -> None:
        """Custom sampling parameters are accepted."""
        result = scan_database(
            fk_engine,
            database_name="test_db",
            sample_pct=50.0,
            max_sample_rows=5,
        )
        assert isinstance(result, ScanResult)
        # Should still have profiles
        for table in result.tables:
            assert len(table.profiles) > 0

    def test_result_storable(self, fk_engine: Engine, storage_backend) -> None:
        """ScanResult can be stored and loaded via StorageBackend."""
        result = scan_database(fk_engine, database_name="store_test")
        storage_backend.save_scan(result)
        loaded = storage_backend.load_scan("store_test")
        assert loaded is not None
        assert loaded.database_name == "store_test"
        assert len(loaded.tables) == len(result.tables)
        assert len(loaded.relationships) == len(result.relationships)

    def test_schema_extraction_failure(self) -> None:
        """ScanError raised when schema extraction fails."""
        engine = create_engine("sqlite:///:memory:")
        with patch(
            "dataconnect.scanner.extract_schema",
            side_effect=ScanError("inspection failed"),
        ):
            with pytest.raises(ScanError, match="inspection failed"):
                scan_database(engine, database_name="broken")

    def test_profiling_failure_graceful(self, fk_engine: Engine) -> None:
        """Profiling failures are handled gracefully (logged, not raised)."""
        with patch(
            "dataconnect.scanner.profile_tables",
            return_value=[],
        ):
            result = scan_database(fk_engine, database_name="test_db")
            # Tables come from extract_schema, but profile_tables returns empty
            assert result.tables == []


# ── Pipeline ordering tests ───────────────────────────────────────


class TestPipelineOrdering:
    """Verify that pipeline stages run in correct order."""

    def test_profiling_runs_after_schema(self, fk_engine: Engine) -> None:
        """Profiles reference columns from schema extraction."""
        result = scan_database(fk_engine, database_name="test_db")
        for table in result.tables:
            col_names = {c.name for c in table.columns}
            profile_names = {p.column_name for p in table.profiles}
            # Every profiled column must exist in schema
            assert profile_names.issubset(col_names)

    def test_relationships_include_discovered(self, fk_engine: Engine) -> None:
        """Relationship discovery runs after profiling."""
        result = scan_database(fk_engine, database_name="test_db")
        rel_types = {r.relationship_type for r in result.relationships}
        # Should have at least declared FK
        assert RelationshipType.DECLARED_FK in rel_types
