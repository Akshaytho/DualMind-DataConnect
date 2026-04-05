"""Tests for scanner schema extraction."""

from __future__ import annotations

from sqlalchemy import (
    Column,
    ForeignKey,
    Integer,
    String,
    Float,
    create_engine,
)
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, Session

from dataconnect.models import RelationshipType
from dataconnect.scanner.schema import extract_schema


class Base(DeclarativeBase):
    """Test database base."""


class Department(Base):
    """Department table for FK testing."""

    __tablename__ = "departments"
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)


class Employee(Base):
    """Employee table with FK to departments."""

    __tablename__ = "employees"
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    salary = Column(Float, nullable=True)
    dept_id = Column(Integer, ForeignKey("departments.id"), nullable=False)


def _make_engine() -> Engine:
    """Create in-memory SQLite with departments + employees."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return engine


class TestExtractSchema:
    """Tests for extract_schema()."""

    def test_finds_all_tables(self) -> None:
        """Should discover all tables in the database."""
        engine = _make_engine()
        tables, _ = extract_schema(engine)
        names = {t.name for t in tables}
        assert names == {"departments", "employees"}

    def test_column_count(self) -> None:
        """Should extract correct number of columns per table."""
        engine = _make_engine()
        tables, _ = extract_schema(engine)
        by_name = {t.name: t for t in tables}
        assert len(by_name["departments"].columns) == 2
        assert len(by_name["employees"].columns) == 4

    def test_primary_key_detection(self) -> None:
        """Should mark primary key columns correctly."""
        engine = _make_engine()
        tables, _ = extract_schema(engine)
        by_name = {t.name: t for t in tables}
        dept_cols = {c.name: c for c in by_name["departments"].columns}
        assert dept_cols["id"].is_primary_key is True
        assert dept_cols["name"].is_primary_key is False

    def test_nullable_detection(self) -> None:
        """Should detect nullable columns."""
        engine = _make_engine()
        tables, _ = extract_schema(engine)
        by_name = {t.name: t for t in tables}
        emp_cols = {c.name: c for c in by_name["employees"].columns}
        assert emp_cols["salary"].nullable is True
        assert emp_cols["name"].nullable is False

    def test_foreign_key_detection(self) -> None:
        """Should detect FK columns and set target."""
        engine = _make_engine()
        tables, _ = extract_schema(engine)
        by_name = {t.name: t for t in tables}
        emp_cols = {c.name: c for c in by_name["employees"].columns}
        assert emp_cols["dept_id"].is_foreign_key is True
        assert emp_cols["dept_id"].foreign_key_target == "departments.id"

    def test_relationship_extraction(self) -> None:
        """Should return declared FK relationships."""
        engine = _make_engine()
        _, rels = extract_schema(engine)
        assert len(rels) == 1
        rel = rels[0]
        assert rel.source_table == "employees"
        assert rel.source_column == "dept_id"
        assert rel.target_table == "departments"
        assert rel.target_column == "id"
        assert rel.relationship_type == RelationshipType.DECLARED_FK
        assert rel.confidence == 1.0

    def test_data_type_extraction(self) -> None:
        """Should capture column data types as strings."""
        engine = _make_engine()
        tables, _ = extract_schema(engine)
        by_name = {t.name: t for t in tables}
        emp_cols = {c.name: c for c in by_name["employees"].columns}
        # SQLAlchemy returns type objects; we store as str
        assert "INT" in emp_cols["id"].data_type.upper()
        assert emp_cols["salary"].data_type.upper() != "UNKNOWN"

    def test_empty_database(self) -> None:
        """Should handle database with no tables gracefully."""
        engine = create_engine("sqlite:///:memory:")
        tables, rels = extract_schema(engine)
        assert tables == []
        assert rels == []

    def test_schema_name_default(self) -> None:
        """Should set schema_name to 'public' when not specified."""
        engine = _make_engine()
        tables, _ = extract_schema(engine)
        for table in tables:
            assert table.schema_name == "public"

    def test_uses_sample_engine_fixture(self, sample_engine: Engine) -> None:
        """Should work with conftest sample_engine (users + orders)."""
        tables, _ = extract_schema(sample_engine)
        names = {t.name for t in tables}
        assert "users" in names
        assert "orders" in names
