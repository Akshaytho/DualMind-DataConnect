"""Shared test fixtures for DataConnect."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest
from sqlalchemy import Column, Integer, String, create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from dataconnect.models import (
    CheckResult,
    CheckStatus,
    ColumnInfo,
    ColumnProfile,
    RelationshipInfo,
    RelationshipType,
    ScanResult,
    TableInfo,
)
from dataconnect.storage import StorageBackend


class SampleBase(DeclarativeBase):
    """Base for sample test database."""


class SampleUsers(SampleBase):
    """Sample users table for testing."""

    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    email = Column(String(255), nullable=True)


class SampleOrders(SampleBase):
    """Sample orders table for testing."""

    __tablename__ = "orders"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, nullable=False)
    amount = Column(Integer, nullable=False)


@pytest.fixture
def sample_engine() -> Engine:
    """In-memory SQLite engine with sample tables and data."""
    engine = create_engine("sqlite:///:memory:")
    SampleBase.metadata.create_all(engine)

    with Session(engine) as session:
        session.add_all([
            SampleUsers(id=1, name="Alice", email="alice@example.com"),
            SampleUsers(id=2, name="Bob", email=None),
        ])
        session.add_all([
            SampleOrders(id=1, user_id=1, amount=100),
            SampleOrders(id=2, user_id=1, amount=200),
            SampleOrders(id=3, user_id=2, amount=50),
        ])
        session.commit()

    return engine


@pytest.fixture
def sample_scan_result() -> ScanResult:
    """A complete ScanResult for testing."""
    return ScanResult(
        database_name="test_db",
        tables=[
            TableInfo(
                name="users",
                columns=[
                    ColumnInfo(name="id", data_type="INTEGER", is_primary_key=True, nullable=False),
                    ColumnInfo(name="name", data_type="VARCHAR(100)", nullable=False),
                    ColumnInfo(name="email", data_type="VARCHAR(255)", nullable=True),
                ],
                row_count_estimate=2,
                profiles=[
                    ColumnProfile(column_name="id", distinct_count=2, null_fraction=0.0),
                    ColumnProfile(column_name="name", distinct_count=2, null_fraction=0.0),
                    ColumnProfile(column_name="email", distinct_count=1, null_fraction=0.5),
                ],
                description="User accounts",
            ),
            TableInfo(
                name="orders",
                columns=[
                    ColumnInfo(name="id", data_type="INTEGER", is_primary_key=True, nullable=False),
                    ColumnInfo(name="user_id", data_type="INTEGER", is_foreign_key=True,
                               foreign_key_target="users.id", nullable=False),
                    ColumnInfo(name="amount", data_type="INTEGER", nullable=False),
                ],
                row_count_estimate=3,
                description="Purchase orders",
            ),
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
        ],
        token_estimate=500,
    )


@pytest.fixture
def storage_dir(tmp_path: Path) -> Path:
    """Temporary directory for storage tests."""
    return tmp_path / "storage"


@pytest.fixture
def storage_backend(storage_dir: Path) -> StorageBackend:
    """Fresh StorageBackend instance."""
    return StorageBackend(storage_dir)
