"""Tests for database module — read-only enforcement, connection handling."""

from __future__ import annotations

import pytest
from sqlalchemy import text

from dataconnect.config import sanitize_connection_string
from dataconnect.database import create_readonly_engine
from dataconnect.exceptions import DatabaseConnectionError, ReadOnlyViolationError


class TestSanitizeConnectionString:
    """Tests for connection string sanitization."""

    def test_masks_password(self) -> None:
        result = sanitize_connection_string("postgresql://user:secret123@host/db")
        assert "secret123" not in result
        assert "***" in result
        assert "user:" in result

    def test_no_password(self) -> None:
        result = sanitize_connection_string("sqlite:///local.db")
        assert result == "sqlite:///local.db"

    def test_complex_password(self) -> None:
        result = sanitize_connection_string("postgresql://admin:p@ss@host:5432/db")
        assert "p@ss" not in result


class TestReadOnlyEngine:
    """Tests for read-only engine creation and write blocking."""

    def test_select_allowed(self, sample_engine: None) -> None:
        engine = create_readonly_engine("sqlite:///:memory:")
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            assert result.scalar() == 1

    def test_insert_blocked(self) -> None:
        engine = create_readonly_engine("sqlite:///:memory:")
        with engine.connect() as conn:
            with pytest.raises(ReadOnlyViolationError, match="INSERT"):
                conn.execute(text("INSERT INTO fake VALUES (1)"))

    def test_drop_blocked(self) -> None:
        engine = create_readonly_engine("sqlite:///:memory:")
        with engine.connect() as conn:
            with pytest.raises(ReadOnlyViolationError, match="DROP"):
                conn.execute(text("DROP TABLE fake"))

    def test_update_blocked(self) -> None:
        engine = create_readonly_engine("sqlite:///:memory:")
        with engine.connect() as conn:
            with pytest.raises(ReadOnlyViolationError, match="UPDATE"):
                conn.execute(text("UPDATE fake SET x=1"))

    def test_delete_blocked(self) -> None:
        engine = create_readonly_engine("sqlite:///:memory:")
        with engine.connect() as conn:
            with pytest.raises(ReadOnlyViolationError, match="DELETE"):
                conn.execute(text("DELETE FROM fake"))

    def test_bad_connection_string(self) -> None:
        with pytest.raises(DatabaseConnectionError):
            create_readonly_engine("postgresql://bad:bad@nonexistent:9999/nodb")
