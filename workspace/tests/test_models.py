"""Tests for Pydantic models — validation, serialization, constraints."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from dataconnect.models import (
    CheckResult,
    CheckStatus,
    ColumnInfo,
    ColumnProfile,
    MatchMethod,
    QueryResult,
    RelationshipInfo,
    RelationshipType,
    RouteResult,
    ScanResult,
    TableInfo,
    TableMatch,
    VerificationResult,
)


class TestColumnInfo:
    """Tests for ColumnInfo model."""

    def test_minimal_column(self) -> None:
        col = ColumnInfo(name="id", data_type="INTEGER")
        assert col.name == "id"
        assert col.nullable is True
        assert col.is_primary_key is False
        assert col.description == ""

    def test_full_column(self) -> None:
        col = ColumnInfo(
            name="user_id",
            data_type="INTEGER",
            nullable=False,
            is_foreign_key=True,
            foreign_key_target="users.id",
        )
        assert col.foreign_key_target == "users.id"


class TestColumnProfile:
    """Tests for ColumnProfile model."""

    def test_null_fraction_bounds(self) -> None:
        with pytest.raises(ValidationError):
            ColumnProfile(column_name="x", null_fraction=1.5)

        with pytest.raises(ValidationError):
            ColumnProfile(column_name="x", null_fraction=-0.1)

    def test_valid_profile(self) -> None:
        p = ColumnProfile(column_name="age", null_fraction=0.1, distinct_count=50)
        assert p.distinct_count == 50


class TestTableInfo:
    """Tests for TableInfo model."""

    def test_defaults(self) -> None:
        t = TableInfo(name="users")
        assert t.schema_name == "public"
        assert t.columns == []
        assert t.row_count_estimate == 0

    def test_with_columns(self) -> None:
        t = TableInfo(
            name="users",
            columns=[ColumnInfo(name="id", data_type="INT")],
            row_count_estimate=100,
        )
        assert len(t.columns) == 1


class TestRelationshipInfo:
    """Tests for RelationshipInfo model."""

    def test_declared_fk(self) -> None:
        r = RelationshipInfo(
            source_table="orders",
            source_column="user_id",
            target_table="users",
            target_column="id",
            relationship_type=RelationshipType.DECLARED_FK,
            confidence=1.0,
        )
        assert r.confidence == 1.0

    def test_confidence_bounds(self) -> None:
        with pytest.raises(ValidationError):
            RelationshipInfo(
                source_table="a", source_column="b",
                target_table="c", target_column="d",
                relationship_type=RelationshipType.NAME_MATCH,
                confidence=1.5,
            )


class TestScanResult:
    """Tests for ScanResult model."""

    def test_roundtrip_json(self) -> None:
        scan = ScanResult(
            database_name="test",
            tables=[TableInfo(name="t1")],
            relationships=[],
            token_estimate=100,
        )
        json_str = scan.model_dump_json()
        loaded = ScanResult.model_validate_json(json_str)
        assert loaded.database_name == "test"
        assert len(loaded.tables) == 1


class TestRouterModels:
    """Tests for Router layer models."""

    def test_table_match(self) -> None:
        m = TableMatch(
            table_name="users",
            methods=[MatchMethod.EMBEDDING, MatchMethod.GRAPH_WALK],
            relevance_score=0.85,
        )
        assert len(m.methods) == 2

    def test_route_result(self) -> None:
        r = RouteResult(query="who are the users?", total_candidates=10)
        assert r.matched_tables == []


class TestVerifierModels:
    """Tests for Verifier layer models."""

    def test_check_result(self) -> None:
        cr = CheckResult(
            check_name="schema_conformity",
            status=CheckStatus.PASSED,
            message="All columns exist",
        )
        assert cr.status == CheckStatus.PASSED

    def test_verification_result_defaults(self) -> None:
        vr = VerificationResult(sql="SELECT 1")
        assert vr.confidence_score == 0.0
        assert vr.is_verified is False
        assert vr.attempt_number == 1


class TestQueryResult:
    """Tests for final QueryResult model."""

    def test_full_result(self) -> None:
        qr = QueryResult(
            query="how many users?",
            sql="SELECT COUNT(*) FROM users",
            verification=VerificationResult(
                sql="SELECT COUNT(*) FROM users",
                confidence_score=95.0,
                is_verified=True,
            ),
            route=RouteResult(query="how many users?", total_candidates=5),
        )
        assert qr.verification.is_verified is True
