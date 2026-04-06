"""Tests for the REST API — auth, rate limiting, and all endpoints."""

from __future__ import annotations

import os
import time
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from dataconnect.api import create_app
from dataconnect.api.auth import reset_rate_limits
from dataconnect.api.routes import set_storage_dir
from dataconnect.exceptions import (
    GenerationError,
    RoutingError,
    ScanError,
    StorageError,
)
from dataconnect.models import (
    CheckResult,
    CheckStatus,
    ColumnInfo,
    MatchMethod,
    RelationshipInfo,
    RelationshipType,
    RouteResult,
    ScanResult,
    TableInfo,
    TableMatch,
    VerificationResult,
)


# ── Fixtures ───────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _clean_rate_limits():
    """Reset rate limit state before each test."""
    reset_rate_limits()
    yield
    reset_rate_limits()


@pytest.fixture()
def _server_key(monkeypatch: pytest.MonkeyPatch):
    """Set the server API key for auth."""
    monkeypatch.setenv("DATACONNECT_SERVER_API_KEY", "test-server-key")


@pytest.fixture()
def client(tmp_path: Path, _server_key) -> TestClient:
    """Create a test client with temporary storage."""
    app = create_app(storage_dir=tmp_path)
    return TestClient(app)


@pytest.fixture()
def auth_headers() -> dict[str, str]:
    """Standard auth headers for requests."""
    return {"X-API-Key": "test-server-key"}


@pytest.fixture()
def sample_scan() -> ScanResult:
    """Sample scan result for testing."""
    return ScanResult(
        database_name="testdb",
        scanned_at=datetime(2024, 1, 1, tzinfo=UTC),
        tables=[
            TableInfo(
                name="users",
                columns=[
                    ColumnInfo(name="id", data_type="INTEGER", is_primary_key=True),
                    ColumnInfo(name="name", data_type="VARCHAR"),
                    ColumnInfo(name="email", data_type="VARCHAR"),
                ],
                row_count_estimate=100,
            ),
            TableInfo(
                name="orders",
                columns=[
                    ColumnInfo(name="id", data_type="INTEGER", is_primary_key=True),
                    ColumnInfo(
                        name="user_id",
                        data_type="INTEGER",
                        is_foreign_key=True,
                        foreign_key_target="users.id",
                    ),
                    ColumnInfo(name="total", data_type="DECIMAL"),
                ],
                row_count_estimate=500,
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
        token_estimate=1200,
    )


@pytest.fixture()
def sample_route() -> RouteResult:
    """Sample route result."""
    return RouteResult(
        query="How many orders?",
        matched_tables=[
            TableMatch(
                table_name="orders",
                methods=[MatchMethod.EMBEDDING],
                relevance_score=0.9,
            ),
        ],
        total_candidates=2,
    )


@pytest.fixture()
def sample_verification() -> VerificationResult:
    """Sample verification result."""
    return VerificationResult(
        sql="SELECT COUNT(*) FROM orders",
        checks=[
            CheckResult(
                check_name="schema_conformity",
                status=CheckStatus.PASSED,
                message="All references valid.",
            ),
        ],
        confidence_score=92.0,
        is_verified=True,
        attempt_number=1,
    )


# ── Auth Tests ─────────────────────────────────────────────────────


class TestAuth:
    """X-API-Key authentication tests."""

    def test_missing_api_key(self, client: TestClient) -> None:
        """Request without X-API-Key header returns 401."""
        resp = client.get("/databases")
        assert resp.status_code == 401
        assert "Missing" in resp.json()["detail"]

    def test_invalid_api_key(self, client: TestClient) -> None:
        """Wrong API key returns 401."""
        resp = client.get(
            "/databases", headers={"X-API-Key": "wrong-key"}
        )
        assert resp.status_code == 401
        assert "Invalid" in resp.json()["detail"]

    def test_valid_api_key(
        self, client: TestClient, auth_headers: dict[str, str]
    ) -> None:
        """Correct API key passes auth."""
        resp = client.get("/databases", headers=auth_headers)
        assert resp.status_code == 200

    def test_no_server_key_configured(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """503 if DATACONNECT_SERVER_API_KEY not set."""
        monkeypatch.delenv("DATACONNECT_SERVER_API_KEY", raising=False)
        app = create_app(storage_dir=tmp_path)
        tc = TestClient(app)
        resp = tc.get(
            "/databases", headers={"X-API-Key": "anything"}
        )
        assert resp.status_code == 503
        assert "not configured" in resp.json()["detail"]


# ── Rate Limit Tests ───────────────────────────────────────────────


class TestRateLimit:
    """Per-key rate limiting tests."""

    def test_within_limit(
        self, client: TestClient, auth_headers: dict[str, str]
    ) -> None:
        """Requests within limit succeed."""
        for _ in range(5):
            resp = client.get("/databases", headers=auth_headers)
            assert resp.status_code == 200

    def test_exceeds_limit(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Requests beyond limit return 429."""
        monkeypatch.setenv("DATACONNECT_SERVER_API_KEY", "test-key")
        # Temporarily lower the limit
        monkeypatch.setattr(
            "dataconnect.api.auth.RATE_LIMIT_PER_MINUTE", 3
        )
        app = create_app(storage_dir=tmp_path)
        tc = TestClient(app)
        headers = {"X-API-Key": "test-key"}

        for _ in range(3):
            resp = tc.get("/databases", headers=headers)
            assert resp.status_code == 200

        resp = tc.get("/databases", headers=headers)
        assert resp.status_code == 429
        assert "Rate limit" in resp.json()["detail"]

    def test_different_keys_independent(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Rate limits are tracked per key (tested via single key reset)."""
        monkeypatch.setenv("DATACONNECT_SERVER_API_KEY", "test-key")
        app = create_app(storage_dir=tmp_path)
        tc = TestClient(app)
        headers = {"X-API-Key": "test-key"}

        # Make some requests
        for _ in range(3):
            resp = tc.get("/databases", headers=headers)
            assert resp.status_code == 200

        # Reset and verify we can make more
        reset_rate_limits()
        resp = tc.get("/databases", headers=headers)
        assert resp.status_code == 200


# ── GET /databases Tests ───────────────────────────────────────────


class TestListDatabases:
    """GET /databases endpoint tests."""

    def test_empty_list(
        self, client: TestClient, auth_headers: dict[str, str]
    ) -> None:
        """Returns empty list when no databases scanned."""
        resp = client.get("/databases", headers=auth_headers)
        assert resp.status_code == 200
        data = resp.json()
        assert data["databases"] == []
        assert data["count"] == 0

    def test_with_databases(
        self,
        client: TestClient,
        auth_headers: dict[str, str],
        tmp_path: Path,
        sample_scan: ScanResult,
    ) -> None:
        """Returns scanned databases."""
        from dataconnect.storage import StorageBackend

        storage = StorageBackend(tmp_path)
        storage.save_scan(sample_scan)

        # Need a client that uses the same tmp_path
        resp = client.get("/databases", headers=auth_headers)
        assert resp.status_code == 200
        data = resp.json()
        assert "testdb" in data["databases"]
        assert data["count"] >= 1

    def test_storage_error(
        self,
        client: TestClient,
        auth_headers: dict[str, str],
    ) -> None:
        """Storage errors return 500."""
        with patch(
            "dataconnect.storage.StorageBackend.list_databases",
            side_effect=StorageError("disk full"),
        ):
            resp = client.get("/databases", headers=auth_headers)
            assert resp.status_code == 500


# ── GET /databases/{name} Tests ────────────────────────────────────


class TestDatabaseInfo:
    """GET /databases/{name} endpoint tests."""

    def test_not_found(
        self, client: TestClient, auth_headers: dict[str, str]
    ) -> None:
        """Unknown database returns 404."""
        resp = client.get("/databases/nonexistent", headers=auth_headers)
        assert resp.status_code == 404
        assert "No scan found" in resp.json()["detail"]

    def test_found(
        self,
        client: TestClient,
        auth_headers: dict[str, str],
        tmp_path: Path,
        sample_scan: ScanResult,
    ) -> None:
        """Returns database details when found."""
        from dataconnect.storage import StorageBackend

        storage = StorageBackend(tmp_path)
        storage.save_scan(sample_scan)

        resp = client.get("/databases/testdb", headers=auth_headers)
        assert resp.status_code == 200
        data = resp.json()
        assert data["database_name"] == "testdb"
        assert data["tables"] == 2
        assert data["relationships"] == 1
        assert data["token_estimate"] == 1200
        assert len(data["table_details"]) == 2

    def test_table_details_structure(
        self,
        client: TestClient,
        auth_headers: dict[str, str],
        tmp_path: Path,
        sample_scan: ScanResult,
    ) -> None:
        """Table details include name, columns, and row count."""
        from dataconnect.storage import StorageBackend

        storage = StorageBackend(tmp_path)
        storage.save_scan(sample_scan)

        resp = client.get("/databases/testdb", headers=auth_headers)
        details = resp.json()["table_details"]
        users_table = next(t for t in details if t["name"] == "users")
        assert users_table["columns"] == 3
        assert users_table["row_count_estimate"] == 100


# ── POST /scan Tests ───────────────────────────────────────────────


class TestScanEndpoint:
    """POST /scan endpoint tests."""

    def test_successful_scan(
        self,
        client: TestClient,
        auth_headers: dict[str, str],
        sample_scan: ScanResult,
    ) -> None:
        """Successful scan returns summary."""
        with (
            patch(
                "dataconnect.database.create_readonly_engine"
            ) as mock_engine,
            patch(
                "dataconnect.scanner.scan_database",
                return_value=sample_scan,
            ),
            patch(
                "dataconnect.storage.StorageBackend.save_scan"
            ),
        ):
            mock_engine.return_value = MagicMock()

            resp = client.post(
                "/scan",
                json={
                    "connection_string": "sqlite:///test.db",
                },
                headers=auth_headers,
            )

        assert resp.status_code == 200
        data = resp.json()
        assert data["database_name"] == "testdb"
        assert data["tables"] == 2
        assert data["relationships"] == 1

    def test_connection_error(
        self,
        client: TestClient,
        auth_headers: dict[str, str],
    ) -> None:
        """Database connection failure returns 400."""
        from dataconnect.exceptions import DatabaseConnectionError

        with patch(
            "dataconnect.database.create_readonly_engine",
            side_effect=DatabaseConnectionError("cannot connect"),
        ):
            resp = client.post(
                "/scan",
                json={"connection_string": "bad://connection"},
                headers=auth_headers,
            )

        assert resp.status_code == 400

    def test_scan_error(
        self,
        client: TestClient,
        auth_headers: dict[str, str],
    ) -> None:
        """Scan failure returns 500."""
        with (
            patch(
                "dataconnect.database.create_readonly_engine"
            ) as mock_engine,
            patch(
                "dataconnect.scanner.scan_database",
                side_effect=ScanError("scan failed"),
            ),
        ):
            mock_engine.return_value = MagicMock()

            resp = client.post(
                "/scan",
                json={"connection_string": "sqlite:///test.db"},
                headers=auth_headers,
            )

        assert resp.status_code == 500

    def test_missing_connection_string(
        self,
        client: TestClient,
        auth_headers: dict[str, str],
    ) -> None:
        """Missing required field returns 422."""
        resp = client.post("/scan", json={}, headers=auth_headers)
        assert resp.status_code == 422

    def test_engine_disposed_on_error(
        self,
        client: TestClient,
        auth_headers: dict[str, str],
    ) -> None:
        """Engine is disposed even if scan fails."""
        mock_eng = MagicMock()
        with (
            patch(
                "dataconnect.database.create_readonly_engine",
                return_value=mock_eng,
            ),
            patch(
                "dataconnect.scanner.scan_database",
                side_effect=ScanError("boom"),
            ),
        ):
            client.post(
                "/scan",
                json={"connection_string": "sqlite:///test.db"},
                headers=auth_headers,
            )

        mock_eng.dispose.assert_called_once()


# ── POST /ask Tests ────────────────────────────────────────────────


class TestAskEndpoint:
    """POST /ask endpoint tests."""

    def test_successful_ask(
        self,
        client: TestClient,
        auth_headers: dict[str, str],
        tmp_path: Path,
        sample_scan: ScanResult,
        sample_route: RouteResult,
        sample_verification: VerificationResult,
    ) -> None:
        """Full pipeline returns expected response."""
        from dataconnect.storage import StorageBackend

        storage = StorageBackend(tmp_path)
        storage.save_scan(sample_scan)

        with (
            patch(
                "dataconnect.router.route_query",
                return_value=sample_route,
            ),
            patch(
                "dataconnect.generator.generate_sql",
                return_value="SELECT COUNT(*) FROM orders",
            ),
            patch(
                "dataconnect.verifier.retry.retry_with_fixes",
                return_value=sample_verification,
            ),
        ):
            resp = client.post(
                "/ask",
                json={
                    "question": "How many orders?",
                    "database_name": "testdb",
                    "model": "gpt-4o",
                    "llm_api_key": "sk-test",
                },
                headers=auth_headers,
            )

        assert resp.status_code == 200
        data = resp.json()
        assert data["question"] == "How many orders?"
        assert data["sql"] == "SELECT COUNT(*) FROM orders"
        assert data["confidence_score"] == 92.0
        assert data["confidence_label"] == "HIGH"
        assert data["is_verified"] is True
        assert data["selected_tables"] == ["orders"]

    def test_no_retry(
        self,
        client: TestClient,
        auth_headers: dict[str, str],
        tmp_path: Path,
        sample_scan: ScanResult,
        sample_route: RouteResult,
        sample_verification: VerificationResult,
    ) -> None:
        """retry=false skips the retry loop."""
        from dataconnect.storage import StorageBackend

        storage = StorageBackend(tmp_path)
        storage.save_scan(sample_scan)

        with (
            patch(
                "dataconnect.router.route_query",
                return_value=sample_route,
            ),
            patch(
                "dataconnect.generator.generate_sql",
                return_value="SELECT COUNT(*) FROM orders",
            ),
            patch(
                "dataconnect.verifier.verify_sql",
                return_value=sample_verification,
            ) as mock_verify,
        ):
            resp = client.post(
                "/ask",
                json={
                    "question": "How many orders?",
                    "database_name": "testdb",
                    "model": "gpt-4o",
                    "llm_api_key": "sk-test",
                    "retry": False,
                },
                headers=auth_headers,
            )

        assert resp.status_code == 200
        mock_verify.assert_called_once()

    def test_database_not_found(
        self,
        client: TestClient,
        auth_headers: dict[str, str],
    ) -> None:
        """Unknown database returns 404."""
        resp = client.post(
            "/ask",
            json={
                "question": "How many orders?",
                "database_name": "nonexistent",
                "model": "gpt-4o",
                "llm_api_key": "sk-test",
            },
            headers=auth_headers,
        )
        assert resp.status_code == 404

    def test_routing_error(
        self,
        client: TestClient,
        auth_headers: dict[str, str],
        tmp_path: Path,
        sample_scan: ScanResult,
    ) -> None:
        """Routing failure returns 500."""
        from dataconnect.storage import StorageBackend

        storage = StorageBackend(tmp_path)
        storage.save_scan(sample_scan)

        with patch(
            "dataconnect.router.route_query",
            side_effect=RoutingError("routing failed"),
        ):
            resp = client.post(
                "/ask",
                json={
                    "question": "How many orders?",
                    "database_name": "testdb",
                    "model": "gpt-4o",
                    "llm_api_key": "sk-test",
                },
                headers=auth_headers,
            )

        assert resp.status_code == 500

    def test_generation_error(
        self,
        client: TestClient,
        auth_headers: dict[str, str],
        tmp_path: Path,
        sample_scan: ScanResult,
        sample_route: RouteResult,
    ) -> None:
        """Generation failure returns 500."""
        from dataconnect.storage import StorageBackend

        storage = StorageBackend(tmp_path)
        storage.save_scan(sample_scan)

        with (
            patch(
                "dataconnect.router.route_query",
                return_value=sample_route,
            ),
            patch(
                "dataconnect.generator.generate_sql",
                side_effect=GenerationError("gen failed"),
            ),
        ):
            resp = client.post(
                "/ask",
                json={
                    "question": "How many orders?",
                    "database_name": "testdb",
                    "model": "gpt-4o",
                    "llm_api_key": "sk-test",
                },
                headers=auth_headers,
            )

        assert resp.status_code == 500

    def test_missing_required_fields(
        self,
        client: TestClient,
        auth_headers: dict[str, str],
    ) -> None:
        """Missing required fields return 422."""
        resp = client.post(
            "/ask",
            json={"question": "How many orders?"},
            headers=auth_headers,
        )
        assert resp.status_code == 422

    def test_checks_in_response(
        self,
        client: TestClient,
        auth_headers: dict[str, str],
        tmp_path: Path,
        sample_scan: ScanResult,
        sample_route: RouteResult,
        sample_verification: VerificationResult,
    ) -> None:
        """Verification checks appear in response."""
        from dataconnect.storage import StorageBackend

        storage = StorageBackend(tmp_path)
        storage.save_scan(sample_scan)

        with (
            patch(
                "dataconnect.router.route_query",
                return_value=sample_route,
            ),
            patch(
                "dataconnect.generator.generate_sql",
                return_value="SELECT COUNT(*) FROM orders",
            ),
            patch(
                "dataconnect.verifier.retry.retry_with_fixes",
                return_value=sample_verification,
            ),
        ):
            resp = client.post(
                "/ask",
                json={
                    "question": "How many orders?",
                    "database_name": "testdb",
                    "model": "gpt-4o",
                    "llm_api_key": "sk-test",
                },
                headers=auth_headers,
            )

        checks = resp.json()["checks"]
        assert len(checks) == 1
        assert checks[0]["check_name"] == "schema_conformity"
        assert checks[0]["status"] == "passed"


# ── App Factory Tests ──────────────────────────────────────────────


class TestAppFactory:
    """create_app() factory tests."""

    def test_creates_app(self, tmp_path: Path, _server_key) -> None:
        """Factory returns a FastAPI instance."""
        app = create_app(storage_dir=tmp_path)
        assert app.title == "DataConnect API"

    def test_default_storage_dir(self, _server_key) -> None:
        """Default storage uses ~/.dataconnect."""
        app = create_app()
        # Just verify it doesn't crash
        assert app is not None

    def test_routes_registered(
        self, tmp_path: Path, _server_key
    ) -> None:
        """All expected routes are registered."""
        app = create_app(storage_dir=tmp_path)
        paths = [route.path for route in app.routes]
        assert "/scan" in paths
        assert "/ask" in paths
        assert "/databases" in paths
        assert "/databases/{name}" in paths
        assert "/health" in paths

    def test_openapi_schema(
        self, client: TestClient, auth_headers: dict[str, str]
    ) -> None:
        """OpenAPI schema is available."""
        resp = client.get("/openapi.json")
        assert resp.status_code == 200
        schema = resp.json()
        assert schema["info"]["title"] == "DataConnect API"


# ── Confidence Label Tests ─────────────────────────────────────────


class TestConfidenceLabel:
    """_confidence_label helper tests."""

    def test_high(self) -> None:
        """Score >= 90 is HIGH."""
        from dataconnect.api.routes import _confidence_label

        assert _confidence_label(95.0) == "HIGH"
        assert _confidence_label(90.0) == "HIGH"

    def test_medium(self) -> None:
        """Score >= 70 is MEDIUM."""
        from dataconnect.api.routes import _confidence_label

        assert _confidence_label(80.0) == "MEDIUM"
        assert _confidence_label(70.0) == "MEDIUM"

    def test_low(self) -> None:
        """Score >= 50 is LOW."""
        from dataconnect.api.routes import _confidence_label

        assert _confidence_label(60.0) == "LOW"
        assert _confidence_label(50.0) == "LOW"

    def test_unverified(self) -> None:
        """Score < 50 is UNVERIFIED."""
        from dataconnect.api.routes import _confidence_label

        assert _confidence_label(30.0) == "UNVERIFIED"
        assert _confidence_label(0.0) == "UNVERIFIED"


# ── GET /health Tests ─────────────────────────────────────────────


class TestHealthEndpoint:
    """GET /health endpoint tests — no auth required."""

    def test_health_no_auth(
        self, tmp_path: Path, _server_key
    ) -> None:
        """Health check works without API key."""
        app = create_app(storage_dir=tmp_path)
        tc = TestClient(app)
        resp = tc.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["version"] == "0.1.0"
        assert data["databases"] == 0

    def test_health_with_databases(
        self, tmp_path: Path, _server_key, sample_scan: ScanResult
    ) -> None:
        """Health check returns database count."""
        from dataconnect.storage import StorageBackend

        storage = StorageBackend(tmp_path)
        storage.save_scan(sample_scan)

        app = create_app(storage_dir=tmp_path)
        tc = TestClient(app)
        resp = tc.get("/health")
        assert resp.status_code == 200
        assert resp.json()["databases"] >= 1

    def test_health_no_server_key(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Health check works even without server API key."""
        monkeypatch.delenv("DATACONNECT_SERVER_API_KEY", raising=False)
        app = create_app(storage_dir=tmp_path)
        tc = TestClient(app)
        resp = tc.get("/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

    def test_health_response_schema(
        self, tmp_path: Path, _server_key
    ) -> None:
        """Health response has expected fields."""
        app = create_app(storage_dir=tmp_path)
        tc = TestClient(app)
        resp = tc.get("/health")
        data = resp.json()
        assert set(data.keys()) == {"status", "version", "databases"}


# ── Profile in /ask Tests ─────────────────────────────────────────


class TestAskWithProfile:
    """POST /ask with tuning profile parameter."""

    def test_ask_with_strict_profile(
        self,
        client: TestClient,
        auth_headers: dict[str, str],
        tmp_path: Path,
        sample_scan: ScanResult,
        sample_route: RouteResult,
        sample_verification: VerificationResult,
    ) -> None:
        """Profile is passed through to verify/retry."""
        from dataconnect.storage import StorageBackend

        storage = StorageBackend(tmp_path)
        storage.save_scan(sample_scan)

        with (
            patch(
                "dataconnect.router.route_query",
                return_value=sample_route,
            ),
            patch(
                "dataconnect.generator.generate_sql",
                return_value="SELECT COUNT(*) FROM orders",
            ),
            patch(
                "dataconnect.verifier.retry.retry_with_fixes",
                return_value=sample_verification,
            ) as mock_retry,
        ):
            resp = client.post(
                "/ask",
                json={
                    "question": "How many orders?",
                    "database_name": "testdb",
                    "model": "gpt-4o",
                    "llm_api_key": "sk-test",
                    "profile": "strict",
                },
                headers=auth_headers,
            )

        assert resp.status_code == 200
        # Verify profile was passed to retry_with_fixes
        call_kwargs = mock_retry.call_args
        assert call_kwargs.kwargs["profile"].name == "strict"

    def test_ask_with_invalid_profile(
        self,
        client: TestClient,
        auth_headers: dict[str, str],
        tmp_path: Path,
        sample_scan: ScanResult,
    ) -> None:
        """Invalid profile returns 400."""
        from dataconnect.storage import StorageBackend

        storage = StorageBackend(tmp_path)
        storage.save_scan(sample_scan)

        resp = client.post(
            "/ask",
            json={
                "question": "How many orders?",
                "database_name": "testdb",
                "model": "gpt-4o",
                "llm_api_key": "sk-test",
                "profile": "nonexistent_profile",
            },
            headers=auth_headers,
        )
        assert resp.status_code == 400

    def test_ask_default_profile_when_null(
        self,
        client: TestClient,
        auth_headers: dict[str, str],
        tmp_path: Path,
        sample_scan: ScanResult,
        sample_route: RouteResult,
        sample_verification: VerificationResult,
    ) -> None:
        """Null profile uses default."""
        from dataconnect.storage import StorageBackend

        storage = StorageBackend(tmp_path)
        storage.save_scan(sample_scan)

        with (
            patch(
                "dataconnect.router.route_query",
                return_value=sample_route,
            ),
            patch(
                "dataconnect.generator.generate_sql",
                return_value="SELECT COUNT(*) FROM orders",
            ),
            patch(
                "dataconnect.verifier.retry.retry_with_fixes",
                return_value=sample_verification,
            ) as mock_retry,
        ):
            resp = client.post(
                "/ask",
                json={
                    "question": "How many orders?",
                    "database_name": "testdb",
                    "model": "gpt-4o",
                    "llm_api_key": "sk-test",
                },
                headers=auth_headers,
            )

        assert resp.status_code == 200
        call_kwargs = mock_retry.call_args
        assert call_kwargs.kwargs["profile"].name == "default"

    def test_ask_profile_no_retry(
        self,
        client: TestClient,
        auth_headers: dict[str, str],
        tmp_path: Path,
        sample_scan: ScanResult,
        sample_route: RouteResult,
        sample_verification: VerificationResult,
    ) -> None:
        """Profile is passed to verify_sql when retry=false."""
        from dataconnect.storage import StorageBackend

        storage = StorageBackend(tmp_path)
        storage.save_scan(sample_scan)

        with (
            patch(
                "dataconnect.router.route_query",
                return_value=sample_route,
            ),
            patch(
                "dataconnect.generator.generate_sql",
                return_value="SELECT COUNT(*) FROM orders",
            ),
            patch(
                "dataconnect.verifier.verify_sql",
                return_value=sample_verification,
            ) as mock_verify,
        ):
            resp = client.post(
                "/ask",
                json={
                    "question": "How many orders?",
                    "database_name": "testdb",
                    "model": "gpt-4o",
                    "llm_api_key": "sk-test",
                    "retry": False,
                    "profile": "lenient",
                },
                headers=auth_headers,
            )

        assert resp.status_code == 200
        call_kwargs = mock_verify.call_args
        assert call_kwargs.kwargs["profile"].name == "lenient"
