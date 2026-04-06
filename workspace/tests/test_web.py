"""Tests for web UI — GET /ui endpoint."""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from dataconnect.api import create_app
from dataconnect.config import PROJECT_NAME
from dataconnect.web import _build_html, router


# ── HTML template tests ──────────────────────────────────────────


class TestBuildHtml:
    """Tests for _build_html() template function."""

    def test_returns_string(self) -> None:
        """Returns a non-empty string."""
        html = _build_html()
        assert isinstance(html, str)
        assert len(html) > 0

    def test_contains_doctype(self) -> None:
        """Has proper HTML5 doctype."""
        html = _build_html()
        assert html.startswith("<!DOCTYPE html>")

    def test_contains_project_name(self) -> None:
        """Uses PROJECT_NAME from config."""
        html = _build_html()
        assert PROJECT_NAME in html

    def test_contains_title(self) -> None:
        """Has title tag with project name."""
        html = _build_html()
        assert f"<title>{PROJECT_NAME}</title>" in html

    def test_contains_api_key_input(self) -> None:
        """Has input for server API key."""
        html = _build_html()
        assert 'id="apiKey"' in html
        assert 'type="password"' in html

    def test_contains_database_select(self) -> None:
        """Has database selector dropdown."""
        html = _build_html()
        assert 'id="dbSelect"' in html
        assert "<select" in html

    def test_contains_question_input(self) -> None:
        """Has question input field."""
        html = _build_html()
        assert 'id="question"' in html

    def test_contains_ask_button(self) -> None:
        """Has the ask button."""
        html = _build_html()
        assert 'id="askBtn"' in html
        assert "askQuestion()" in html

    def test_contains_llm_model_input(self) -> None:
        """Has LLM model input field."""
        html = _build_html()
        assert 'id="llmModel"' in html

    def test_contains_llm_key_input(self) -> None:
        """Has LLM API key input field."""
        html = _build_html()
        assert 'id="llmKey"' in html

    def test_contains_results_container(self) -> None:
        """Has results display container."""
        html = _build_html()
        assert 'id="results"' in html

    def test_contains_status_area(self) -> None:
        """Has status display area."""
        html = _build_html()
        assert 'id="status"' in html

    def test_contains_fetch_calls(self) -> None:
        """JS makes fetch calls to API endpoints."""
        html = _build_html()
        assert '"/databases"' in html
        assert '"/ask"' in html

    def test_contains_xss_escape(self) -> None:
        """Has XSS escape function for user content."""
        html = _build_html()
        assert "function esc(" in html
        assert "textContent" in html

    def test_contains_enter_key_handler(self) -> None:
        """Enter key triggers ask."""
        html = _build_html()
        assert '"Enter"' in html

    def test_contains_confidence_classes(self) -> None:
        """Has CSS classes for confidence levels."""
        html = _build_html()
        assert "conf-HIGH" in html
        assert "conf-MEDIUM" in html
        assert "conf-LOW" in html
        assert "conf-UNVERIFIED" in html

    def test_contains_check_status_classes(self) -> None:
        """Has CSS classes for check statuses."""
        html = _build_html()
        assert "st-passed" in html
        assert "st-warning" in html
        assert "st-failed" in html


# ── Endpoint tests ───────────────────────────────────────────────


@pytest.fixture()
def web_client(tmp_path: object) -> TestClient:
    """Create a test client with web UI enabled."""
    app = create_app(storage_dir=tmp_path)  # type: ignore[arg-type]
    return TestClient(app)


class TestWebUiEndpoint:
    """Tests for GET /ui endpoint."""

    def test_returns_200(self, web_client: TestClient) -> None:
        """GET /ui returns 200."""
        response = web_client.get("/ui")
        assert response.status_code == 200

    def test_returns_html(self, web_client: TestClient) -> None:
        """Response content type is HTML."""
        response = web_client.get("/ui")
        assert "text/html" in response.headers["content-type"]

    def test_no_auth_required(self, web_client: TestClient) -> None:
        """No X-API-Key needed for the UI page."""
        response = web_client.get("/ui")
        assert response.status_code == 200

    def test_contains_project_name(self, web_client: TestClient) -> None:
        """Response HTML contains project name."""
        response = web_client.get("/ui")
        assert PROJECT_NAME in response.text

    def test_not_in_openapi_schema(self, web_client: TestClient) -> None:
        """Web UI endpoint is excluded from OpenAPI schema."""
        response = web_client.get("/openapi.json")
        schema = response.json()
        assert "/ui" not in schema.get("paths", {})


# ── Router tests ─────────────────────────────────────────────────


class TestWebRouter:
    """Tests for the web router configuration."""

    def test_router_has_routes(self) -> None:
        """Router has at least one route."""
        assert len(router.routes) > 0

    def test_router_tag(self) -> None:
        """Router is tagged 'web'."""
        assert "web" in router.tags

    def test_ui_route_exists(self) -> None:
        """Router includes /ui path."""
        paths = [r.path for r in router.routes]  # type: ignore[union-attr]
        assert "/ui" in paths
