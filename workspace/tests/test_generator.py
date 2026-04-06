"""Tests for SQL generation module."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from dataconnect.exceptions import GenerationError, LLMError
from dataconnect.generator import (
    _build_generation_prompt,
    _build_table_context,
    _extract_sql,
    generate_sql,
)
from dataconnect.models import (
    ColumnInfo,
    MatchMethod,
    RelationshipInfo,
    RelationshipType,
    RouteResult,
    ScanResult,
    TableMatch,
    TableInfo,
)


# ── Fixtures ─────────────────────────────────────────────────────────


@pytest.fixture
def scan_result() -> ScanResult:
    """Sample scan result with two tables."""
    return ScanResult(
        database_name="testdb",
        tables=[
            TableInfo(
                name="users",
                description="User accounts",
                columns=[
                    ColumnInfo(
                        name="id", data_type="INTEGER",
                        is_primary_key=True, nullable=False,
                    ),
                    ColumnInfo(name="name", data_type="VARCHAR(100)"),
                    ColumnInfo(name="email", data_type="VARCHAR(255)"),
                ],
            ),
            TableInfo(
                name="orders",
                description="Customer orders",
                columns=[
                    ColumnInfo(
                        name="id", data_type="INTEGER",
                        is_primary_key=True, nullable=False,
                    ),
                    ColumnInfo(
                        name="user_id", data_type="INTEGER",
                        is_foreign_key=True,
                        foreign_key_target="users.id",
                    ),
                    ColumnInfo(name="amount", data_type="NUMERIC(10,2)"),
                    ColumnInfo(name="created_at", data_type="TIMESTAMP"),
                ],
            ),
            TableInfo(
                name="products",
                description="Product catalog",
                columns=[
                    ColumnInfo(name="id", data_type="INTEGER"),
                    ColumnInfo(name="name", data_type="VARCHAR(200)"),
                ],
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
    )


@pytest.fixture
def route_result() -> RouteResult:
    """Route result selecting users and orders."""
    return RouteResult(
        query="How many orders per user?",
        matched_tables=[
            TableMatch(
                table_name="users",
                methods=[MatchMethod.EMBEDDING],
                relevance_score=0.9,
            ),
            TableMatch(
                table_name="orders",
                methods=[MatchMethod.GRAPH_WALK],
                relevance_score=0.8,
            ),
        ],
        total_candidates=3,
    )


# ── _build_table_context ─────────────────────────────────────────────


class TestBuildTableContext:
    """Tests for table context builder."""

    def test_includes_selected_tables_only(
        self, scan_result: ScanResult, route_result: RouteResult,
    ) -> None:
        """Only router-selected tables appear in context."""
        ctx = _build_table_context(scan_result, route_result)
        assert "users" in ctx
        assert "orders" in ctx
        assert "products" not in ctx

    def test_includes_column_info(
        self, scan_result: ScanResult, route_result: RouteResult,
    ) -> None:
        """Column names and types appear."""
        ctx = _build_table_context(scan_result, route_result)
        assert "id INTEGER" in ctx
        assert "email VARCHAR(255)" in ctx
        assert "amount NUMERIC(10,2)" in ctx

    def test_includes_primary_key(
        self, scan_result: ScanResult, route_result: RouteResult,
    ) -> None:
        """PRIMARY KEY annotation present."""
        ctx = _build_table_context(scan_result, route_result)
        assert "PRIMARY KEY" in ctx

    def test_includes_foreign_key(
        self, scan_result: ScanResult, route_result: RouteResult,
    ) -> None:
        """FK annotation present."""
        ctx = _build_table_context(scan_result, route_result)
        assert "FK -> users.id" in ctx

    def test_includes_relationships(
        self, scan_result: ScanResult, route_result: RouteResult,
    ) -> None:
        """Relationship section present."""
        ctx = _build_table_context(scan_result, route_result)
        assert "Relationships:" in ctx
        assert "orders.user_id -> users.id" in ctx

    def test_includes_table_description(
        self, scan_result: ScanResult, route_result: RouteResult,
    ) -> None:
        """Table description in context."""
        ctx = _build_table_context(scan_result, route_result)
        assert "User accounts" in ctx

    def test_not_null_annotation(
        self, scan_result: ScanResult, route_result: RouteResult,
    ) -> None:
        """NOT NULL annotation for non-nullable columns."""
        ctx = _build_table_context(scan_result, route_result)
        assert "NOT NULL" in ctx

    def test_empty_route_result(self, scan_result: ScanResult) -> None:
        """Empty route result produces empty context."""
        empty_route = RouteResult(query="test", matched_tables=[], total_candidates=0)
        ctx = _build_table_context(scan_result, empty_route)
        assert ctx == ""

    def test_no_relationships_no_section(self) -> None:
        """No relationships means no Relationships section."""
        sr = ScanResult(
            database_name="test",
            tables=[TableInfo(name="t1", columns=[ColumnInfo(name="id", data_type="INT")])],
            relationships=[],
        )
        rr = RouteResult(
            query="test",
            matched_tables=[TableMatch(table_name="t1", methods=[MatchMethod.EMBEDDING])],
            total_candidates=1,
        )
        ctx = _build_table_context(sr, rr)
        assert "Relationships:" not in ctx


# ── _build_generation_prompt ─────────────────────────────────────────


class TestBuildGenerationPrompt:
    """Tests for prompt builder."""

    def test_contains_question(self) -> None:
        """Question appears in prompt."""
        prompt = _build_generation_prompt("How many users?", "Table: users")
        assert "How many users?" in prompt

    def test_contains_schema(self) -> None:
        """Schema context in prompt."""
        prompt = _build_generation_prompt("test", "Table: users\n  id INTEGER")
        assert "Table: users" in prompt

    def test_read_only_rule(self) -> None:
        """Prompt instructs SELECT only."""
        prompt = _build_generation_prompt("test", "schema")
        assert "SELECT" in prompt
        assert "read-only" in prompt

    def test_no_markdown_rule(self) -> None:
        """Prompt requests raw SQL."""
        prompt = _build_generation_prompt("test", "schema")
        assert "no markdown" in prompt.lower() or "no explanation" in prompt.lower()


# ── _extract_sql ─────────────────────────────────────────────────────


class TestExtractSql:
    """Tests for SQL extraction from LLM response."""

    def test_raw_sql(self) -> None:
        """Raw SQL passes through."""
        assert _extract_sql("SELECT * FROM users") == "SELECT * FROM users"

    def test_strips_whitespace(self) -> None:
        """Leading/trailing whitespace removed."""
        assert _extract_sql("  SELECT 1  \n") == "SELECT 1"

    def test_strips_markdown_fences(self) -> None:
        """Markdown ```sql ... ``` stripped."""
        text = "```sql\nSELECT * FROM users\n```"
        assert _extract_sql(text) == "SELECT * FROM users"

    def test_strips_plain_fences(self) -> None:
        """Plain ``` ... ``` stripped."""
        text = "```\nSELECT 1\n```"
        assert _extract_sql(text) == "SELECT 1"

    def test_case_insensitive_fences(self) -> None:
        """```SQL works too."""
        text = "```SQL\nSELECT 1\n```"
        assert _extract_sql(text) == "SELECT 1"

    def test_empty_raises(self) -> None:
        """Empty response raises GenerationError."""
        with pytest.raises(GenerationError, match="empty"):
            _extract_sql("")

    def test_whitespace_only_raises(self) -> None:
        """Whitespace-only raises."""
        with pytest.raises(GenerationError, match="empty"):
            _extract_sql("   \n  ")

    def test_fences_only_raises(self) -> None:
        """Fences with no content raises."""
        with pytest.raises(GenerationError, match="No SQL"):
            _extract_sql("```sql\n```")

    def test_multiline_sql(self) -> None:
        """Multi-line SQL preserved."""
        sql = "SELECT u.name,\n       COUNT(o.id)\nFROM users u\nJOIN orders o ON o.user_id = u.id\nGROUP BY u.name"
        assert _extract_sql(sql) == sql


# ── generate_sql ─────────────────────────────────────────────────────


class TestGenerateSql:
    """Tests for the main generate_sql function."""

    def test_empty_question_raises(
        self, scan_result: ScanResult, route_result: RouteResult,
    ) -> None:
        """Empty question raises GenerationError."""
        with pytest.raises(GenerationError, match="empty"):
            generate_sql("", scan_result, route_result, model="m", api_key="k")

    def test_whitespace_question_raises(
        self, scan_result: ScanResult, route_result: RouteResult,
    ) -> None:
        """Whitespace-only question raises."""
        with pytest.raises(GenerationError, match="empty"):
            generate_sql("  ", scan_result, route_result, model="m", api_key="k")

    def test_no_matched_tables_raises(self, scan_result: ScanResult) -> None:
        """Empty route result raises."""
        empty_route = RouteResult(query="q", matched_tables=[], total_candidates=0)
        with pytest.raises(GenerationError, match="No tables"):
            generate_sql("q", scan_result, empty_route, model="m", api_key="k")

    def test_calls_litellm(
        self,
        scan_result: ScanResult,
        route_result: RouteResult,
    ) -> None:
        """LLM is called with correct parameters."""
        mock_litellm = MagicMock()
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = "SELECT COUNT(*) FROM users"
        mock_litellm.completion.return_value = mock_response

        with patch.dict("sys.modules", {"litellm": mock_litellm}):
            result = generate_sql(
                "How many users?",
                scan_result,
                route_result,
                model="gpt-4o",
                api_key="test-key",
            )

        assert result == "SELECT COUNT(*) FROM users"
        mock_litellm.completion.assert_called_once()
        call_kwargs = mock_litellm.completion.call_args
        assert call_kwargs.kwargs["model"] == "gpt-4o"
        assert call_kwargs.kwargs["api_key"] == "test-key"
        assert call_kwargs.kwargs["temperature"] == 0.0

    def test_llm_failure_raises(
        self,
        scan_result: ScanResult,
        route_result: RouteResult,
    ) -> None:
        """LLM call failure raises LLMError."""
        mock_litellm = MagicMock()
        mock_litellm.completion.side_effect = RuntimeError("API down")

        with patch.dict("sys.modules", {"litellm": mock_litellm}):
            with pytest.raises(LLMError, match="LLM call failed"):
                generate_sql(
                    "How many users?",
                    scan_result,
                    route_result,
                    model="gpt-4o",
                    api_key="test-key",
                )

    def test_returns_extracted_sql(
        self,
        scan_result: ScanResult,
        route_result: RouteResult,
    ) -> None:
        """Markdown fences stripped from LLM response."""
        mock_litellm = MagicMock()
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = "```sql\nSELECT 1\n```"
        mock_litellm.completion.return_value = mock_response

        with patch.dict("sys.modules", {"litellm": mock_litellm}):
            result = generate_sql(
                "test", scan_result, route_result, model="m", api_key="k",
            )
        assert result == "SELECT 1"

    def test_prompt_contains_selected_tables(
        self,
        scan_result: ScanResult,
        route_result: RouteResult,
    ) -> None:
        """Prompt sent to LLM contains the selected tables."""
        mock_litellm = MagicMock()
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = "SELECT 1"
        mock_litellm.completion.return_value = mock_response

        with patch.dict("sys.modules", {"litellm": mock_litellm}):
            generate_sql(
                "How many users?",
                scan_result,
                route_result,
                model="m",
                api_key="k",
            )

        prompt = mock_litellm.completion.call_args.kwargs["messages"][0]["content"]
        assert "users" in prompt
        assert "orders" in prompt
        # Products not selected by router
        assert "products" not in prompt

    def test_prompt_has_no_sample_data(
        self,
        route_result: RouteResult,
    ) -> None:
        """Prompt never contains sample data values (CODING_RULES rule 8)."""
        sr = ScanResult(
            database_name="test",
            tables=[
                TableInfo(
                    name="users",
                    columns=[ColumnInfo(name="id", data_type="INT")],
                ),
                TableInfo(
                    name="orders",
                    columns=[ColumnInfo(name="id", data_type="INT")],
                ),
            ],
        )

        mock_litellm = MagicMock()
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = "SELECT 1"
        mock_litellm.completion.return_value = mock_response

        with patch.dict("sys.modules", {"litellm": mock_litellm}):
            generate_sql("q", sr, route_result, model="m", api_key="k")

        prompt = mock_litellm.completion.call_args.kwargs["messages"][0]["content"]
        assert "sample_values" not in prompt.lower()
