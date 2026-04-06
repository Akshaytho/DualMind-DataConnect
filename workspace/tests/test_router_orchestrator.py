"""Tests for router orchestrator — route_query() and helpers."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from dataconnect.exceptions import LLMError, RoutingError
from dataconnect.models import (
    ColumnInfo,
    MatchMethod,
    RelationshipInfo,
    RelationshipType,
    RouteResult,
    ScanResult,
    TableInfo,
    TableMatch,
)
from dataconnect.router import (
    _build_llm_prompt,
    _merge_matches,
    _parse_llm_response,
    route_query,
)


# ── _merge_matches tests ──────────────────────────────────────────


class TestMergeMatches:
    """Tests for the match-merging logic."""

    def test_empty_all(self) -> None:
        result = _merge_matches([], [], [])
        assert result == []

    def test_single_source(self) -> None:
        matches = [
            TableMatch(
                table_name="users",
                methods=[MatchMethod.EMBEDDING],
                relevance_score=0.9,
                reasoning="Semantic",
            ),
        ]
        result = _merge_matches(matches, [], [])
        assert len(result) == 1
        assert result[0].table_name == "users"

    def test_dedup_same_table(self) -> None:
        emb = [TableMatch(
            table_name="users", methods=[MatchMethod.EMBEDDING],
            relevance_score=0.8, reasoning="Embedding match",
        )]
        graph = [TableMatch(
            table_name="users", methods=[MatchMethod.GRAPH_WALK],
            relevance_score=0.6, reasoning="Graph walk",
        )]
        result = _merge_matches(emb, graph, [])
        assert len(result) == 1
        assert MatchMethod.EMBEDDING in result[0].methods
        assert MatchMethod.GRAPH_WALK in result[0].methods
        assert result[0].relevance_score == 0.8  # highest

    def test_three_methods_merge(self) -> None:
        emb = [TableMatch(
            table_name="orders", methods=[MatchMethod.EMBEDDING],
            relevance_score=0.7, reasoning="Emb",
        )]
        graph = [TableMatch(
            table_name="orders", methods=[MatchMethod.GRAPH_WALK],
            relevance_score=0.5, reasoning="Graph",
        )]
        llm = [TableMatch(
            table_name="orders", methods=[MatchMethod.LLM_CROSSCHECK],
            relevance_score=0.8, reasoning="LLM",
        )]
        result = _merge_matches(emb, graph, llm)
        assert len(result) == 1
        assert len(result[0].methods) == 3
        assert result[0].relevance_score == 0.8

    def test_sorted_by_score_desc(self) -> None:
        emb = [
            TableMatch(table_name="low", methods=[MatchMethod.EMBEDDING],
                       relevance_score=0.3, reasoning="Low"),
            TableMatch(table_name="high", methods=[MatchMethod.EMBEDDING],
                       relevance_score=0.9, reasoning="High"),
            TableMatch(table_name="mid", methods=[MatchMethod.EMBEDDING],
                       relevance_score=0.6, reasoning="Mid"),
        ]
        result = _merge_matches(emb, [], [])
        scores = [m.relevance_score for m in result]
        assert scores == sorted(scores, reverse=True)

    def test_union_strategy(self) -> None:
        """Any table from any method is included (maximize recall)."""
        emb = [TableMatch(table_name="users", methods=[MatchMethod.EMBEDDING],
                          relevance_score=0.9, reasoning="Emb")]
        graph = [TableMatch(table_name="products", methods=[MatchMethod.GRAPH_WALK],
                            relevance_score=0.5, reasoning="Graph")]
        llm = [TableMatch(table_name="categories", methods=[MatchMethod.LLM_CROSSCHECK],
                          relevance_score=0.8, reasoning="LLM")]
        result = _merge_matches(emb, graph, llm)
        names = {m.table_name for m in result}
        assert names == {"users", "products", "categories"}


# ── _build_llm_prompt tests ───────────────────────────────────────


class TestBuildLLMPrompt:
    """Tests for LLM prompt construction."""

    def test_contains_query(self) -> None:
        prompt = _build_llm_prompt("total sales", ["orders"], ["orders"])
        assert "total sales" in prompt

    def test_contains_all_tables(self) -> None:
        prompt = _build_llm_prompt("q", ["users", "orders", "products"], [])
        assert "users" in prompt
        assert "orders" in prompt
        assert "products" in prompt

    def test_contains_candidates(self) -> None:
        prompt = _build_llm_prompt("q", ["users"], ["users"])
        assert "Already selected" in prompt
        assert "users" in prompt

    def test_asks_for_json(self) -> None:
        prompt = _build_llm_prompt("q", ["t"], [])
        assert "JSON" in prompt


# ── _parse_llm_response tests ────────────────────────────────────


class TestParseLLMResponse:
    """Tests for parsing LLM cross-check responses."""

    def test_valid_json(self) -> None:
        response = json.dumps({
            "tables": ["users", "orders"],
            "reasoning": "Both needed for user purchase query",
        })
        result = _parse_llm_response(response, {"users", "orders", "products"})
        assert len(result) == 2
        names = {m.table_name for m in result}
        assert names == {"users", "orders"}

    def test_filters_nonexistent_tables(self) -> None:
        response = json.dumps({"tables": ["users", "fake_table"], "reasoning": "test"})
        result = _parse_llm_response(response, {"users", "orders"})
        assert len(result) == 1
        assert result[0].table_name == "users"

    def test_markdown_code_fence(self) -> None:
        response = '```json\n{"tables": ["users"], "reasoning": "test"}\n```'
        result = _parse_llm_response(response, {"users"})
        assert len(result) == 1

    def test_invalid_json_raises(self) -> None:
        with pytest.raises(LLMError, match="invalid JSON"):
            _parse_llm_response("not json", {"users"})

    def test_missing_tables_key(self) -> None:
        with pytest.raises(LLMError, match="missing 'tables'"):
            _parse_llm_response('{"result": []}', {"users"})

    def test_tables_not_list(self) -> None:
        with pytest.raises(LLMError, match="not a list"):
            _parse_llm_response('{"tables": "users"}', {"users"})

    def test_non_string_items_skipped(self) -> None:
        response = json.dumps({"tables": ["users", 123, None], "reasoning": "t"})
        result = _parse_llm_response(response, {"users"})
        assert len(result) == 1

    def test_llm_match_method(self) -> None:
        response = json.dumps({"tables": ["users"], "reasoning": "test"})
        result = _parse_llm_response(response, {"users"})
        assert result[0].methods == [MatchMethod.LLM_CROSSCHECK]

    def test_default_score_is_0_8(self) -> None:
        response = json.dumps({"tables": ["users"], "reasoning": "test"})
        result = _parse_llm_response(response, {"users"})
        assert result[0].relevance_score == 0.8

    def test_empty_tables_list(self) -> None:
        response = json.dumps({"tables": [], "reasoning": "none needed"})
        result = _parse_llm_response(response, {"users"})
        assert result == []


# ── route_query tests ─────────────────────────────────────────────


def _make_scan_result(
    tables: list[TableInfo] | None = None,
    relationships: list[RelationshipInfo] | None = None,
) -> ScanResult:
    """Helper to create ScanResult for tests."""
    if tables is None:
        tables = [
            TableInfo(name="users", columns=[
                ColumnInfo(name="id", data_type="INTEGER"),
                ColumnInfo(name="name", data_type="VARCHAR"),
            ], description="User accounts"),
            TableInfo(name="orders", columns=[
                ColumnInfo(name="id", data_type="INTEGER"),
                ColumnInfo(name="user_id", data_type="INTEGER"),
                ColumnInfo(name="amount", data_type="INTEGER"),
            ], description="Purchase orders"),
            TableInfo(name="products", columns=[
                ColumnInfo(name="id", data_type="INTEGER"),
                ColumnInfo(name="name", data_type="VARCHAR"),
                ColumnInfo(name="price", data_type="DECIMAL"),
            ], description="Product catalog"),
        ]
    return ScanResult(
        database_name="test_db",
        tables=tables,
        relationships=relationships or [],
        token_estimate=500,
    )


def _mock_embedding_index(match_tables: list[str]) -> MagicMock:
    """Create a mock EmbeddingIndex that returns specified tables."""
    index = MagicMock()
    index.is_built = True
    index.search.return_value = [
        TableMatch(
            table_name=name,
            methods=[MatchMethod.EMBEDDING],
            relevance_score=0.9 - i * 0.1,
            reasoning=f"Embedding match: {name}",
        )
        for i, name in enumerate(match_tables)
    ]
    return index


class TestRouteQuery:
    """Tests for the route_query orchestrator."""

    def test_empty_query_raises(self) -> None:
        scan = _make_scan_result()
        with pytest.raises(RoutingError, match="empty"):
            route_query("", scan, embedding_index=_mock_embedding_index([]))

    def test_whitespace_query_raises(self) -> None:
        scan = _make_scan_result()
        with pytest.raises(RoutingError, match="empty"):
            route_query("   ", scan, embedding_index=_mock_embedding_index([]))

    def test_no_tables_raises(self) -> None:
        scan = _make_scan_result(tables=[])
        with pytest.raises(RoutingError, match="no tables"):
            route_query("test", scan)

    def test_returns_route_result(self) -> None:
        scan = _make_scan_result()
        index = _mock_embedding_index(["users"])
        result = route_query("find users", scan, embedding_index=index)
        assert isinstance(result, RouteResult)
        assert result.query == "find users"

    def test_total_candidates_is_table_count(self) -> None:
        scan = _make_scan_result()
        index = _mock_embedding_index(["users"])
        result = route_query("find users", scan, embedding_index=index)
        assert result.total_candidates == 3

    def test_embedding_matches_included(self) -> None:
        scan = _make_scan_result()
        index = _mock_embedding_index(["users", "orders"])
        result = route_query("user orders", scan, embedding_index=index)
        names = {m.table_name for m in result.matched_tables}
        assert "users" in names
        assert "orders" in names

    def test_graph_walk_adds_connected_tables(self) -> None:
        rels = [RelationshipInfo(
            source_table="orders", source_column="user_id",
            target_table="users", target_column="id",
            relationship_type=RelationshipType.DECLARED_FK,
            confidence=1.0,
        )]
        scan = _make_scan_result(relationships=rels)
        # Embedding only matches "orders", graph should find "users"
        index = _mock_embedding_index(["orders"])
        result = route_query("order total", scan, embedding_index=index)
        names = {m.table_name for m in result.matched_tables}
        assert "users" in names

    def test_builds_index_if_not_provided(self) -> None:
        """route_query creates and builds an EmbeddingIndex if none given."""
        scan = _make_scan_result()
        mock_index = MagicMock()
        mock_index.is_built = False
        mock_index.search.return_value = [
            TableMatch(table_name="users", methods=[MatchMethod.EMBEDDING],
                       relevance_score=0.9, reasoning="test"),
        ]
        result = route_query("test", scan, embedding_index=mock_index)
        mock_index.build.assert_called_once_with(scan.tables)

    def test_skips_build_if_already_built(self) -> None:
        scan = _make_scan_result()
        index = _mock_embedding_index(["users"])
        route_query("test", scan, embedding_index=index)
        index.build.assert_not_called()

    def test_llm_crosscheck_when_credentials_provided(self) -> None:
        scan = _make_scan_result()
        index = _mock_embedding_index(["users"])

        llm_response = json.dumps({
            "tables": ["users", "orders"],
            "reasoning": "Need orders for purchase history",
        })

        with patch("dataconnect.router._call_llm", return_value=llm_response):
            result = route_query(
                "user purchase history", scan,
                embedding_index=index,
                llm_model="gpt-4o",
                llm_api_key="test-key",
            )

        names = {m.table_name for m in result.matched_tables}
        assert "orders" in names

    def test_llm_failure_graceful(self) -> None:
        """LLM failure doesn't crash — falls back to embedding + graph."""
        scan = _make_scan_result()
        index = _mock_embedding_index(["users"])

        with patch("dataconnect.router._call_llm", side_effect=LLMError("fail")):
            result = route_query(
                "test", scan,
                embedding_index=index,
                llm_model="gpt-4o",
                llm_api_key="test-key",
            )

        assert isinstance(result, RouteResult)
        assert len(result.matched_tables) > 0

    def test_no_llm_without_model(self) -> None:
        """LLM cross-check skipped when no model specified."""
        scan = _make_scan_result()
        index = _mock_embedding_index(["users"])

        with patch("dataconnect.router._call_llm") as mock_llm:
            route_query("test", scan, embedding_index=index)
            mock_llm.assert_not_called()

    def test_no_llm_without_api_key(self) -> None:
        """LLM cross-check skipped when no API key."""
        scan = _make_scan_result()
        index = _mock_embedding_index(["users"])

        with patch("dataconnect.router._call_llm") as mock_llm:
            route_query("test", scan, embedding_index=index, llm_model="gpt-4o")
            mock_llm.assert_not_called()

    def test_merged_methods_on_overlap(self) -> None:
        """Table found by embedding + LLM gets both methods listed."""
        scan = _make_scan_result()
        index = _mock_embedding_index(["users"])

        llm_response = json.dumps({
            "tables": ["users"],
            "reasoning": "Users needed",
        })

        with patch("dataconnect.router._call_llm", return_value=llm_response):
            result = route_query(
                "find users", scan,
                embedding_index=index,
                llm_model="gpt-4o",
                llm_api_key="test-key",
            )

        users_match = next(
            m for m in result.matched_tables if m.table_name == "users"
        )
        assert MatchMethod.EMBEDDING in users_match.methods
        assert MatchMethod.LLM_CROSSCHECK in users_match.methods

    def test_custom_top_k(self) -> None:
        scan = _make_scan_result()
        index = _mock_embedding_index(["users"])
        route_query("test", scan, embedding_index=index, top_k=3)
        index.search.assert_called_once_with("test", top_k=3)


# ── _call_llm tests ──────────────────────────────────────────────


class TestCallLLM:
    """Tests for the _call_llm wrapper."""

    def test_missing_litellm_raises(self) -> None:
        from dataconnect.router import _call_llm

        with patch.dict("sys.modules", {"litellm": None}):
            with pytest.raises(LLMError, match="litellm required"):
                _call_llm("prompt", "model", "key")

    def test_calls_litellm_completion(self) -> None:
        from dataconnect.router import _call_llm

        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = "response text"

        mock_litellm = MagicMock()
        mock_litellm.completion.return_value = mock_response

        with patch.dict("sys.modules", {"litellm": mock_litellm}):
            result = _call_llm("test prompt", "gpt-4o", "sk-test")

        assert result == "response text"
        mock_litellm.completion.assert_called_once_with(
            model="gpt-4o",
            messages=[{"role": "user", "content": "test prompt"}],
            api_key="sk-test",
            temperature=0.0,
            max_tokens=1024,
        )

    def test_litellm_error_wrapped(self) -> None:
        from dataconnect.router import _call_llm

        mock_litellm = MagicMock()
        mock_litellm.completion.side_effect = RuntimeError("API error")

        with patch.dict("sys.modules", {"litellm": mock_litellm}):
            with pytest.raises(LLMError, match="LLM call failed"):
                _call_llm("prompt", "model", "key")
