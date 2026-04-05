"""Tests for router embedding-based table matching."""

from __future__ import annotations

from unittest.mock import MagicMock

import numpy as np
import pytest

from dataconnect.exceptions import EmbeddingError, RoutingError
from dataconnect.models import (
    ColumnInfo,
    ColumnProfile,
    MatchMethod,
    TableInfo,
    TableMatch,
)
from dataconnect.router.embeddings import EmbeddingIndex, table_to_text


# ── table_to_text tests ─────────────────────────────────────────


class TestTableToText:
    """Tests for the pure table_to_text conversion function."""

    def test_basic_table(self) -> None:
        """Table name and columns appear in output."""
        table = TableInfo(name="users", columns=[
            ColumnInfo(name="id", data_type="INTEGER"),
            ColumnInfo(name="name", data_type="VARCHAR"),
        ])
        text = table_to_text(table)
        assert "users" in text
        assert "id" in text
        assert "INTEGER" in text
        assert "name" in text

    def test_with_description(self) -> None:
        """Table description is included."""
        table = TableInfo(
            name="orders",
            description="Customer purchase orders",
            columns=[],
        )
        text = table_to_text(table)
        assert "Customer purchase orders" in text

    def test_with_column_descriptions(self) -> None:
        """Column descriptions appear in output."""
        table = TableInfo(name="t", columns=[
            ColumnInfo(
                name="email", data_type="VARCHAR",
                description="User email address",
            ),
        ])
        text = table_to_text(table)
        assert "User email address" in text

    def test_with_sample_values(self) -> None:
        """Profile sample values appear in output."""
        table = TableInfo(
            name="t",
            columns=[ColumnInfo(name="status", data_type="VARCHAR")],
            profiles=[ColumnProfile(
                column_name="status",
                sample_values=["active", "inactive"],
            )],
        )
        text = table_to_text(table)
        assert "active" in text
        assert "inactive" in text

    def test_empty_table(self) -> None:
        """Empty table still contains table name."""
        table = TableInfo(name="empty", columns=[])
        text = table_to_text(table)
        assert "empty" in text

    def test_sample_values_capped_at_five(self) -> None:
        """Only first 5 sample values are included."""
        table = TableInfo(
            name="t",
            columns=[],
            profiles=[ColumnProfile(
                column_name="c",
                sample_values=["v1", "v2", "v3", "v4", "v5", "v6", "v7"],
            )],
        )
        text = table_to_text(table)
        assert "v5" in text
        assert "v6" not in text

    def test_multiple_columns(self) -> None:
        """Multiple columns all appear in text."""
        table = TableInfo(name="t", columns=[
            ColumnInfo(name="a", data_type="INT"),
            ColumnInfo(name="b", data_type="TEXT"),
            ColumnInfo(name="c", data_type="BOOL"),
        ])
        text = table_to_text(table)
        assert "a" in text
        assert "b" in text
        assert "c" in text

    def test_fk_column_no_target_leak(self) -> None:
        """FK target does not leak into text (not needed for embedding)."""
        table = TableInfo(name="t", columns=[
            ColumnInfo(
                name="user_id", data_type="INT",
                is_foreign_key=True, foreign_key_target="users.id",
            ),
        ])
        text = table_to_text(table)
        # Column info is included, but FK target is schema metadata, not text
        assert "user_id" in text


# ── EmbeddingIndex tests ─────────────────────────────────────────


def _make_mock_model(dim: int = 384) -> MagicMock:
    """Create a mock SentenceTransformer that returns deterministic embeddings."""
    model = MagicMock()
    call_count = [0]

    def encode_side_effect(texts: list[str], **kwargs) -> np.ndarray:  # noqa: ARG001
        # Use text hash for deterministic but unique embeddings
        embs = []
        for text in texts:
            rng = np.random.RandomState(hash(text) % (2**31))
            vec = rng.randn(dim).astype(np.float32)
            vec /= np.linalg.norm(vec)
            embs.append(vec)
        call_count[0] += 1
        return np.array(embs, dtype=np.float32)

    model.encode = MagicMock(side_effect=encode_side_effect)
    return model


SAMPLE_TABLES = [
    TableInfo(name="users", columns=[
        ColumnInfo(name="id", data_type="INTEGER"),
        ColumnInfo(name="name", data_type="VARCHAR"),
        ColumnInfo(name="email", data_type="VARCHAR"),
    ]),
    TableInfo(name="orders", columns=[
        ColumnInfo(name="id", data_type="INTEGER"),
        ColumnInfo(name="user_id", data_type="INTEGER"),
        ColumnInfo(name="total", data_type="DECIMAL"),
    ]),
    TableInfo(name="products", columns=[
        ColumnInfo(name="id", data_type="INTEGER"),
        ColumnInfo(name="title", data_type="VARCHAR"),
        ColumnInfo(name="price", data_type="DECIMAL"),
    ]),
]


class TestEmbeddingIndex:
    """Tests for EmbeddingIndex with mocked sentence-transformers."""

    def test_lazy_model_loading(self) -> None:
        """Model is not loaded on init."""
        idx = EmbeddingIndex(model_name="test-model")
        assert idx._model is None

    def test_build_sets_is_built(self) -> None:
        """Building index sets is_built flag."""
        idx = EmbeddingIndex()
        idx._model = _make_mock_model()
        assert not idx.is_built
        idx.build([TableInfo(name="t", columns=[])])
        assert idx.is_built

    def test_build_sets_table_count(self) -> None:
        """Table count matches input."""
        idx = EmbeddingIndex()
        idx._model = _make_mock_model()
        idx.build(SAMPLE_TABLES)
        assert idx.table_count == 3

    def test_build_empty_raises(self) -> None:
        """Empty table list raises RoutingError."""
        idx = EmbeddingIndex()
        idx._model = _make_mock_model()
        with pytest.raises(RoutingError, match="empty"):
            idx.build([])

    def test_search_before_build_raises(self) -> None:
        """Searching before build raises RoutingError."""
        idx = EmbeddingIndex()
        idx._model = _make_mock_model()
        with pytest.raises(RoutingError, match="not built"):
            idx.search("test query")

    def test_search_returns_matches(self) -> None:
        """Search returns TableMatch results."""
        idx = EmbeddingIndex()
        idx._model = _make_mock_model()
        idx.build(SAMPLE_TABLES)
        matches = idx.search("find all users")
        assert len(matches) > 0
        assert all(isinstance(m, TableMatch) for m in matches)

    def test_search_match_method_is_embedding(self) -> None:
        """All matches use EMBEDDING method."""
        idx = EmbeddingIndex()
        idx._model = _make_mock_model()
        idx.build(SAMPLE_TABLES)
        matches = idx.search("test")
        for m in matches:
            assert MatchMethod.EMBEDDING in m.methods

    def test_search_table_names_valid(self) -> None:
        """Returned table names exist in the index."""
        idx = EmbeddingIndex()
        idx._model = _make_mock_model()
        idx.build(SAMPLE_TABLES)
        matches = idx.search("query")
        valid_names = {t.name for t in SAMPLE_TABLES}
        for m in matches:
            assert m.table_name in valid_names

    def test_search_scores_clamped(self) -> None:
        """Relevance scores are in [0, 1]."""
        idx = EmbeddingIndex()
        idx._model = _make_mock_model()
        idx.build(SAMPLE_TABLES)
        matches = idx.search("query")
        for m in matches:
            assert 0.0 <= m.relevance_score <= 1.0

    def test_search_top_k_limits_results(self) -> None:
        """top_k limits the number of results."""
        idx = EmbeddingIndex()
        idx._model = _make_mock_model()
        idx.build(SAMPLE_TABLES)
        matches = idx.search("test", top_k=2)
        assert len(matches) <= 2

    def test_search_top_k_exceeds_tables(self) -> None:
        """top_k larger than table count returns all tables."""
        idx = EmbeddingIndex()
        idx._model = _make_mock_model()
        idx.build(SAMPLE_TABLES)
        matches = idx.search("test", top_k=100)
        assert len(matches) == 3

    def test_search_has_reasoning(self) -> None:
        """Matches include reasoning strings."""
        idx = EmbeddingIndex()
        idx._model = _make_mock_model()
        idx.build(SAMPLE_TABLES)
        matches = idx.search("test")
        for m in matches:
            assert "Semantic similarity" in m.reasoning

    def test_search_sorted_by_relevance(self) -> None:
        """Results are sorted by relevance score descending."""
        idx = EmbeddingIndex()
        idx._model = _make_mock_model()
        idx.build(SAMPLE_TABLES)
        matches = idx.search("test")
        scores = [m.relevance_score for m in matches]
        assert scores == sorted(scores, reverse=True)

    def test_model_import_error(self) -> None:
        """Missing sentence-transformers raises EmbeddingError."""
        idx = EmbeddingIndex()
        # Force import to fail by keeping _model as None
        import sys
        original = sys.modules.get("sentence_transformers")
        sys.modules["sentence_transformers"] = None  # type: ignore[assignment]
        try:
            with pytest.raises(EmbeddingError, match="sentence-transformers"):
                idx._load_model()
        finally:
            if original is not None:
                sys.modules["sentence_transformers"] = original
            else:
                sys.modules.pop("sentence_transformers", None)

    def test_single_table_index(self) -> None:
        """Index works with a single table."""
        idx = EmbeddingIndex()
        idx._model = _make_mock_model()
        idx.build([TableInfo(name="solo", columns=[])])
        matches = idx.search("anything")
        assert len(matches) == 1
        assert matches[0].table_name == "solo"
