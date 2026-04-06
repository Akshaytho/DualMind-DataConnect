"""Tests for FAISS optimization in the embedding index."""

from __future__ import annotations

import sys
from types import ModuleType
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from dataconnect.models import ColumnInfo, MatchMethod, TableInfo, TableMatch
from dataconnect.router.embeddings import EmbeddingIndex, _try_import_faiss


# ── Helpers ──────────────────────────────────────────────────────


def _make_mock_model(dim: int = 384) -> MagicMock:
    """Create a mock SentenceTransformer with deterministic embeddings."""
    model = MagicMock()

    def encode_side_effect(texts: list[str], **kwargs) -> np.ndarray:  # noqa: ARG001
        embs = []
        for text in texts:
            rng = np.random.RandomState(hash(text) % (2**31))
            vec = rng.randn(dim).astype(np.float32)
            vec /= np.linalg.norm(vec)
            embs.append(vec)
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
    TableInfo(name="reviews", columns=[
        ColumnInfo(name="id", data_type="INTEGER"),
        ColumnInfo(name="product_id", data_type="INTEGER"),
        ColumnInfo(name="rating", data_type="INTEGER"),
    ]),
    TableInfo(name="categories", columns=[
        ColumnInfo(name="id", data_type="INTEGER"),
        ColumnInfo(name="name", data_type="VARCHAR"),
    ]),
]


def _make_fake_faiss() -> ModuleType:
    """Create a minimal fake faiss module for testing."""
    faiss_mod = ModuleType("faiss")

    class FakeIndexFlatIP:
        """Minimal FAISS IndexFlatIP implementation for tests."""

        def __init__(self, dim: int) -> None:
            self.d = dim
            self.ntotal = 0
            self._data: np.ndarray | None = None

        def add(self, vectors: np.ndarray) -> None:
            """Add vectors to the index."""
            if self._data is None:
                self._data = vectors.copy()
            else:
                self._data = np.vstack([self._data, vectors])
            self.ntotal = self._data.shape[0]

        def search(
            self, query: np.ndarray, k: int,
        ) -> tuple[np.ndarray, np.ndarray]:
            """Search for k nearest neighbors by inner product."""
            assert self._data is not None
            sims = (query @ self._data.T)
            actual_k = min(k, self.ntotal)
            indices = np.argsort(-sims, axis=1)[:, :actual_k]
            scores = np.take_along_axis(sims, indices, axis=1)
            # Pad with -1 if fewer results than k
            if actual_k < k:
                pad_i = np.full(
                    (query.shape[0], k - actual_k), -1, dtype=np.int64,
                )
                pad_s = np.full(
                    (query.shape[0], k - actual_k), 0.0, dtype=np.float32,
                )
                indices = np.hstack([indices, pad_i])
                scores = np.hstack([scores, pad_s])
            return scores, indices

    faiss_mod.IndexFlatIP = FakeIndexFlatIP  # type: ignore[attr-defined]
    return faiss_mod


# ── _try_import_faiss tests ──────────────────────────────────────


class TestTryImportFaiss:
    """Tests for the lazy faiss import helper."""

    def test_returns_none_when_missing(self) -> None:
        """Returns None when faiss is not installed."""
        original = sys.modules.get("faiss")
        sys.modules["faiss"] = None  # type: ignore[assignment]
        try:
            result = _try_import_faiss()
            assert result is None
        finally:
            if original is not None:
                sys.modules["faiss"] = original
            else:
                sys.modules.pop("faiss", None)

    def test_returns_module_when_available(self) -> None:
        """Returns faiss module when available."""
        fake = _make_fake_faiss()
        original = sys.modules.get("faiss")
        sys.modules["faiss"] = fake
        try:
            result = _try_import_faiss()
            assert result is fake
        finally:
            if original is not None:
                sys.modules["faiss"] = original
            else:
                sys.modules.pop("faiss", None)


# ── FAISS backend tests ─────────────────────────────────────────


class TestFAISSBackend:
    """Tests for FAISS-accelerated embedding search."""

    def _build_index_with_faiss(
        self, tables: list[TableInfo] | None = None,
    ) -> EmbeddingIndex:
        """Build an index with fake FAISS available."""
        fake_faiss = _make_fake_faiss()
        idx = EmbeddingIndex(use_faiss=True)
        idx._model = _make_mock_model()
        with patch.dict(sys.modules, {"faiss": fake_faiss}):
            idx.build(tables or SAMPLE_TABLES)
        return idx

    def test_backend_is_faiss(self) -> None:
        """Backend reports 'faiss' when FAISS is available."""
        idx = self._build_index_with_faiss()
        assert idx.backend == "faiss"

    def test_faiss_index_created(self) -> None:
        """FAISS index object is created on build."""
        idx = self._build_index_with_faiss()
        assert idx._faiss_index is not None

    def test_faiss_index_has_correct_count(self) -> None:
        """FAISS index contains correct number of vectors."""
        idx = self._build_index_with_faiss()
        assert idx._faiss_index.ntotal == len(SAMPLE_TABLES)

    def test_search_returns_matches(self) -> None:
        """FAISS search returns TableMatch results."""
        idx = self._build_index_with_faiss()
        matches = idx.search("find all users")
        assert len(matches) > 0
        assert all(isinstance(m, TableMatch) for m in matches)

    def test_search_table_names_valid(self) -> None:
        """Returned table names exist in the index."""
        idx = self._build_index_with_faiss()
        matches = idx.search("product pricing")
        valid_names = {t.name for t in SAMPLE_TABLES}
        for m in matches:
            assert m.table_name in valid_names

    def test_search_scores_clamped(self) -> None:
        """Relevance scores are in [0, 1]."""
        idx = self._build_index_with_faiss()
        matches = idx.search("query")
        for m in matches:
            assert 0.0 <= m.relevance_score <= 1.0

    def test_search_method_is_embedding(self) -> None:
        """All matches use EMBEDDING method."""
        idx = self._build_index_with_faiss()
        matches = idx.search("test")
        for m in matches:
            assert MatchMethod.EMBEDDING in m.methods

    def test_search_top_k_limits(self) -> None:
        """top_k limits the number of results."""
        idx = self._build_index_with_faiss()
        matches = idx.search("test", top_k=2)
        assert len(matches) <= 2

    def test_search_sorted_by_relevance(self) -> None:
        """Results are sorted by relevance score descending."""
        idx = self._build_index_with_faiss()
        matches = idx.search("test")
        scores = [m.relevance_score for m in matches]
        assert scores == sorted(scores, reverse=True)

    def test_search_has_reasoning(self) -> None:
        """Matches include reasoning strings."""
        idx = self._build_index_with_faiss()
        matches = idx.search("test")
        for m in matches:
            assert "Semantic similarity" in m.reasoning

    def test_single_table(self) -> None:
        """FAISS works with a single table."""
        idx = self._build_index_with_faiss(
            [TableInfo(name="solo", columns=[])],
        )
        matches = idx.search("anything")
        assert len(matches) == 1
        assert matches[0].table_name == "solo"

    def test_top_k_exceeds_tables(self) -> None:
        """top_k larger than table count returns all tables."""
        idx = self._build_index_with_faiss()
        matches = idx.search("test", top_k=100)
        assert len(matches) == len(SAMPLE_TABLES)


# ── NumPy fallback tests ────────────────────────────────────────


class TestNumpyFallback:
    """Tests for numpy fallback when FAISS is disabled or unavailable."""

    def test_backend_is_numpy_when_disabled(self) -> None:
        """Backend reports 'numpy' when use_faiss=False."""
        idx = EmbeddingIndex(use_faiss=False)
        idx._model = _make_mock_model()
        idx.build(SAMPLE_TABLES)
        assert idx.backend == "numpy"

    def test_backend_is_numpy_when_unavailable(self) -> None:
        """Backend reports 'numpy' when faiss import fails."""
        idx = EmbeddingIndex(use_faiss=True)
        idx._model = _make_mock_model()
        original = sys.modules.get("faiss")
        sys.modules["faiss"] = None  # type: ignore[assignment]
        try:
            idx.build(SAMPLE_TABLES)
        finally:
            if original is not None:
                sys.modules["faiss"] = original
            else:
                sys.modules.pop("faiss", None)
        assert idx.backend == "numpy"
        assert idx._faiss_index is None

    def test_numpy_search_still_works(self) -> None:
        """Search works with numpy fallback."""
        idx = EmbeddingIndex(use_faiss=False)
        idx._model = _make_mock_model()
        idx.build(SAMPLE_TABLES)
        matches = idx.search("find all users")
        assert len(matches) > 0
        assert all(isinstance(m, TableMatch) for m in matches)

    def test_numpy_scores_clamped(self) -> None:
        """Numpy fallback scores are in [0, 1]."""
        idx = EmbeddingIndex(use_faiss=False)
        idx._model = _make_mock_model()
        idx.build(SAMPLE_TABLES)
        for m in idx.search("query"):
            assert 0.0 <= m.relevance_score <= 1.0


# ── Consistency tests ────────────────────────────────────────────


class TestBackendConsistency:
    """Verify FAISS and numpy backends produce equivalent results."""

    def test_same_table_names_returned(self) -> None:
        """Both backends return the same set of table names."""
        # Build numpy index
        np_idx = EmbeddingIndex(use_faiss=False)
        np_idx._model = _make_mock_model()
        np_idx.build(SAMPLE_TABLES)
        np_matches = np_idx.search("user orders")

        # Build FAISS index
        fake_faiss = _make_fake_faiss()
        faiss_idx = EmbeddingIndex(use_faiss=True)
        faiss_idx._model = _make_mock_model()
        with patch.dict(sys.modules, {"faiss": fake_faiss}):
            faiss_idx.build(SAMPLE_TABLES)
        faiss_matches = faiss_idx.search("user orders")

        np_names = [m.table_name for m in np_matches]
        faiss_names = [m.table_name for m in faiss_matches]
        assert np_names == faiss_names

    def test_same_scores_returned(self) -> None:
        """Both backends return similar scores (within float tolerance)."""
        np_idx = EmbeddingIndex(use_faiss=False)
        np_idx._model = _make_mock_model()
        np_idx.build(SAMPLE_TABLES)
        np_matches = np_idx.search("product reviews")

        fake_faiss = _make_fake_faiss()
        faiss_idx = EmbeddingIndex(use_faiss=True)
        faiss_idx._model = _make_mock_model()
        with patch.dict(sys.modules, {"faiss": fake_faiss}):
            faiss_idx.build(SAMPLE_TABLES)
        faiss_matches = faiss_idx.search("product reviews")

        for np_m, faiss_m in zip(np_matches, faiss_matches):
            assert abs(np_m.relevance_score - faiss_m.relevance_score) < 1e-5

    def test_same_result_count(self) -> None:
        """Both backends return the same number of results."""
        np_idx = EmbeddingIndex(use_faiss=False)
        np_idx._model = _make_mock_model()
        np_idx.build(SAMPLE_TABLES)

        fake_faiss = _make_fake_faiss()
        faiss_idx = EmbeddingIndex(use_faiss=True)
        faiss_idx._model = _make_mock_model()
        with patch.dict(sys.modules, {"faiss": fake_faiss}):
            faiss_idx.build(SAMPLE_TABLES)

        for query in ["users", "sales", "analytics"]:
            np_n = len(np_idx.search(query, top_k=3))
            faiss_n = len(faiss_idx.search(query, top_k=3))
            assert np_n == faiss_n


# ── pyproject.toml [faiss] extra ─────────────────────────────────


class TestFaissPackaging:
    """Verify [faiss] optional dependency in pyproject.toml."""

    def test_faiss_extra_exists(self) -> None:
        """[faiss] optional dependency group exists."""
        if sys.version_info >= (3, 11):
            import tomllib
        else:
            pytest.skip("tomllib requires Python 3.11+")
        from pathlib import Path
        pyproject = Path(__file__).resolve().parent.parent / "pyproject.toml"
        with open(pyproject, "rb") as f:
            data = tomllib.load(f)
        assert "faiss" in data["project"]["optional-dependencies"]

    def test_faiss_has_faiss_cpu(self) -> None:
        """faiss-cpu is in [faiss] extras."""
        if sys.version_info >= (3, 11):
            import tomllib
        else:
            pytest.skip("tomllib requires Python 3.11+")
        from pathlib import Path
        pyproject = Path(__file__).resolve().parent.parent / "pyproject.toml"
        with open(pyproject, "rb") as f:
            data = tomllib.load(f)
        deps = data["project"]["optional-dependencies"]["faiss"]
        dep_names = [d.split("==")[0] for d in deps]
        assert "faiss-cpu" in dep_names

    def test_faiss_deps_pinned(self) -> None:
        """All faiss dependencies use exact version pinning."""
        if sys.version_info >= (3, 11):
            import tomllib
        else:
            pytest.skip("tomllib requires Python 3.11+")
        from pathlib import Path
        pyproject = Path(__file__).resolve().parent.parent / "pyproject.toml"
        with open(pyproject, "rb") as f:
            data = tomllib.load(f)
        for dep in data["project"]["optional-dependencies"]["faiss"]:
            assert "==" in dep, f"Faiss dep not pinned: {dep}"

    def test_faiss_not_in_core(self) -> None:
        """faiss-cpu is NOT in core deps."""
        if sys.version_info >= (3, 11):
            import tomllib
        else:
            pytest.skip("tomllib requires Python 3.11+")
        from pathlib import Path
        pyproject = Path(__file__).resolve().parent.parent / "pyproject.toml"
        with open(pyproject, "rb") as f:
            data = tomllib.load(f)
        dep_names = [d.split("==")[0] for d in data["project"]["dependencies"]]
        assert "faiss-cpu" not in dep_names
