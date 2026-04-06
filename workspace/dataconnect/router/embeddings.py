"""Embedding-based semantic table matching.

Uses sentence-transformers to embed table descriptions and cosine
similarity for nearest-neighbor search. Supports two backends:

- **FAISS** (preferred): SIMD-optimized exact search via IndexFlatIP.
  Install with ``pip install dataconnect[faiss]``.
- **NumPy** (fallback): exact dot-product search, sufficient for <100 tables.

The embedding model and FAISS backend are loaded lazily on first use.
"""

from __future__ import annotations

import logging
from typing import Any

import numpy as np

from dataconnect.config import EMBEDDING_MODEL, MAX_RELEVANT_TABLES
from dataconnect.exceptions import EmbeddingError, RoutingError
from dataconnect.models import MatchMethod, TableInfo, TableMatch

logger = logging.getLogger(__name__)


def _try_import_faiss() -> Any | None:
    """Try to import faiss; return module or None if unavailable."""
    try:
        import faiss  # type: ignore[import-untyped]
        return faiss
    except ImportError:
        return None


def table_to_text(table: TableInfo) -> str:
    """Convert table metadata to a text representation for embedding.

    Combines table name, description, column names/types, and sample
    values into a single string that captures the table's semantic meaning.

    Args:
        table: Table metadata from scanner.

    Returns:
        Text representation suitable for embedding.
    """
    parts: list[str] = [f"Table: {table.name}"]

    if table.description:
        parts.append(table.description)

    col_parts: list[str] = []
    for col in table.columns:
        col_text = f"{col.name} ({col.data_type})"
        if col.description:
            col_text += f": {col.description}"
        col_parts.append(col_text)

    if col_parts:
        parts.append("Columns: " + ", ".join(col_parts))

    # Add sample values from profiles for richer semantic signal
    for profile in table.profiles:
        if profile.sample_values:
            vals = ", ".join(profile.sample_values[:5])
            parts.append(f"{profile.column_name} values: {vals}")

    return ". ".join(parts)


class EmbeddingIndex:
    """Semantic search over table metadata using sentence-transformers.

    Embeds table descriptions and uses cosine similarity for matching.
    The model is loaded lazily on first use. Normalized embeddings make
    dot product equivalent to cosine similarity.
    """

    def __init__(
        self,
        model_name: str = EMBEDDING_MODEL,
        *,
        use_faiss: bool = True,
    ) -> None:
        """Initialize the embedding index.

        Args:
            model_name: Name of the sentence-transformers model to use.
            use_faiss: If True, use FAISS when available (default).
                Set to False to force numpy fallback.
        """
        self._model_name = model_name
        self._model: Any = None  # SentenceTransformer, lazy loaded
        self._embeddings: np.ndarray | None = None
        self._table_names: list[str] = []
        self._use_faiss = use_faiss
        self._faiss_index: Any = None  # faiss.IndexFlatIP, optional

    def _load_model(self) -> Any:
        """Load the sentence-transformer model (lazy, first call only).

        Returns:
            Loaded SentenceTransformer model.

        Raises:
            EmbeddingError: If sentence-transformers is not installed.
        """
        if self._model is None:
            try:
                from sentence_transformers import SentenceTransformer
            except ImportError as exc:
                raise EmbeddingError(
                    "sentence-transformers required for embedding search. "
                    "Install with: pip install sentence-transformers"
                ) from exc
            self._model = SentenceTransformer(self._model_name)
            logger.info("Loaded embedding model: %s", self._model_name)
        return self._model

    def _encode(self, texts: list[str]) -> np.ndarray:
        """Encode texts to normalized embedding vectors.

        Args:
            texts: Strings to embed.

        Returns:
            2D array of shape (len(texts), embedding_dim), L2-normalized.
        """
        model = self._load_model()
        embeddings = model.encode(
            texts, normalize_embeddings=True, show_progress_bar=False,
        )
        return np.asarray(embeddings, dtype=np.float32)

    def build(self, tables: list[TableInfo]) -> None:
        """Build embedding index from table metadata.

        Args:
            tables: Tables to index for semantic search.

        Raises:
            RoutingError: If no tables provided.
        """
        if not tables:
            raise RoutingError("Cannot build embedding index from empty table list")

        texts = [table_to_text(t) for t in tables]
        self._table_names = [t.name for t in tables]
        self._embeddings = self._encode(texts)

        # Build FAISS index if available and requested
        self._faiss_index = None
        if self._use_faiss:
            faiss = _try_import_faiss()
            if faiss is not None:
                dim = self._embeddings.shape[1]
                self._faiss_index = faiss.IndexFlatIP(dim)
                self._faiss_index.add(self._embeddings)
                logger.info("FAISS index built: %d vectors", len(tables))

        backend = "FAISS" if self._faiss_index is not None else "numpy"
        logger.info(
            "Built embedding index (%s): %d tables, %d dimensions",
            backend, len(tables), self._embeddings.shape[1],
        )

    def search(
        self, query: str, top_k: int = MAX_RELEVANT_TABLES,
    ) -> list[TableMatch]:
        """Find tables semantically relevant to a natural language query.

        Args:
            query: Natural language question about the database.
            top_k: Maximum number of tables to return.

        Returns:
            Ranked list of TableMatch results with relevance scores.

        Raises:
            RoutingError: If index hasn't been built yet.
        """
        if self._embeddings is None:
            raise RoutingError("Embedding index not built. Call build() first.")

        query_emb = self._encode([query])
        k = min(top_k, len(self._table_names))

        if self._faiss_index is not None:
            scores, indices = self._faiss_index.search(query_emb, k)
            top_scores = scores[0]
            top_indices = indices[0]
        else:
            # NumPy fallback (normalized embeddings → dot product = cosine sim)
            similarities = (query_emb @ self._embeddings.T)[0]
            top_indices = np.argsort(-similarities)[:k]
            top_scores = similarities[top_indices]

        matches: list[TableMatch] = []
        for i, idx in enumerate(top_indices):
            if idx == -1:
                break  # FAISS pads with -1 when fewer results than k
            score = float(max(0.0, min(1.0, top_scores[i])))
            matches.append(TableMatch(
                table_name=self._table_names[idx],
                methods=[MatchMethod.EMBEDDING],
                relevance_score=score,
                reasoning=f"Semantic similarity: {score:.3f}",
            ))

        return matches

    @property
    def is_built(self) -> bool:
        """Whether the index has been built."""
        return self._embeddings is not None

    @property
    def table_count(self) -> int:
        """Number of tables in the index."""
        return len(self._table_names)

    @property
    def backend(self) -> str:
        """Return the active search backend name ('faiss' or 'numpy')."""
        return "faiss" if self._faiss_index is not None else "numpy"
