"""Tuning profiles for accuracy configuration.

Exposes all tuneable parameters (verifier weights, confidence thresholds,
router settings) as a single ``TuningProfile`` model.  Profiles can be
loaded from JSON files or selected from built-in presets.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from pydantic import BaseModel, field_validator

from dataconnect.exceptions import TuningError

logger = logging.getLogger(__name__)


class TuningProfile(BaseModel):
    """All tuneable knobs in one place.

    Attributes:
        name: Human-readable profile name.
        check_weights: Per-check weight (must sum to ~1.0).
        status_scores: Score per CheckStatus name (PASSED/WARNING/FAILED/SKIPPED).
        verified_threshold: Minimum confidence to mark ``is_verified=True``.
        max_retry_attempts: Max LLM fix-and-retry loops.
        router_top_k: Max tables from embedding search.
        relationship_depth: BFS depth for graph walk.
    """

    name: str = "default"

    # ── Verifier weights (sum to 1.0) ────────────────────────────
    check_weights: dict[str, float] = {
        "schema_conformity": 0.25,
        "join_validation": 0.20,
        "aggregation_validation": 0.20,
        "filter_validation": 0.15,
        "result_plausibility": 0.10,
        "completeness_audit": 0.10,
    }

    # ── Status → score mapping ───────────────────────────────────
    status_scores: dict[str, float] = {
        "PASSED": 100.0,
        "WARNING": 60.0,
        "FAILED": 0.0,
        "SKIPPED": 50.0,
    }

    # ── Confidence thresholds ────────────────────────────────────
    verified_threshold: float = 50.0

    # ── Retry settings ───────────────────────────────────────────
    max_retry_attempts: int = 3

    # ── Router settings ──────────────────────────────────────────
    router_top_k: int = 8
    relationship_depth: int = 2

    @field_validator("check_weights")
    @classmethod
    def _weights_sum_close_to_one(
        cls, v: dict[str, float],
    ) -> dict[str, float]:
        total = sum(v.values())
        if abs(total - 1.0) > 0.05:
            raise ValueError(
                f"check_weights must sum to ~1.0, got {total:.3f}"
            )
        return v

    @field_validator("verified_threshold")
    @classmethod
    def _threshold_in_range(cls, v: float) -> float:
        if not 0.0 <= v <= 100.0:
            raise ValueError(
                f"verified_threshold must be 0-100, got {v}"
            )
        return v

    @field_validator("max_retry_attempts")
    @classmethod
    def _retries_positive(cls, v: int) -> int:
        if v < 0:
            raise ValueError(
                f"max_retry_attempts must be >= 0, got {v}"
            )
        return v

    @field_validator("router_top_k")
    @classmethod
    def _top_k_positive(cls, v: int) -> int:
        if v < 1:
            raise ValueError(f"router_top_k must be >= 1, got {v}")
        return v


# ── Built-in presets ─────────────────────────────────────────────

PRESETS: dict[str, TuningProfile] = {
    "default": TuningProfile(name="default"),
    "strict": TuningProfile(
        name="strict",
        check_weights={
            "schema_conformity": 0.30,
            "join_validation": 0.25,
            "aggregation_validation": 0.20,
            "filter_validation": 0.10,
            "result_plausibility": 0.10,
            "completeness_audit": 0.05,
        },
        status_scores={
            "PASSED": 100.0,
            "WARNING": 40.0,
            "FAILED": 0.0,
            "SKIPPED": 30.0,
        },
        verified_threshold=70.0,
        max_retry_attempts=3,
        router_top_k=6,
    ),
    "lenient": TuningProfile(
        name="lenient",
        check_weights={
            "schema_conformity": 0.20,
            "join_validation": 0.15,
            "aggregation_validation": 0.15,
            "filter_validation": 0.20,
            "result_plausibility": 0.15,
            "completeness_audit": 0.15,
        },
        status_scores={
            "PASSED": 100.0,
            "WARNING": 75.0,
            "FAILED": 0.0,
            "SKIPPED": 60.0,
        },
        verified_threshold=40.0,
        max_retry_attempts=5,
        router_top_k=10,
    ),
}


def load_profile(source: str) -> TuningProfile:
    """Load a tuning profile by preset name or JSON file path.

    Args:
        source: Either a preset name ("default", "strict", "lenient")
            or a path to a JSON file.

    Returns:
        Resolved TuningProfile.

    Raises:
        TuningError: If preset not found or file is invalid.
    """
    # Check presets first
    if source in PRESETS:
        logger.info("Using preset tuning profile: %s", source)
        return PRESETS[source].model_copy()

    # Try as file path
    path = Path(source)
    if not path.is_file():
        available = ", ".join(sorted(PRESETS.keys()))
        raise TuningError(
            f"Unknown profile '{source}'. "
            f"Available presets: {available}. "
            f"Or provide a path to a JSON file."
        )

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        raise TuningError(
            f"Cannot read tuning profile from {path}: {exc}"
        ) from exc

    if not isinstance(raw, dict):
        raise TuningError(
            f"Tuning profile must be a JSON object, got {type(raw).__name__}"
        )

    try:
        return TuningProfile(**raw)
    except Exception as exc:
        raise TuningError(
            f"Invalid tuning profile: {exc}"
        ) from exc


def get_profile(source: str | None = None) -> TuningProfile:
    """Convenience wrapper: returns default profile when source is None.

    Args:
        source: Preset name, file path, or None for default.

    Returns:
        Resolved TuningProfile.
    """
    if source is None:
        return PRESETS["default"].model_copy()
    return load_profile(source)
