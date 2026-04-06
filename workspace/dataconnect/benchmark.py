"""Benchmark harness for measuring DataConnect accuracy.

Loads BIRD-format question-SQL pairs, runs them through the full
pipeline, and compares generated SQL against golden answers via
execution accuracy (run both, compare result sets).
"""

from __future__ import annotations

import logging
import re
import time
from enum import Enum
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

from dataconnect.exceptions import BenchmarkError, DataConnectError

logger = logging.getLogger(__name__)


# ── Models ────────────────────────────────────────────────────────


class Difficulty(str, Enum):
    """BIRD benchmark difficulty levels."""

    SIMPLE = "simple"
    MODERATE = "moderate"
    CHALLENGING = "challenging"


class BenchmarkCase(BaseModel):
    """Single benchmark test case (BIRD format)."""

    question: str
    golden_sql: str
    db_id: str
    difficulty: Difficulty = Difficulty.SIMPLE


class CaseResult(BaseModel):
    """Result from running a single benchmark case."""

    case: BenchmarkCase
    generated_sql: str = ""
    confidence_score: float = 0.0
    confidence_label: str = "UNVERIFIED"
    is_verified: bool = False
    execution_match: bool | None = None
    error: str | None = None
    elapsed_ms: float = 0.0


class DifficultyStats(BaseModel):
    """Accuracy stats for a single difficulty level."""

    total: int = 0
    correct: int = 0
    errored: int = 0
    accuracy: float = 0.0


class BenchmarkReport(BaseModel):
    """Aggregate benchmark results."""

    total_cases: int = 0
    correct: int = 0
    incorrect: int = 0
    errored: int = 0
    execution_accuracy: float = 0.0
    avg_confidence: float = 0.0
    avg_elapsed_ms: float = 0.0
    by_difficulty: dict[str, DifficultyStats] = Field(
        default_factory=dict,
    )
    high_confidence_correct: int = 0
    high_confidence_total: int = 0
    calibration_accuracy: float = 0.0
    results: list[CaseResult] = Field(default_factory=list)


# ── Loading ───────────────────────────────────────────────────────


def load_cases(path: Path) -> list[BenchmarkCase]:
    """Load benchmark cases from a JSON file.

    Expected format: JSON array of objects with keys:
      question (str), SQL (str), db_id (str),
      difficulty (str, optional: simple/moderate/challenging).

    Args:
        path: Path to JSON file.

    Returns:
        List of parsed benchmark cases.

    Raises:
        BenchmarkError: If file cannot be loaded or parsed.
    """
    import json

    if not path.exists():
        raise BenchmarkError(f"Benchmark file not found: {path}")

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        raise BenchmarkError(
            f"Failed to load benchmark file: {exc}"
        ) from exc

    if not isinstance(raw, list):
        raise BenchmarkError(
            "Benchmark file must contain a JSON array",
        )

    cases: list[BenchmarkCase] = []
    for i, item in enumerate(raw):
        if not isinstance(item, dict):
            raise BenchmarkError(f"Case {i} is not an object")

        try:
            case = BenchmarkCase(
                question=item["question"],
                golden_sql=item["SQL"],
                db_id=item["db_id"],
                difficulty=item.get("difficulty", "simple"),
            )
            cases.append(case)
        except (KeyError, ValueError) as exc:
            raise BenchmarkError(
                f"Case {i} missing or invalid fields: {exc}"
            ) from exc

    return cases


# ── Comparison ────────────────────────────────────────────────────


def normalize_sql(sql: str) -> str:
    """Normalize SQL for string comparison (fallback only).

    Lowercases, strips whitespace/semicolons, collapses spaces.
    NOT reliable — use execution comparison when possible.

    Args:
        sql: Raw SQL string.

    Returns:
        Normalized SQL string.
    """
    normalized = sql.lower().strip().rstrip(";").strip()
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized


def compare_execution(
    generated_sql: str,
    golden_sql: str,
    engine: Any,
) -> bool:
    """Compare two SQL queries by executing both and checking results.

    Both queries run as read-only SELECTs. Results are compared
    as sorted string representations for order-independent matching.

    Args:
        generated_sql: Generated SQL to test.
        golden_sql: Known-correct SQL.
        engine: SQLAlchemy engine connected to the target database.

    Returns:
        True if both queries return the same result set.

    Raises:
        BenchmarkError: If execution fails for either query.
    """
    from sqlalchemy import text

    try:
        with engine.connect() as conn:
            gen_rows = conn.execute(text(generated_sql)).fetchall()
            gold_rows = conn.execute(text(golden_sql)).fetchall()
    except Exception as exc:
        raise BenchmarkError(
            f"SQL execution failed: {exc}"
        ) from exc

    return sorted(str(r) for r in gen_rows) == sorted(
        str(r) for r in gold_rows
    )


# ── Pipeline ──────────────────────────────────────────────────────


def _confidence_label(score: float) -> str:
    """Map confidence score to human-readable label.

    Args:
        score: Confidence score 0-100.

    Returns:
        Label string.
    """
    from dataconnect.config import (
        CONFIDENCE_HIGH,
        CONFIDENCE_LOW,
        CONFIDENCE_MEDIUM,
    )

    if score >= CONFIDENCE_HIGH:
        return "HIGH"
    if score >= CONFIDENCE_MEDIUM:
        return "MEDIUM"
    if score >= CONFIDENCE_LOW:
        return "LOW"
    return "UNVERIFIED"


def run_case(
    case: BenchmarkCase,
    scan_result: Any,
    model: str,
    api_key: str,
    *,
    engine: Any | None = None,
) -> CaseResult:
    """Run a single benchmark case through the full pipeline.

    Pipeline: route -> generate -> verify.
    If engine is provided, compares execution results.

    Args:
        case: The benchmark case to run.
        scan_result: ScanResult for the case's database.
        model: LLM model ID.
        api_key: LLM API key.
        engine: Optional engine for execution comparison.

    Returns:
        CaseResult with generated SQL and match status.
    """
    from dataconnect.generator import generate_sql
    from dataconnect.router import route_query
    from dataconnect.verifier import verify_sql

    start = time.monotonic()

    try:
        route_result = route_query(
            case.question,
            scan_result,
            llm_model=model,
            llm_api_key=api_key,
        )

        sql = generate_sql(
            case.question,
            scan_result,
            route_result,
            model=model,
            api_key=api_key,
        )

        context = {
            "scan_result": scan_result,
            "route_result": route_result,
        }
        verification = verify_sql(sql, context)

        elapsed_ms = (time.monotonic() - start) * 1000

        execution_match = None
        if engine is not None:
            try:
                execution_match = compare_execution(
                    sql, case.golden_sql, engine,
                )
            except BenchmarkError as exc:
                logger.warning(
                    "Execution comparison failed for '%s': %s",
                    case.question[:50],
                    exc,
                )

        return CaseResult(
            case=case,
            generated_sql=sql,
            confidence_score=verification.confidence_score,
            confidence_label=_confidence_label(
                verification.confidence_score,
            ),
            is_verified=verification.is_verified,
            execution_match=execution_match,
            elapsed_ms=round(elapsed_ms, 1),
        )

    except DataConnectError as exc:
        elapsed_ms = (time.monotonic() - start) * 1000
        return CaseResult(
            case=case,
            error=str(exc),
            elapsed_ms=round(elapsed_ms, 1),
        )


# ── Reporting ─────────────────────────────────────────────────────


def compute_report(results: list[CaseResult]) -> BenchmarkReport:
    """Compute aggregate metrics from individual case results.

    Args:
        results: List of individual case results.

    Returns:
        BenchmarkReport with aggregate statistics.
    """
    from dataconnect.config import CONFIDENCE_HIGH

    total = len(results)
    if total == 0:
        return BenchmarkReport()

    correct = sum(1 for r in results if r.execution_match is True)
    errored = sum(1 for r in results if r.error is not None)
    incorrect = total - correct - errored

    high_conf = [
        r for r in results if r.confidence_score >= CONFIDENCE_HIGH
    ]
    high_conf_correct = sum(
        1 for r in high_conf if r.execution_match is True
    )

    by_difficulty: dict[str, DifficultyStats] = {}
    for r in results:
        d = r.case.difficulty.value
        if d not in by_difficulty:
            by_difficulty[d] = DifficultyStats()
        stats = by_difficulty[d]
        stats.total += 1
        if r.execution_match is True:
            stats.correct += 1
        if r.error is not None:
            stats.errored += 1

    for stats in by_difficulty.values():
        if stats.total > 0:
            stats.accuracy = round(
                stats.correct / stats.total * 100, 1,
            )

    scored = [r for r in results if r.error is None]
    avg_confidence = (
        sum(r.confidence_score for r in scored) / len(scored)
        if scored
        else 0.0
    )
    avg_elapsed = sum(r.elapsed_ms for r in results) / total

    return BenchmarkReport(
        total_cases=total,
        correct=correct,
        incorrect=incorrect,
        errored=errored,
        execution_accuracy=(
            round(correct / total * 100, 1) if total else 0.0
        ),
        avg_confidence=round(avg_confidence, 1),
        avg_elapsed_ms=round(avg_elapsed, 1),
        by_difficulty=by_difficulty,
        high_confidence_correct=high_conf_correct,
        high_confidence_total=len(high_conf),
        calibration_accuracy=(
            round(high_conf_correct / len(high_conf) * 100, 1)
            if high_conf
            else 0.0
        ),
        results=results,
    )


# ── Orchestrator ──────────────────────────────────────────────────


def run_benchmark(
    cases: list[BenchmarkCase],
    scan_results: dict[str, Any],
    model: str,
    api_key: str,
    *,
    engines: dict[str, Any] | None = None,
) -> BenchmarkReport:
    """Run all benchmark cases and compute aggregate report.

    Args:
        cases: List of benchmark cases to run.
        scan_results: Map of db_id to ScanResult.
        model: LLM model ID.
        api_key: LLM API key.
        engines: Optional map of db_id to SQLAlchemy engine.

    Returns:
        BenchmarkReport with all results and metrics.

    Raises:
        BenchmarkError: If a required scan result is missing.
    """
    results: list[CaseResult] = []

    for i, case in enumerate(cases):
        scan_result = scan_results.get(case.db_id)
        if scan_result is None:
            raise BenchmarkError(
                f"No scan result for database '{case.db_id}' "
                f"(case {i}: '{case.question[:50]}')"
            )

        engine = engines.get(case.db_id) if engines else None

        logger.info(
            "Running case %d/%d: %s",
            i + 1,
            len(cases),
            case.question[:60],
        )

        result = run_case(
            case, scan_result, model, api_key, engine=engine,
        )
        results.append(result)

    return compute_report(results)
