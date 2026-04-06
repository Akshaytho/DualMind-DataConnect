"""Verifier layer — deterministic SQL validation and confidence scoring.

Orchestrates all 6 verification checks and aggregates results
into a single VerificationResult with confidence score.
"""

from __future__ import annotations

import logging
from typing import Any

from dataconnect.exceptions import VerificationError
from dataconnect.models import CheckResult, CheckStatus, VerificationResult
from dataconnect.verifier.aggregation_validation import AggregationValidationCheck
from dataconnect.verifier.base import CheckProtocol
from dataconnect.verifier.completeness_audit import CompletenessAuditCheck
from dataconnect.verifier.filter_validation import FilterValidationCheck
from dataconnect.verifier.join_validation import JoinValidationCheck
from dataconnect.verifier.result_plausibility import ResultPlausibilityCheck
from dataconnect.verifier.schema_conformity import SchemaConformityCheck

logger = logging.getLogger(__name__)

# ── Default check ordering (critical first, advisory last) ─────────

_DEFAULT_CHECKS: list[type] = [
    SchemaConformityCheck,
    JoinValidationCheck,
    AggregationValidationCheck,
    FilterValidationCheck,
    ResultPlausibilityCheck,
    CompletenessAuditCheck,
]

# ── Confidence scoring weights ─────────────────────────────────────
# Structural checks (1-3) matter more than advisory checks (4-6).
# Weights sum to 1.0 — each check's contribution is proportional.

_CHECK_WEIGHTS: dict[str, float] = {
    "schema_conformity": 0.25,
    "join_validation": 0.20,
    "aggregation_validation": 0.20,
    "filter_validation": 0.15,
    "result_plausibility": 0.10,
    "completeness_audit": 0.10,
}

# Score per status — PASSED=full credit, WARNING=partial, FAILED/SKIPPED=0
_STATUS_SCORES: dict[CheckStatus, float] = {
    CheckStatus.PASSED: 100.0,
    CheckStatus.WARNING: 60.0,
    CheckStatus.FAILED: 0.0,
    CheckStatus.SKIPPED: 50.0,
}

# Confidence thresholds (from TASK.md)
_VERIFIED_THRESHOLD = 50.0


def compute_confidence(checks: list[CheckResult]) -> float:
    """Compute weighted confidence score from check results.

    Each check contributes its weight * status score.
    Unknown checks get equal share of any remaining weight.

    Args:
        checks: List of completed check results.

    Returns:
        Confidence score between 0.0 and 100.0.
    """
    if not checks:
        return 0.0

    total_score = 0.0
    total_weight = 0.0

    # Weight for checks not in the predefined map
    known_names = set(_CHECK_WEIGHTS.keys())
    unknown_checks = [c for c in checks if c.check_name not in known_names]
    known_weight_sum = sum(
        _CHECK_WEIGHTS[c.check_name]
        for c in checks
        if c.check_name in known_names
    )

    # Distribute remaining weight equally among unknown checks
    remaining_weight = max(0.0, 1.0 - known_weight_sum)
    unknown_weight = (
        remaining_weight / len(unknown_checks) if unknown_checks else 0.0
    )

    for check in checks:
        weight = _CHECK_WEIGHTS.get(check.check_name, unknown_weight)
        score = _STATUS_SCORES.get(check.status, 0.0)
        total_score += weight * score
        total_weight += weight

    if total_weight == 0.0:
        return 0.0

    # Normalize to 100-point scale
    return round(total_score / total_weight, 1)


def _run_single_check(
    check: CheckProtocol,
    sql: str,
    context: dict[str, Any],
) -> CheckResult:
    """Run a single check with error handling.

    If a check raises, it returns SKIPPED rather than crashing
    the entire verification pipeline.

    Args:
        check: Check instance implementing CheckProtocol.
        sql: SQL query to verify.
        context: Schema info, scan results, route results, etc.

    Returns:
        CheckResult from the check, or SKIPPED on error.
    """
    try:
        return check.run(sql, context)
    except Exception as exc:
        logger.warning(
            "Check '%s' raised %s: %s — marking SKIPPED",
            check.name,
            type(exc).__name__,
            exc,
        )
        return CheckResult(
            check_name=check.name,
            status=CheckStatus.SKIPPED,
            message=f"Check raised {type(exc).__name__}: {exc}",
        )


def verify_sql(
    sql: str,
    context: dict[str, Any],
    *,
    checks: list[CheckProtocol] | None = None,
    attempt_number: int = 1,
    fail_fast: bool = False,
) -> VerificationResult:
    """Run all verification checks and compute confidence score.

    This is the main entry point for the verifier layer.

    Args:
        sql: The SQL query to verify.
        context: Must contain 'scan_result' (ScanResult). May also
            contain 'route_result' (RouteResult) for completeness audit.
        checks: Optional list of check instances. Defaults to all 6 checks.
        attempt_number: Which retry attempt this is (1-based).
        fail_fast: If True, stop on first FAILED check.

    Returns:
        VerificationResult with all check results and confidence score.

    Raises:
        VerificationError: If sql is empty or context is missing scan_result.
    """
    if not sql or not sql.strip():
        raise VerificationError("Cannot verify empty SQL")

    if "scan_result" not in context:
        raise VerificationError(
            "Context must contain 'scan_result' for verification"
        )

    # Instantiate default checks if none provided
    if checks is None:
        checks = [cls() for cls in _DEFAULT_CHECKS]

    results: list[CheckResult] = []

    for check in checks:
        result = _run_single_check(check, sql, context)
        results.append(result)

        if fail_fast and result.status == CheckStatus.FAILED:
            logger.info(
                "Fail-fast: stopping after FAILED check '%s'",
                check.name,
            )
            break

    confidence = compute_confidence(results)

    return VerificationResult(
        sql=sql,
        checks=results,
        confidence_score=confidence,
        is_verified=confidence >= _VERIFIED_THRESHOLD,
        attempt_number=attempt_number,
    )
