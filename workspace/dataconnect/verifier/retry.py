"""Fix-and-retry loop — LLM re-generation for failed verification checks.

When verify_sql() returns FAILED checks, this module sends the failures
to the user's LLM to generate a corrected SQL query, then re-verifies.
Repeats up to max_attempts times. Returns the best result across attempts.
"""

from __future__ import annotations

import logging
import re
from typing import Any

from dataconnect.exceptions import LLMError, RetryExhaustedError, VerificationError
from dataconnect.models import (
    CheckResult,
    CheckStatus,
    ScanResult,
    VerificationResult,
)
from dataconnect.tuning import TuningProfile
from dataconnect.verifier import verify_sql

logger = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────────────

_MAX_ATTEMPTS = 3
_VERIFIED_THRESHOLD = 50.0


def _has_failures(result: VerificationResult) -> bool:
    """Check if any verification check has FAILED status.

    Args:
        result: Verification result to inspect.

    Returns:
        True if at least one check has FAILED status.
    """
    return any(c.status == CheckStatus.FAILED for c in result.checks)


def _format_failures(checks: list[CheckResult]) -> str:
    """Format failed/warning checks into a concise error summary.

    Only includes FAILED and WARNING checks — PASSED/SKIPPED are
    omitted to keep the prompt focused on what needs fixing.

    Args:
        checks: List of check results from verification.

    Returns:
        Formatted string describing each failure/warning.
    """
    lines: list[str] = []
    for check in checks:
        if check.status in (CheckStatus.FAILED, CheckStatus.WARNING):
            lines.append(
                f"- [{check.status.value.upper()}] {check.check_name}: "
                f"{check.message}"
            )
    return "\n".join(lines)


def _build_schema_summary(scan_result: ScanResult) -> str:
    """Build a concise schema summary for the LLM prompt.

    Includes table names and column names/types only.
    Never includes sample data values (CODING_RULES rule 8).

    Args:
        scan_result: Schema information from the scanner.

    Returns:
        Schema summary string.
    """
    lines: list[str] = []
    for table in scan_result.tables:
        cols = ", ".join(
            f"{c.name} ({c.data_type})" for c in table.columns
        )
        lines.append(f"  {table.name}: {cols}")
    return "\n".join(lines)


def _build_fix_prompt(
    query: str,
    failed_sql: str,
    failures: str,
    schema_summary: str,
) -> str:
    """Build the LLM prompt for SQL fix generation.

    Args:
        query: Original natural-language question.
        failed_sql: SQL that failed verification.
        failures: Formatted failure descriptions.
        schema_summary: Database schema summary.

    Returns:
        Complete prompt for the LLM.
    """
    return (
        "You are a SQL expert. A generated SQL query failed verification "
        "checks. Fix the SQL to address the issues below.\n\n"
        f"Original question: {query}\n\n"
        f"Failed SQL:\n{failed_sql}\n\n"
        f"Verification issues:\n{failures}\n\n"
        f"Database schema:\n{schema_summary}\n\n"
        "Return ONLY the corrected SQL query. No explanation, no markdown "
        "code fences, no comments — just the raw SQL."
    )


def _extract_sql(response_text: str) -> str:
    """Extract SQL from LLM response, stripping any wrapping.

    Handles common LLM response patterns:
    - Raw SQL (ideal)
    - SQL wrapped in markdown code fences
    - SQL with leading/trailing whitespace

    Args:
        response_text: Raw LLM response text.

    Returns:
        Cleaned SQL string.

    Raises:
        LLMError: If response is empty or contains no SQL.
    """
    text = response_text.strip()
    if not text:
        raise LLMError("LLM returned empty response")

    # Strip markdown code fences (```sql ... ``` or ``` ... ```)
    fence_match = re.search(
        r"```(?:sql)?\s*\n?(.*?)```",
        text,
        re.DOTALL | re.IGNORECASE,
    )
    if fence_match:
        text = fence_match.group(1).strip()

    if not text:
        raise LLMError("LLM response contained no SQL after parsing")

    return text


def _call_llm_for_fix(
    prompt: str,
    model: str,
    api_key: str,
) -> str:
    """Call LLM via litellm to get corrected SQL.

    Args:
        prompt: The fix prompt with failures and schema.
        model: litellm model identifier.
        api_key: User's API key for the provider.

    Returns:
        Raw text response from the LLM.

    Raises:
        LLMError: If litellm is not installed or call fails.
    """
    try:
        import litellm
    except ImportError as exc:
        raise LLMError(
            "litellm required for fix-and-retry. "
            "Install with: pip install litellm"
        ) from exc

    try:
        response = litellm.completion(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            api_key=api_key,
            temperature=0.2,
            max_tokens=2048,
        )
        return response.choices[0].message.content  # type: ignore[union-attr]
    except Exception as exc:
        raise LLMError(f"LLM fix call failed: {exc}") from exc


def retry_with_fixes(
    sql: str,
    query: str,
    context: dict[str, Any],
    *,
    model: str,
    api_key: str,
    max_attempts: int = _MAX_ATTEMPTS,
    profile: TuningProfile | None = None,
) -> VerificationResult:
    """Run verify → fix → re-verify loop up to max_attempts times.

    First verifies the initial SQL. If any checks FAIL, sends failures
    to the LLM for a corrected query, then re-verifies. Tracks the best
    result across all attempts and returns it.

    Args:
        sql: Initial SQL query to verify.
        query: Original natural-language question (for LLM context).
        context: Verification context with 'scan_result' (required)
            and optionally 'route_result'.
        model: litellm model identifier (e.g. "gpt-4o").
        api_key: API key for the LLM provider.
        max_attempts: Maximum verification attempts (1 = no retries).

    Returns:
        Best VerificationResult across all attempts.

    Raises:
        RetryExhaustedError: If all attempts exhaust without reaching
            verified status and the caller needs to know.
        VerificationError: If context is invalid (propagated from
            verify_sql).
    """
    if max_attempts < 1:
        max_attempts = 1

    if "scan_result" not in context:
        raise VerificationError(
            "Context must contain 'scan_result' for retry loop"
        )

    scan_result: ScanResult = context["scan_result"]
    schema_summary = _build_schema_summary(scan_result)

    best_result: VerificationResult | None = None
    current_sql = sql

    for attempt in range(1, max_attempts + 1):
        logger.info("Verification attempt %d/%d", attempt, max_attempts)

        result = verify_sql(
            current_sql,
            context,
            attempt_number=attempt,
            profile=profile,
        )

        # Track best result by confidence score
        if best_result is None or (
            result.confidence_score > best_result.confidence_score
        ):
            best_result = result

        # Success — no failures
        if not _has_failures(result):
            logger.info(
                "Attempt %d: no failures (confidence: %.1f%%)",
                attempt,
                result.confidence_score,
            )
            return result

        # Last attempt — don't try to fix, just return best
        if attempt == max_attempts:
            logger.info(
                "Max attempts (%d) reached. Best confidence: %.1f%%",
                max_attempts,
                best_result.confidence_score,
            )
            break

        # Build fix prompt from failures
        failures = _format_failures(result.checks)
        prompt = _build_fix_prompt(
            query, current_sql, failures, schema_summary,
        )

        logger.info(
            "Attempt %d: %d failures, requesting LLM fix",
            attempt,
            sum(
                1
                for c in result.checks
                if c.status == CheckStatus.FAILED
            ),
        )

        # Call LLM for fix
        try:
            response_text = _call_llm_for_fix(prompt, model, api_key)
            current_sql = _extract_sql(response_text)
        except LLMError as exc:
            logger.warning(
                "LLM fix failed on attempt %d: %s", attempt, exc,
            )
            # Can't fix without LLM — return best so far
            break

    assert best_result is not None  # At least one attempt always runs
    return best_result
