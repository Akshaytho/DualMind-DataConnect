"""Tests for verifier orchestrator — verify_sql() and compute_confidence()."""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

import pytest

from dataconnect.models import (
    CheckResult,
    CheckStatus,
    ScanResult,
    VerificationResult,
)
from dataconnect.verifier import (
    _DEFAULT_CHECKS,
    _STATUS_SCORES,
    _VERIFIED_THRESHOLD,
    compute_confidence,
    verify_sql,
)
from dataconnect.verifier.base import CheckProtocol


# ── Helpers ────────────────────────────────────────────────────────


class StubCheck:
    """Stub check that returns a predetermined result."""

    def __init__(
        self, check_name: str, status: CheckStatus, message: str = ""
    ) -> None:
        self._name = check_name
        self._status = status
        self._message = message

    @property
    def name(self) -> str:
        return self._name

    def run(self, sql: str, context: dict[str, Any]) -> CheckResult:
        return CheckResult(
            check_name=self._name,
            status=self._status,
            message=self._message,
        )


class ExplodingCheck:
    """Check that always raises."""

    @property
    def name(self) -> str:
        return "exploding_check"

    def run(self, sql: str, context: dict[str, Any]) -> CheckResult:
        raise RuntimeError("Something went wrong")


def _make_results(*statuses: CheckStatus) -> list[CheckResult]:
    """Build CheckResults with default check names matching weight map."""
    names = [
        "schema_conformity",
        "join_validation",
        "aggregation_validation",
        "filter_validation",
        "result_plausibility",
        "completeness_audit",
    ]
    return [
        CheckResult(check_name=names[i], status=s)
        for i, s in enumerate(statuses)
    ]


# ── compute_confidence tests ───────────────────────────────────────


class TestComputeConfidence:
    """Tests for confidence score calculation."""

    def test_empty_checks_returns_zero(self) -> None:
        assert compute_confidence([]) == 0.0

    def test_all_passed_returns_100(self) -> None:
        results = _make_results(*([CheckStatus.PASSED] * 6))
        assert compute_confidence(results) == 100.0

    def test_all_failed_returns_zero(self) -> None:
        results = _make_results(*([CheckStatus.FAILED] * 6))
        assert compute_confidence(results) == 0.0

    def test_all_warnings_returns_60(self) -> None:
        results = _make_results(*([CheckStatus.WARNING] * 6))
        score = compute_confidence(results)
        assert score == _STATUS_SCORES[CheckStatus.WARNING]

    def test_all_skipped(self) -> None:
        results = _make_results(*([CheckStatus.SKIPPED] * 6))
        score = compute_confidence(results)
        assert score == _STATUS_SCORES[CheckStatus.SKIPPED]

    def test_mixed_statuses(self) -> None:
        """3 passed, 2 warnings, 1 failed → weighted mix."""
        results = _make_results(
            CheckStatus.PASSED,   # schema_conformity: 0.25
            CheckStatus.PASSED,   # join_validation: 0.20
            CheckStatus.PASSED,   # aggregation_validation: 0.20
            CheckStatus.WARNING,  # filter_validation: 0.15
            CheckStatus.WARNING,  # result_plausibility: 0.10
            CheckStatus.FAILED,   # completeness_audit: 0.10
        )
        score = compute_confidence(results)
        # (0.25+0.20+0.20)*100 + (0.15+0.10)*60 + 0.10*0 = 65 + 15 = 80
        assert score == 80.0

    def test_single_check(self) -> None:
        results = [CheckResult(check_name="schema_conformity", status=CheckStatus.PASSED)]
        assert compute_confidence(results) == 100.0

    def test_unknown_check_name_gets_remaining_weight(self) -> None:
        """A check with unknown name gets share of remaining weight."""
        results = [
            CheckResult(check_name="schema_conformity", status=CheckStatus.PASSED),
            CheckResult(check_name="custom_check", status=CheckStatus.FAILED),
        ]
        score = compute_confidence(results)
        # schema_conformity: weight=0.25, score=100 → 25
        # custom_check: weight=(1.0-0.25)/1=0.75, score=0 → 0
        # total = 25/1.0 = 25.0
        assert score == 25.0

    def test_structural_checks_weighted_higher(self) -> None:
        """Failing structural checks should hurt more than advisory."""
        # Fail structural (schema+join), pass advisory
        results_structural_fail = _make_results(
            CheckStatus.FAILED,
            CheckStatus.FAILED,
            CheckStatus.PASSED,
            CheckStatus.PASSED,
            CheckStatus.PASSED,
            CheckStatus.PASSED,
        )
        # Pass structural, fail advisory
        results_advisory_fail = _make_results(
            CheckStatus.PASSED,
            CheckStatus.PASSED,
            CheckStatus.PASSED,
            CheckStatus.PASSED,
            CheckStatus.FAILED,
            CheckStatus.FAILED,
        )
        score_structural_fail = compute_confidence(results_structural_fail)
        score_advisory_fail = compute_confidence(results_advisory_fail)
        # Advisory fails should score higher (less damage)
        assert score_advisory_fail > score_structural_fail


# ── verify_sql input validation tests ──────────────────────────────


class TestVerifySqlValidation:
    """Tests for verify_sql input validation."""

    def test_empty_sql_raises(self, sample_scan_result: ScanResult) -> None:
        with pytest.raises(Exception, match="empty SQL"):
            verify_sql("", {"scan_result": sample_scan_result})

    def test_whitespace_sql_raises(self, sample_scan_result: ScanResult) -> None:
        with pytest.raises(Exception, match="empty SQL"):
            verify_sql("   ", {"scan_result": sample_scan_result})

    def test_missing_scan_result_raises(self) -> None:
        with pytest.raises(Exception, match="scan_result"):
            verify_sql("SELECT 1", {})

    def test_missing_scan_result_key_raises(self) -> None:
        with pytest.raises(Exception, match="scan_result"):
            verify_sql("SELECT 1", {"other_key": "value"})


# ── verify_sql orchestration tests ─────────────────────────────────


class TestVerifySqlOrchestration:
    """Tests for verify_sql check execution and result aggregation."""

    def test_returns_verification_result(
        self, sample_scan_result: ScanResult
    ) -> None:
        checks = [StubCheck("test_check", CheckStatus.PASSED)]
        result = verify_sql(
            "SELECT 1",
            {"scan_result": sample_scan_result},
            checks=checks,
        )
        assert isinstance(result, VerificationResult)

    def test_all_checks_run(self, sample_scan_result: ScanResult) -> None:
        checks = [
            StubCheck("check_a", CheckStatus.PASSED),
            StubCheck("check_b", CheckStatus.WARNING),
            StubCheck("check_c", CheckStatus.FAILED),
        ]
        result = verify_sql(
            "SELECT 1",
            {"scan_result": sample_scan_result},
            checks=checks,
        )
        assert len(result.checks) == 3
        names = [c.check_name for c in result.checks]
        assert names == ["check_a", "check_b", "check_c"]

    def test_preserves_check_order(
        self, sample_scan_result: ScanResult
    ) -> None:
        checks = [
            StubCheck("z_check", CheckStatus.PASSED),
            StubCheck("a_check", CheckStatus.PASSED),
            StubCheck("m_check", CheckStatus.PASSED),
        ]
        result = verify_sql(
            "SELECT 1",
            {"scan_result": sample_scan_result},
            checks=checks,
        )
        names = [c.check_name for c in result.checks]
        assert names == ["z_check", "a_check", "m_check"]

    def test_attempt_number_forwarded(
        self, sample_scan_result: ScanResult
    ) -> None:
        result = verify_sql(
            "SELECT 1",
            {"scan_result": sample_scan_result},
            checks=[StubCheck("c", CheckStatus.PASSED)],
            attempt_number=3,
        )
        assert result.attempt_number == 3

    def test_sql_preserved_in_result(
        self, sample_scan_result: ScanResult
    ) -> None:
        sql = "SELECT id FROM users"
        result = verify_sql(
            sql,
            {"scan_result": sample_scan_result},
            checks=[StubCheck("c", CheckStatus.PASSED)],
        )
        assert result.sql == sql

    def test_confidence_in_result(
        self, sample_scan_result: ScanResult
    ) -> None:
        checks = [StubCheck("schema_conformity", CheckStatus.PASSED)]
        result = verify_sql(
            "SELECT 1",
            {"scan_result": sample_scan_result},
            checks=checks,
        )
        assert result.confidence_score == 100.0

    def test_is_verified_when_above_threshold(
        self, sample_scan_result: ScanResult
    ) -> None:
        checks = [StubCheck("schema_conformity", CheckStatus.PASSED)]
        result = verify_sql(
            "SELECT 1",
            {"scan_result": sample_scan_result},
            checks=checks,
        )
        assert result.is_verified is True

    def test_not_verified_when_below_threshold(
        self, sample_scan_result: ScanResult
    ) -> None:
        checks = [StubCheck("schema_conformity", CheckStatus.FAILED)]
        result = verify_sql(
            "SELECT 1",
            {"scan_result": sample_scan_result},
            checks=checks,
        )
        assert result.is_verified is False
        assert result.confidence_score < _VERIFIED_THRESHOLD


# ── fail_fast mode tests ──────────────────────────────────────────


class TestFailFast:
    """Tests for fail_fast behavior."""

    def test_fail_fast_stops_on_failure(
        self, sample_scan_result: ScanResult
    ) -> None:
        checks = [
            StubCheck("first", CheckStatus.FAILED),
            StubCheck("second", CheckStatus.PASSED),
            StubCheck("third", CheckStatus.PASSED),
        ]
        result = verify_sql(
            "SELECT 1",
            {"scan_result": sample_scan_result},
            checks=checks,
            fail_fast=True,
        )
        assert len(result.checks) == 1
        assert result.checks[0].check_name == "first"

    def test_fail_fast_continues_on_warning(
        self, sample_scan_result: ScanResult
    ) -> None:
        checks = [
            StubCheck("first", CheckStatus.WARNING),
            StubCheck("second", CheckStatus.PASSED),
        ]
        result = verify_sql(
            "SELECT 1",
            {"scan_result": sample_scan_result},
            checks=checks,
            fail_fast=True,
        )
        assert len(result.checks) == 2

    def test_fail_fast_false_runs_all(
        self, sample_scan_result: ScanResult
    ) -> None:
        checks = [
            StubCheck("first", CheckStatus.FAILED),
            StubCheck("second", CheckStatus.PASSED),
        ]
        result = verify_sql(
            "SELECT 1",
            {"scan_result": sample_scan_result},
            checks=checks,
            fail_fast=False,
        )
        assert len(result.checks) == 2


# ── Error handling tests ──────────────────────────────────────────


class TestErrorHandling:
    """Tests for check error handling (SKIPPED on exception)."""

    def test_exploding_check_becomes_skipped(
        self, sample_scan_result: ScanResult
    ) -> None:
        checks = [ExplodingCheck()]
        result = verify_sql(
            "SELECT 1",
            {"scan_result": sample_scan_result},
            checks=checks,
        )
        assert len(result.checks) == 1
        assert result.checks[0].status == CheckStatus.SKIPPED
        assert "RuntimeError" in result.checks[0].message

    def test_exploding_check_doesnt_block_others(
        self, sample_scan_result: ScanResult
    ) -> None:
        checks = [
            ExplodingCheck(),
            StubCheck("after", CheckStatus.PASSED),
        ]
        result = verify_sql(
            "SELECT 1",
            {"scan_result": sample_scan_result},
            checks=checks,
        )
        assert len(result.checks) == 2
        assert result.checks[0].status == CheckStatus.SKIPPED
        assert result.checks[1].status == CheckStatus.PASSED

    def test_exploding_check_in_fail_fast_continues(
        self, sample_scan_result: ScanResult
    ) -> None:
        """SKIPPED is not FAILED — fail_fast should not stop."""
        checks = [
            ExplodingCheck(),
            StubCheck("after", CheckStatus.PASSED),
        ]
        result = verify_sql(
            "SELECT 1",
            {"scan_result": sample_scan_result},
            checks=checks,
            fail_fast=True,
        )
        assert len(result.checks) == 2


# ── Default checks tests ──────────────────────────────────────────


class TestDefaultChecks:
    """Tests for default check registration."""

    def test_six_default_checks(self) -> None:
        assert len(_DEFAULT_CHECKS) == 6

    def test_default_checks_implement_protocol(self) -> None:
        for cls in _DEFAULT_CHECKS:
            instance = cls()
            assert isinstance(instance, CheckProtocol)

    def test_default_check_names_unique(self) -> None:
        names = [cls().name for cls in _DEFAULT_CHECKS]
        assert len(names) == len(set(names))

    def test_all_default_names_have_weights(self) -> None:
        from dataconnect.verifier import _CHECK_WEIGHTS
        for cls in _DEFAULT_CHECKS:
            assert cls().name in _CHECK_WEIGHTS


# ── Integration test with real checks ─────────────────────────────


class TestIntegration:
    """Run verify_sql with real checks against sample data."""

    def test_valid_query_passes(self, sample_scan_result: ScanResult) -> None:
        result = verify_sql(
            "SELECT id, name FROM users",
            {"scan_result": sample_scan_result},
        )
        assert isinstance(result, VerificationResult)
        assert result.confidence_score > 0.0
        assert len(result.checks) == 6

    def test_invalid_table_fails(
        self, sample_scan_result: ScanResult
    ) -> None:
        result = verify_sql(
            "SELECT id FROM nonexistent_table",
            {"scan_result": sample_scan_result},
        )
        # Schema conformity should fail
        schema_check = next(
            c for c in result.checks if c.check_name == "schema_conformity"
        )
        assert schema_check.status == CheckStatus.FAILED

    def test_join_query_all_checks_run(
        self, sample_scan_result: ScanResult
    ) -> None:
        result = verify_sql(
            "SELECT u.name, o.amount FROM users u "
            "JOIN orders o ON u.id = o.user_id",
            {"scan_result": sample_scan_result},
        )
        assert len(result.checks) == 6
        assert result.confidence_score >= 0.0

    def test_verified_flag_matches_threshold(
        self, sample_scan_result: ScanResult
    ) -> None:
        result = verify_sql(
            "SELECT id FROM users",
            {"scan_result": sample_scan_result},
        )
        expected = result.confidence_score >= _VERIFIED_THRESHOLD
        assert result.is_verified == expected
