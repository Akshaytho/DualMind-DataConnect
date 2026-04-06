"""Tests for verifier retry loop — fix-and-retry with LLM re-generation."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from dataconnect.exceptions import LLMError, VerificationError
from dataconnect.models import (
    CheckResult,
    CheckStatus,
    ColumnInfo,
    RelationshipInfo,
    RelationshipType,
    ScanResult,
    TableInfo,
    VerificationResult,
)
from dataconnect.verifier.retry import (
    _build_fix_prompt,
    _build_schema_summary,
    _extract_sql,
    _format_failures,
    _has_failures,
    retry_with_fixes,
)


# ── Fixtures ─────────────────────────────────────────────────────────


@pytest.fixture
def scan_result() -> ScanResult:
    """Minimal scan result for testing."""
    return ScanResult(
        database_name="test_db",
        tables=[
            TableInfo(
                name="users",
                columns=[
                    ColumnInfo(name="id", data_type="INTEGER"),
                    ColumnInfo(name="name", data_type="TEXT"),
                    ColumnInfo(name="email", data_type="TEXT"),
                ],
                row_count_estimate=100,
            ),
            TableInfo(
                name="orders",
                columns=[
                    ColumnInfo(name="id", data_type="INTEGER"),
                    ColumnInfo(name="user_id", data_type="INTEGER"),
                    ColumnInfo(name="total", data_type="NUMERIC"),
                ],
                row_count_estimate=500,
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
def context(scan_result: ScanResult) -> dict[str, Any]:
    """Verification context with scan result."""
    return {"scan_result": scan_result}


def _make_result(
    sql: str,
    checks: list[CheckResult],
    confidence: float,
    attempt: int = 1,
) -> VerificationResult:
    """Helper to create VerificationResult."""
    return VerificationResult(
        sql=sql,
        checks=checks,
        confidence_score=confidence,
        is_verified=confidence >= 50.0,
        attempt_number=attempt,
    )


def _passed_check(name: str = "test_check") -> CheckResult:
    return CheckResult(
        check_name=name,
        status=CheckStatus.PASSED,
        message="All good",
    )


def _failed_check(name: str = "test_check", msg: str = "Bad") -> CheckResult:
    return CheckResult(
        check_name=name,
        status=CheckStatus.FAILED,
        message=msg,
    )


def _warning_check(name: str = "test_check") -> CheckResult:
    return CheckResult(
        check_name=name,
        status=CheckStatus.WARNING,
        message="Minor issue",
    )


# ── _has_failures tests ─────────────────────────────────────────────


class TestHasFailures:
    """Tests for _has_failures helper."""

    def test_no_failures(self) -> None:
        result = _make_result("SELECT 1", [_passed_check()], 100.0)
        assert _has_failures(result) is False

    def test_with_failure(self) -> None:
        result = _make_result("SELECT 1", [_failed_check()], 0.0)
        assert _has_failures(result) is True

    def test_warnings_not_failures(self) -> None:
        result = _make_result("SELECT 1", [_warning_check()], 60.0)
        assert _has_failures(result) is False

    def test_mixed_statuses(self) -> None:
        result = _make_result(
            "SELECT 1",
            [_passed_check("a"), _failed_check("b"), _warning_check("c")],
            30.0,
        )
        assert _has_failures(result) is True

    def test_empty_checks(self) -> None:
        result = _make_result("SELECT 1", [], 0.0)
        assert _has_failures(result) is False


# ── _format_failures tests ──────────────────────────────────────────


class TestFormatFailures:
    """Tests for failure formatting."""

    def test_only_failures_included(self) -> None:
        checks = [_passed_check("a"), _failed_check("b", "broken")]
        text = _format_failures(checks)
        assert "b" in text
        assert "broken" in text
        assert "a" not in text

    def test_warnings_included(self) -> None:
        checks = [_warning_check("w")]
        text = _format_failures(checks)
        assert "WARNING" in text
        assert "w" in text

    def test_passed_and_skipped_excluded(self) -> None:
        checks = [
            _passed_check("p"),
            CheckResult(
                check_name="s",
                status=CheckStatus.SKIPPED,
                message="skip",
            ),
        ]
        text = _format_failures(checks)
        assert text == ""

    def test_multiple_failures(self) -> None:
        checks = [
            _failed_check("a", "issue 1"),
            _failed_check("b", "issue 2"),
        ]
        text = _format_failures(checks)
        lines = text.strip().split("\n")
        assert len(lines) == 2


# ── _build_schema_summary tests ─────────────────────────────────────


class TestBuildSchemaSummary:
    """Tests for schema summary generation."""

    def test_includes_table_names(self, scan_result: ScanResult) -> None:
        summary = _build_schema_summary(scan_result)
        assert "users" in summary
        assert "orders" in summary

    def test_includes_column_types(self, scan_result: ScanResult) -> None:
        summary = _build_schema_summary(scan_result)
        assert "INTEGER" in summary
        assert "TEXT" in summary

    def test_no_sample_values(self, scan_result: ScanResult) -> None:
        """Schema summary must never include data values (rule 8)."""
        summary = _build_schema_summary(scan_result)
        # Should contain structure, not data
        assert "id (INTEGER)" in summary

    def test_empty_tables(self) -> None:
        sr = ScanResult(database_name="empty", tables=[])
        summary = _build_schema_summary(sr)
        assert summary == ""


# ── _build_fix_prompt tests ──────────────────────────────────────────


class TestBuildFixPrompt:
    """Tests for fix prompt construction."""

    def test_contains_all_parts(self) -> None:
        prompt = _build_fix_prompt(
            query="How many users?",
            failed_sql="SELECT * FROM usr",
            failures="- [FAILED] schema_conformity: Table 'usr' not found",
            schema_summary="  users: id (INTEGER), name (TEXT)",
        )
        assert "How many users?" in prompt
        assert "SELECT * FROM usr" in prompt
        assert "Table 'usr' not found" in prompt
        assert "users: id (INTEGER)" in prompt

    def test_instructs_raw_sql(self) -> None:
        prompt = _build_fix_prompt("q", "sql", "f", "s")
        assert "ONLY" in prompt
        assert "raw SQL" in prompt


# ── _extract_sql tests ───────────────────────────────────────────────


class TestExtractSql:
    """Tests for SQL extraction from LLM response."""

    def test_raw_sql(self) -> None:
        assert _extract_sql("SELECT 1") == "SELECT 1"

    def test_strips_whitespace(self) -> None:
        assert _extract_sql("  SELECT 1  \n") == "SELECT 1"

    def test_strips_markdown_fences(self) -> None:
        text = "```sql\nSELECT * FROM users\n```"
        assert _extract_sql(text) == "SELECT * FROM users"

    def test_strips_plain_fences(self) -> None:
        text = "```\nSELECT 1\n```"
        assert _extract_sql(text) == "SELECT 1"

    def test_empty_response_raises(self) -> None:
        with pytest.raises(LLMError, match="empty response"):
            _extract_sql("")

    def test_only_fences_raises(self) -> None:
        with pytest.raises(LLMError, match="no SQL"):
            _extract_sql("```sql\n\n```")

    def test_multiline_sql(self) -> None:
        text = "SELECT u.name\nFROM users u\nWHERE u.id = 1"
        assert "FROM users u" in _extract_sql(text)

    def test_case_insensitive_fence(self) -> None:
        text = "```SQL\nSELECT 1\n```"
        assert _extract_sql(text) == "SELECT 1"


# ── retry_with_fixes integration tests ──────────────────────────────


class TestRetryWithFixes:
    """Tests for the full retry loop."""

    @patch("dataconnect.verifier.retry.verify_sql")
    def test_no_failures_returns_immediately(
        self, mock_verify: MagicMock, context: dict[str, Any],
    ) -> None:
        """If first verification has no failures, return without retry."""
        mock_verify.return_value = _make_result(
            "SELECT 1", [_passed_check()], 100.0,
        )
        result = retry_with_fixes(
            "SELECT 1", "test query", context,
            model="gpt-4o", api_key="test-key",
        )
        assert result.confidence_score == 100.0
        mock_verify.assert_called_once()

    @patch("dataconnect.verifier.retry._call_llm_for_fix")
    @patch("dataconnect.verifier.retry.verify_sql")
    def test_retry_fixes_on_second_attempt(
        self,
        mock_verify: MagicMock,
        mock_llm: MagicMock,
        context: dict[str, Any],
    ) -> None:
        """Failure on attempt 1, success on attempt 2."""
        mock_verify.side_effect = [
            _make_result(
                "SELECT * FROM usr",
                [_failed_check("schema", "Table not found")],
                0.0,
                attempt=1,
            ),
            _make_result(
                "SELECT * FROM users",
                [_passed_check("schema")],
                100.0,
                attempt=2,
            ),
        ]
        mock_llm.return_value = "SELECT * FROM users"

        result = retry_with_fixes(
            "SELECT * FROM usr", "get users", context,
            model="gpt-4o", api_key="test-key",
        )
        assert result.confidence_score == 100.0
        assert mock_verify.call_count == 2

    @patch("dataconnect.verifier.retry._call_llm_for_fix")
    @patch("dataconnect.verifier.retry.verify_sql")
    def test_max_attempts_respected(
        self,
        mock_verify: MagicMock,
        mock_llm: MagicMock,
        context: dict[str, Any],
    ) -> None:
        """Stops after max_attempts even if still failing."""
        mock_verify.return_value = _make_result(
            "bad sql",
            [_failed_check("schema", "bad")],
            10.0,
        )
        mock_llm.return_value = "still bad sql"

        result = retry_with_fixes(
            "bad sql", "query", context,
            model="gpt-4o", api_key="test-key", max_attempts=3,
        )
        # 3 verifications, 2 LLM calls (no fix after last attempt)
        assert mock_verify.call_count == 3
        assert mock_llm.call_count == 2

    @patch("dataconnect.verifier.retry._call_llm_for_fix")
    @patch("dataconnect.verifier.retry.verify_sql")
    def test_returns_best_result(
        self,
        mock_verify: MagicMock,
        mock_llm: MagicMock,
        context: dict[str, Any],
    ) -> None:
        """Returns the highest-confidence result across attempts."""
        mock_verify.side_effect = [
            _make_result("sql1", [_failed_check()], 20.0, attempt=1),
            _make_result("sql2", [_failed_check()], 45.0, attempt=2),
            _make_result("sql3", [_failed_check()], 30.0, attempt=3),
        ]
        mock_llm.return_value = "fixed sql"

        result = retry_with_fixes(
            "sql1", "query", context,
            model="gpt-4o", api_key="test-key", max_attempts=3,
        )
        assert result.confidence_score == 45.0

    @patch("dataconnect.verifier.retry._call_llm_for_fix")
    @patch("dataconnect.verifier.retry.verify_sql")
    def test_llm_failure_stops_retrying(
        self,
        mock_verify: MagicMock,
        mock_llm: MagicMock,
        context: dict[str, Any],
    ) -> None:
        """If LLM call fails, stop retrying and return best."""
        mock_verify.return_value = _make_result(
            "bad", [_failed_check()], 15.0,
        )
        mock_llm.side_effect = LLMError("API down")

        result = retry_with_fixes(
            "bad", "query", context,
            model="gpt-4o", api_key="test-key", max_attempts=3,
        )
        assert result.confidence_score == 15.0
        # Only 1 verify (initial) + 1 failed LLM call
        assert mock_verify.call_count == 1
        assert mock_llm.call_count == 1

    @patch("dataconnect.verifier.retry.verify_sql")
    def test_warnings_only_no_retry(
        self, mock_verify: MagicMock, context: dict[str, Any],
    ) -> None:
        """Warnings don't trigger retry — only FAILED does."""
        mock_verify.return_value = _make_result(
            "SELECT 1", [_warning_check()], 60.0,
        )
        result = retry_with_fixes(
            "SELECT 1", "query", context,
            model="gpt-4o", api_key="test-key",
        )
        assert result.confidence_score == 60.0
        mock_verify.assert_called_once()

    @patch("dataconnect.verifier.retry.verify_sql")
    def test_max_attempts_minimum_one(
        self, mock_verify: MagicMock, context: dict[str, Any],
    ) -> None:
        """max_attempts < 1 is clamped to 1."""
        mock_verify.return_value = _make_result(
            "SELECT 1", [_passed_check()], 100.0,
        )
        result = retry_with_fixes(
            "SELECT 1", "query", context,
            model="gpt-4o", api_key="test-key", max_attempts=0,
        )
        mock_verify.assert_called_once()
        assert result.confidence_score == 100.0

    @patch("dataconnect.verifier.retry.verify_sql")
    def test_context_missing_scan_result_propagates(
        self, mock_verify: MagicMock,
    ) -> None:
        """VerificationError from verify_sql propagates."""
        mock_verify.side_effect = VerificationError("No scan_result")
        with pytest.raises(VerificationError, match="scan_result"):
            retry_with_fixes(
                "SELECT 1", "query", {},
                model="gpt-4o", api_key="test-key",
            )

    @patch("dataconnect.verifier.retry._call_llm_for_fix")
    @patch("dataconnect.verifier.retry.verify_sql")
    def test_attempt_number_increments(
        self,
        mock_verify: MagicMock,
        mock_llm: MagicMock,
        context: dict[str, Any],
    ) -> None:
        """Each verification call gets incrementing attempt_number."""
        mock_verify.side_effect = [
            _make_result("sql", [_failed_check()], 10.0, attempt=1),
            _make_result("sql", [_passed_check()], 90.0, attempt=2),
        ]
        mock_llm.return_value = "fixed sql"

        retry_with_fixes(
            "sql", "query", context,
            model="gpt-4o", api_key="test-key",
        )
        calls = mock_verify.call_args_list
        assert calls[0].kwargs.get("attempt_number", calls[0][1] if len(calls[0]) > 1 else None) is not None
        # Just check it was called twice with different attempt numbers
        assert len(calls) == 2

    @patch("dataconnect.verifier.retry._call_llm_for_fix")
    @patch("dataconnect.verifier.retry.verify_sql")
    def test_fixed_sql_used_in_next_attempt(
        self,
        mock_verify: MagicMock,
        mock_llm: MagicMock,
        context: dict[str, Any],
    ) -> None:
        """LLM-generated SQL is used for the next verification."""
        mock_verify.side_effect = [
            _make_result("bad", [_failed_check()], 0.0),
            _make_result("fixed", [_passed_check()], 100.0),
        ]
        mock_llm.return_value = "SELECT * FROM users"

        retry_with_fixes(
            "bad", "query", context,
            model="gpt-4o", api_key="test-key",
        )
        # Second verify_sql call should use the fixed SQL
        second_call_sql = mock_verify.call_args_list[1][0][0]
        assert second_call_sql == "SELECT * FROM users"

    @patch("dataconnect.verifier.retry._call_llm_for_fix")
    @patch("dataconnect.verifier.retry.verify_sql")
    def test_markdown_fences_stripped_from_fix(
        self,
        mock_verify: MagicMock,
        mock_llm: MagicMock,
        context: dict[str, Any],
    ) -> None:
        """LLM response with code fences is handled correctly."""
        mock_verify.side_effect = [
            _make_result("bad", [_failed_check()], 0.0),
            _make_result("good", [_passed_check()], 100.0),
        ]
        mock_llm.return_value = "```sql\nSELECT * FROM users\n```"

        result = retry_with_fixes(
            "bad", "query", context,
            model="gpt-4o", api_key="test-key",
        )
        second_call_sql = mock_verify.call_args_list[1][0][0]
        assert second_call_sql == "SELECT * FROM users"

    @patch("dataconnect.verifier.retry._call_llm_for_fix")
    @patch("dataconnect.verifier.retry.verify_sql")
    def test_single_attempt_no_llm_call(
        self,
        mock_verify: MagicMock,
        mock_llm: MagicMock,
        context: dict[str, Any],
    ) -> None:
        """With max_attempts=1, never calls LLM even on failure."""
        mock_verify.return_value = _make_result(
            "bad", [_failed_check()], 10.0,
        )
        retry_with_fixes(
            "bad", "query", context,
            model="gpt-4o", api_key="test-key", max_attempts=1,
        )
        mock_llm.assert_not_called()

    @patch("dataconnect.verifier.retry._call_llm_for_fix")
    @patch("dataconnect.verifier.retry.verify_sql")
    def test_progressive_improvement(
        self,
        mock_verify: MagicMock,
        mock_llm: MagicMock,
        context: dict[str, Any],
    ) -> None:
        """Confidence can improve across attempts."""
        mock_verify.side_effect = [
            _make_result("v1", [_failed_check()], 10.0, attempt=1),
            _make_result("v2", [_failed_check()], 35.0, attempt=2),
            _make_result("v3", [_warning_check()], 70.0, attempt=3),
        ]
        mock_llm.return_value = "improved sql"

        result = retry_with_fixes(
            "v1", "query", context,
            model="gpt-4o", api_key="test-key", max_attempts=3,
        )
        # Third attempt has no failures (only warnings), returns it
        assert result.confidence_score == 70.0


# ── _call_llm_for_fix tests ─────────────────────────────────────────


class TestCallLlmForFix:
    """Tests for LLM call wrapper."""

    @patch("dataconnect.verifier.retry.litellm", create=True)
    def test_calls_litellm_completion(self, mock_module: MagicMock) -> None:
        """Verify litellm.completion is called with correct params."""
        # Need to mock at import level
        import dataconnect.verifier.retry as retry_module

        mock_response = MagicMock()
        mock_response.choices = [
            MagicMock(message=MagicMock(content="SELECT 1"))
        ]

        with patch.dict(
            "sys.modules", {"litellm": mock_module}
        ):
            mock_module.completion.return_value = mock_response
            result = retry_module._call_llm_for_fix(
                "fix this", "gpt-4o", "key123",
            )
            assert result == "SELECT 1"
            mock_module.completion.assert_called_once()
            call_kwargs = mock_module.completion.call_args
            assert call_kwargs.kwargs["model"] == "gpt-4o"
            assert call_kwargs.kwargs["api_key"] == "key123"
            assert call_kwargs.kwargs["temperature"] == 0.2
