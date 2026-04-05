"""Tests for verifier base module — protocol and helpers."""

from __future__ import annotations

from typing import Any

from dataconnect.models import CheckResult, CheckStatus
from dataconnect.verifier.base import CheckProtocol, make_result


class FakeCheck:
    """A check that satisfies CheckProtocol for testing."""

    @property
    def name(self) -> str:
        return "fake_check"

    def run(self, sql: str, context: dict[str, Any]) -> CheckResult:
        return make_result(self.name, CheckStatus.PASSED, "all good")


class TestCheckProtocol:
    """Tests for CheckProtocol compliance."""

    def test_fake_check_is_protocol(self) -> None:
        check = FakeCheck()
        assert isinstance(check, CheckProtocol)

    def test_fake_check_runs(self) -> None:
        check = FakeCheck()
        result = check.run("SELECT 1", {})
        assert result.status == CheckStatus.PASSED
        assert result.check_name == "fake_check"


class TestMakeResult:
    """Tests for make_result helper."""

    def test_basic(self) -> None:
        r = make_result("test", CheckStatus.FAILED, "bad sql")
        assert r.check_name == "test"
        assert r.status == CheckStatus.FAILED

    def test_with_details(self) -> None:
        r = make_result("test", CheckStatus.WARNING, "hmm", column="id", table="users")
        assert r.details["column"] == "id"
        assert r.details["table"] == "users"
