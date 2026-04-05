"""Verifier base: shared protocol and utilities for all checks."""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from dataconnect.models import CheckResult, CheckStatus


@runtime_checkable
class CheckProtocol(Protocol):
    """Interface all verification checks must implement."""

    @property
    def name(self) -> str:
        """Unique name for this check."""
        ...

    def run(self, sql: str, context: dict[str, Any]) -> CheckResult:
        """Execute the check against a SQL statement.

        Args:
            sql: The SQL query to verify.
            context: Schema info, scan results, etc.

        Returns:
            CheckResult with status and details.
        """
        ...


def make_result(
    check_name: str,
    status: CheckStatus,
    message: str = "",
    **details: Any,
) -> CheckResult:
    """Helper to build a CheckResult with minimal boilerplate."""
    return CheckResult(
        check_name=check_name,
        status=status,
        message=message,
        details=details,
    )
