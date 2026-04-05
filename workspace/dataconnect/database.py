"""Read-only database engine factory.

Connection details never cross module boundaries — only Engine objects.
All queries MUST go through SQLAlchemy parameterized execution.
"""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import Engine

from dataconnect.config import sanitize_connection_string
from dataconnect.exceptions import DatabaseConnectionError, ReadOnlyViolationError

logger = logging.getLogger(__name__)

# SQL statements that are NOT allowed
_WRITE_KEYWORDS = frozenset({
    "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE",
    "TRUNCATE", "REPLACE", "MERGE", "GRANT", "REVOKE",
})


def _block_writes(conn: Any, cursor: Any, statement: str, *_args: Any) -> None:
    """SQLAlchemy event listener that blocks non-SELECT statements."""
    first_word = statement.strip().split()[0].upper() if statement.strip() else ""
    if first_word in _WRITE_KEYWORDS:
        raise ReadOnlyViolationError(
            f"Write operation blocked: {first_word} not allowed"
        )


def create_readonly_engine(connection_string: str, **kwargs: Any) -> Engine:
    """Create a read-only SQLAlchemy engine.

    Args:
        connection_string: Database URL (password will be masked in logs).
        **kwargs: Additional engine arguments.

    Returns:
        SQLAlchemy Engine with write-blocking listener.

    Raises:
        DatabaseConnectionError: If connection cannot be established.
    """
    safe_url = sanitize_connection_string(connection_string)
    logger.info("Connecting to database: %s", safe_url)

    try:
        engine = create_engine(
            connection_string,
            pool_pre_ping=True,
            **kwargs,
        )
        # Block all write operations
        event.listen(engine, "before_cursor_execute", _block_writes)

        # Verify connectivity
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))

        logger.info("Database connection verified: %s", safe_url)
        return engine

    except ReadOnlyViolationError:
        raise
    except Exception as exc:
        raise DatabaseConnectionError(
            f"Failed to connect to {safe_url}: {exc}"
        ) from exc
