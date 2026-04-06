"""API authentication and rate limiting.

X-API-Key header validation and per-key rate limiting.
Server API key is set via DATACONNECT_SERVER_API_KEY env var.
"""

from __future__ import annotations

import logging
import os
import time
from collections import defaultdict
from threading import Lock

from fastapi import Depends, HTTPException, Security
from fastapi.security import APIKeyHeader

from dataconnect.config import RATE_LIMIT_PER_MINUTE
from dataconnect.exceptions import AuthenticationError, RateLimitError

logger = logging.getLogger(__name__)

_API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)

# In-memory rate limiter: key -> list of timestamps
_rate_buckets: dict[str, list[float]] = defaultdict(list)
_rate_lock = Lock()


def _get_server_api_key() -> str | None:
    """Read server API key from environment.

    Returns:
        The configured key, or None if not set.
    """
    return os.environ.get("DATACONNECT_SERVER_API_KEY")


def validate_api_key(
    api_key: str | None = Security(_API_KEY_HEADER),
) -> str:
    """FastAPI dependency: validate X-API-Key header.

    Args:
        api_key: Value from X-API-Key header.

    Returns:
        The validated API key string.

    Raises:
        HTTPException: 401 if missing/invalid, 503 if server key not configured.
    """
    server_key = _get_server_api_key()

    if server_key is None:
        raise HTTPException(
            status_code=503,
            detail="Server API key not configured. "
            "Set DATACONNECT_SERVER_API_KEY environment variable.",
        )

    if not api_key:
        raise HTTPException(
            status_code=401,
            detail="Missing X-API-Key header.",
        )

    if api_key != server_key:
        raise HTTPException(
            status_code=401,
            detail="Invalid API key.",
        )

    return api_key


def check_rate_limit(
    api_key: str = Depends(validate_api_key),
) -> str:
    """FastAPI dependency: enforce per-key rate limiting.

    Allows RATE_LIMIT_PER_MINUTE requests per key per rolling minute.

    Args:
        api_key: Validated API key (from validate_api_key).

    Returns:
        The API key string.

    Raises:
        HTTPException: 429 if rate limit exceeded.
    """
    now = time.monotonic()
    window_start = now - 60.0

    with _rate_lock:
        # Prune expired entries
        bucket = _rate_buckets[api_key]
        _rate_buckets[api_key] = [
            ts for ts in bucket if ts > window_start
        ]

        if len(_rate_buckets[api_key]) >= RATE_LIMIT_PER_MINUTE:
            raise HTTPException(
                status_code=429,
                detail=f"Rate limit exceeded: {RATE_LIMIT_PER_MINUTE} "
                f"requests per minute.",
            )

        _rate_buckets[api_key].append(now)

    return api_key


def reset_rate_limits() -> None:
    """Clear all rate limit state. Used in tests."""
    with _rate_lock:
        _rate_buckets.clear()
