"""API layer — FastAPI application factory.

Usage:
    from dataconnect.api import create_app
    app = create_app()
    # Run with: uvicorn dataconnect.api:app
"""

from __future__ import annotations

import logging
from pathlib import Path

from fastapi import FastAPI

from dataconnect.api.routes import router, set_storage_dir
from dataconnect.config import PROJECT_NAME

logger = logging.getLogger(__name__)

# Default storage location (same as CLI)
_DEFAULT_STORAGE_DIR = Path.home() / ".dataconnect"


def create_app(
    *,
    storage_dir: Path | None = None,
) -> FastAPI:
    """Create and configure the FastAPI application.

    Args:
        storage_dir: Directory for scan result storage.
            Defaults to ~/.dataconnect.

    Returns:
        Configured FastAPI instance.
    """
    app = FastAPI(
        title=f"{PROJECT_NAME} API",
        description=(
            "Query databases in plain English with verified SQL. "
            "All endpoints require X-API-Key header."
        ),
        version="0.1.0",
    )

    resolved_dir = storage_dir or _DEFAULT_STORAGE_DIR
    set_storage_dir(resolved_dir)
    logger.info("API storage directory: %s", resolved_dir)

    app.include_router(router)

    return app


# Module-level app for `uvicorn dataconnect.api:app`
app = create_app()
