"""API route handlers — FastAPI router with all endpoints.

Routes mirror the CLI commands:
  POST /scan — scan a database
  POST /ask  — ask a question
  GET  /databases — list scanned databases
  GET  /databases/{name} — database details
"""

from __future__ import annotations

import logging
import time
from pathlib import Path

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field

from dataconnect.api.auth import check_rate_limit
from dataconnect.config import sanitize_connection_string
from dataconnect.exceptions import (
    DataConnectError,
    GenerationError,
    LLMError,
    RoutingError,
    ScanError,
    StorageError,
)

logger = logging.getLogger(__name__)

router = APIRouter()


# ── Request / Response schemas ─────────────────────────────────────


class ScanRequest(BaseModel):
    """POST /scan request body."""

    connection_string: str = Field(
        ..., description="SQLAlchemy connection URL."
    )
    database_name: str | None = Field(
        default=None, description="Override database name."
    )
    schema_name: str | None = Field(
        default=None, description="Database schema to scan."
    )


class ScanResponse(BaseModel):
    """POST /scan response."""

    database_name: str
    tables: int
    relationships: int
    token_estimate: int
    elapsed_seconds: float


class AskRequest(BaseModel):
    """POST /ask request body."""

    question: str = Field(
        ..., description="Natural-language question about the database."
    )
    database_name: str = Field(
        ..., description="Name of a previously scanned database."
    )
    model: str = Field(
        ..., description="LLM model ID (e.g. gpt-4o)."
    )
    llm_api_key: str = Field(
        ..., description="API key for the LLM provider."
    )
    retry: bool = Field(
        default=True, description="Enable fix-and-retry loop."
    )
    profile: str | None = Field(
        default=None,
        description="Tuning profile: preset name (default/strict/lenient) or JSON path.",
    )


class CheckDetail(BaseModel):
    """Single verification check in the response."""

    check_name: str
    status: str
    message: str = ""


class AskResponse(BaseModel):
    """POST /ask response."""

    question: str
    sql: str
    confidence_score: float
    confidence_label: str
    is_verified: bool
    attempt_number: int
    checks: list[CheckDetail]
    selected_tables: list[str]
    execution_time_ms: float


class DatabaseListResponse(BaseModel):
    """GET /databases response."""

    databases: list[str]
    count: int


class TableSummary(BaseModel):
    """Table info within database details."""

    name: str
    columns: int
    row_count_estimate: int


class DatabaseInfoResponse(BaseModel):
    """GET /databases/{name} response."""

    database_name: str
    scanned_at: str
    tables: int
    relationships: int
    token_estimate: int
    table_details: list[TableSummary]


class ErrorResponse(BaseModel):
    """Standard error response body."""

    detail: str


class HealthResponse(BaseModel):
    """GET /health response."""

    status: str
    version: str
    databases: int


# ── Storage helper ─────────────────────────────────────────────────

# Injected by create_app via app.state; accessed via dependency
_storage_dir: Path | None = None


def set_storage_dir(path: Path) -> None:
    """Set the storage directory for route handlers.

    Called by create_app during startup.

    Args:
        path: Storage directory path.
    """
    global _storage_dir  # noqa: PLW0603
    _storage_dir = path


def _get_storage_dir() -> Path:
    """Return the configured storage directory.

    Returns:
        Storage directory path.

    Raises:
        RuntimeError: If not configured.
    """
    if _storage_dir is None:
        msg = "Storage directory not configured"
        raise RuntimeError(msg)
    return _storage_dir


def _confidence_label(score: float) -> str:
    """Map confidence score to label.

    Args:
        score: Confidence score 0-100.

    Returns:
        Human-readable label.
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


# ── Route handlers ─────────────────────────────────────────────────


@router.get(
    "/health",
    response_model=HealthResponse,
    summary="Health check — no authentication required.",
)
def health_check() -> HealthResponse:
    """Return service status, version, and database count."""
    from dataconnect import __version__

    db_count = 0
    try:
        from dataconnect.storage import StorageBackend

        storage = StorageBackend(_get_storage_dir())
        db_count = len(storage.list_databases())
    except Exception:
        pass  # Storage not configured yet — still healthy

    return HealthResponse(
        status="ok",
        version=__version__,
        databases=db_count,
    )


@router.post(
    "/scan",
    response_model=ScanResponse,
    responses={400: {"model": ErrorResponse}, 500: {"model": ErrorResponse}},
    summary="Scan a database and store the schema index.",
)
def scan_database_endpoint(
    request: ScanRequest,
    _api_key: str = Depends(check_rate_limit),
) -> ScanResponse:
    """Connect to a database, scan its schema, and save the result."""
    from dataconnect.database import create_readonly_engine
    from dataconnect.scanner import scan_database
    from dataconnect.storage import StorageBackend

    safe_conn = sanitize_connection_string(request.connection_string)
    logger.info("API scan request for: %s", safe_conn)

    try:
        engine = create_readonly_engine(request.connection_string)
    except DataConnectError as exc:
        from fastapi import HTTPException

        raise HTTPException(status_code=400, detail=str(exc)) from exc

    try:
        start = time.monotonic()
        result = scan_database(
            engine,
            database_name=request.database_name,
            schema=request.schema_name,
        )
        elapsed = time.monotonic() - start
    except ScanError as exc:
        from fastapi import HTTPException

        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        engine.dispose()

    try:
        storage = StorageBackend(_get_storage_dir())
        storage.save_scan(result)
    except StorageError as exc:
        from fastapi import HTTPException

        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return ScanResponse(
        database_name=result.database_name,
        tables=len(result.tables),
        relationships=len(result.relationships),
        token_estimate=result.token_estimate,
        elapsed_seconds=round(elapsed, 2),
    )


@router.post(
    "/ask",
    response_model=AskResponse,
    responses={400: {"model": ErrorResponse}, 500: {"model": ErrorResponse}},
    summary="Ask a natural-language question about a scanned database.",
)
def ask_question_endpoint(
    request: AskRequest,
    _api_key: str = Depends(check_rate_limit),
) -> AskResponse:
    """Run the full pipeline: load → route → generate → verify → retry."""
    from dataconnect.generator import generate_sql
    from dataconnect.router import route_query
    from dataconnect.storage import StorageBackend
    from dataconnect.tuning import get_profile
    from dataconnect.verifier import verify_sql
    from dataconnect.verifier.retry import retry_with_fixes

    # Load tuning profile
    try:
        tuning = get_profile(request.profile)
    except DataConnectError as exc:
        from fastapi import HTTPException

        raise HTTPException(status_code=400, detail=str(exc)) from exc

    # Load scan result
    try:
        storage = StorageBackend(_get_storage_dir())
        scan_result = storage.load_scan(request.database_name)
    except StorageError as exc:
        from fastapi import HTTPException

        raise HTTPException(status_code=500, detail=str(exc)) from exc

    if scan_result is None:
        from fastapi import HTTPException

        raise HTTPException(
            status_code=404,
            detail=f"No scan found for database '{request.database_name}'. "
            "Run POST /scan first.",
        )

    start = time.monotonic()

    # Route query
    try:
        route_result = route_query(
            request.question,
            scan_result,
            llm_model=request.model,
            llm_api_key=request.llm_api_key,
        )
    except RoutingError as exc:
        from fastapi import HTTPException

        raise HTTPException(status_code=500, detail=str(exc)) from exc

    # Generate SQL
    try:
        sql = generate_sql(
            request.question,
            scan_result,
            route_result,
            model=request.model,
            api_key=request.llm_api_key,
        )
    except (GenerationError, LLMError) as exc:
        from fastapi import HTTPException

        raise HTTPException(status_code=500, detail=str(exc)) from exc

    # Verify (and optionally retry)
    context = {"scan_result": scan_result, "route_result": route_result}

    try:
        if request.retry:
            verification = retry_with_fixes(
                sql,
                request.question,
                context,
                model=request.model,
                api_key=request.llm_api_key,
                profile=tuning,
            )
        else:
            verification = verify_sql(sql, context, profile=tuning)
    except DataConnectError as exc:
        from fastapi import HTTPException

        raise HTTPException(status_code=500, detail=str(exc)) from exc

    elapsed_ms = (time.monotonic() - start) * 1000

    checks = [
        CheckDetail(
            check_name=c.check_name,
            status=c.status.value,
            message=c.message,
        )
        for c in verification.checks
    ]

    table_names = [m.table_name for m in route_result.matched_tables]

    return AskResponse(
        question=request.question,
        sql=verification.sql,
        confidence_score=verification.confidence_score,
        confidence_label=_confidence_label(verification.confidence_score),
        is_verified=verification.is_verified,
        attempt_number=verification.attempt_number,
        checks=checks,
        selected_tables=table_names,
        execution_time_ms=round(elapsed_ms, 1),
    )


@router.get(
    "/databases",
    response_model=DatabaseListResponse,
    responses={500: {"model": ErrorResponse}},
    summary="List all scanned databases.",
)
def list_databases_endpoint(
    _api_key: str = Depends(check_rate_limit),
) -> DatabaseListResponse:
    """Return names of all previously scanned databases."""
    from dataconnect.storage import StorageBackend

    try:
        storage = StorageBackend(_get_storage_dir())
        databases = storage.list_databases()
    except StorageError as exc:
        from fastapi import HTTPException

        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return DatabaseListResponse(
        databases=sorted(databases),
        count=len(databases),
    )


@router.get(
    "/databases/{name}",
    response_model=DatabaseInfoResponse,
    responses={404: {"model": ErrorResponse}, 500: {"model": ErrorResponse}},
    summary="Get details about a scanned database.",
)
def database_info_endpoint(
    name: str,
    _api_key: str = Depends(check_rate_limit),
) -> DatabaseInfoResponse:
    """Return schema details for a previously scanned database."""
    from dataconnect.storage import StorageBackend

    try:
        storage = StorageBackend(_get_storage_dir())
        result = storage.load_scan(name)
    except StorageError as exc:
        from fastapi import HTTPException

        raise HTTPException(status_code=500, detail=str(exc)) from exc

    if result is None:
        from fastapi import HTTPException

        raise HTTPException(
            status_code=404,
            detail=f"No scan found for '{name}'.",
        )

    table_details = [
        TableSummary(
            name=t.name,
            columns=len(t.columns),
            row_count_estimate=t.row_count_estimate,
        )
        for t in result.tables
    ]

    return DatabaseInfoResponse(
        database_name=result.database_name,
        scanned_at=result.scanned_at.isoformat(),
        tables=len(result.tables),
        relationships=len(result.relationships),
        token_estimate=result.token_estimate,
        table_details=table_details,
    )
