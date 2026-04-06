"""CLI interface — dataconnect scan/ask/list/info commands.

Entry point for the DataConnect command-line tool.
Uses click for argument parsing and output formatting.
"""

from __future__ import annotations

import logging
import sys
import time
from pathlib import Path

import click

from dataconnect.config import (
    CONFIDENCE_HIGH,
    CONFIDENCE_LOW,
    CONFIDENCE_MEDIUM,
    PROJECT_NAME,
    sanitize_connection_string,
)
from dataconnect.exceptions import (
    DataConnectError,
    GenerationError,
    LLMError,
    RoutingError,
    ScanError,
    StorageError,
)

logger = logging.getLogger(__name__)

# Default storage location
_DEFAULT_STORAGE_DIR = Path.home() / ".dataconnect"


def _setup_logging(verbose: bool) -> None:
    """Configure logging based on verbosity flag.

    Args:
        verbose: If True, set DEBUG level. Otherwise WARNING.
    """
    level = logging.DEBUG if verbose else logging.WARNING
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        stream=sys.stderr,
    )


def _confidence_label(score: float) -> str:
    """Map confidence score to human-readable label.

    Args:
        score: Confidence score 0-100.

    Returns:
        Label string.
    """
    if score >= CONFIDENCE_HIGH:
        return "HIGH"
    if score >= CONFIDENCE_MEDIUM:
        return "MEDIUM"
    if score >= CONFIDENCE_LOW:
        return "LOW"
    return "UNVERIFIED"


@click.group()
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose logging.")
@click.option(
    "--storage-dir",
    type=click.Path(),
    default=str(_DEFAULT_STORAGE_DIR),
    envvar="DATACONNECT_STORAGE_DIR",
    help="Storage directory for scan results.",
    show_default=True,
)
@click.pass_context
def cli(ctx: click.Context, verbose: bool, storage_dir: str) -> None:
    """DataConnect — query databases in plain English with verified SQL."""
    _setup_logging(verbose)
    ctx.ensure_object(dict)
    ctx.obj["storage_dir"] = Path(storage_dir)


@cli.command()
@click.argument("connection_string")
@click.option("--name", "-n", default=None, help="Override database name.")
@click.option("--schema", "-s", default=None, help="Database schema to scan.")
@click.pass_context
def scan(
    ctx: click.Context,
    connection_string: str,
    name: str | None,
    schema: str | None,
) -> None:
    """Scan a database and store the schema index.

    CONNECTION_STRING is a SQLAlchemy connection URL, e.g.
    postgresql://user:pass@host/dbname or sqlite:///path/to/db.sqlite
    """
    from dataconnect.database import create_readonly_engine
    from dataconnect.scanner import scan_database
    from dataconnect.storage import StorageBackend

    safe_conn = sanitize_connection_string(connection_string)
    click.echo(f"Scanning database: {safe_conn}")

    try:
        engine = create_readonly_engine(connection_string)
    except DataConnectError as exc:
        click.echo(f"Error connecting: {exc}", err=True)
        raise SystemExit(1) from exc

    try:
        start = time.monotonic()
        result = scan_database(engine, database_name=name, schema=schema)
        elapsed = time.monotonic() - start
    except ScanError as exc:
        click.echo(f"Scan failed: {exc}", err=True)
        raise SystemExit(1) from exc
    finally:
        engine.dispose()

    # Save to storage
    try:
        storage = StorageBackend(ctx.obj["storage_dir"])
        storage.save_scan(result)
    except StorageError as exc:
        click.echo(f"Failed to save scan: {exc}", err=True)
        raise SystemExit(1) from exc

    click.echo(
        f"Scan complete: {result.database_name}\n"
        f"  Tables: {len(result.tables)}\n"
        f"  Relationships: {len(result.relationships)}\n"
        f"  Token estimate: {result.token_estimate:,}\n"
        f"  Time: {elapsed:.1f}s"
    )


@cli.command()
@click.argument("question")
@click.option("--db", "-d", required=True, help="Database name (from scan).")
@click.option(
    "--model", "-m",
    envvar="DATACONNECT_MODEL",
    required=True,
    help="LLM model ID (e.g. gpt-4o, claude-sonnet-4-20250514).",
)
@click.option(
    "--api-key", "-k",
    envvar="DATACONNECT_API_KEY",
    required=True,
    help="LLM API key (or set DATACONNECT_API_KEY env var).",
)
@click.option("--no-retry", is_flag=True, help="Skip fix-and-retry loop.")
@click.option(
    "--profile", "-p",
    default=None,
    help="Tuning profile: preset name (default/strict/lenient) or JSON path.",
)
@click.pass_context
def ask(
    ctx: click.Context,
    question: str,
    db: str,
    model: str,
    api_key: str,
    no_retry: bool,
    profile: str | None,
) -> None:
    """Ask a question about a scanned database.

    QUESTION is a natural-language query, e.g.
    "How many orders were placed last month?"
    """
    from dataconnect.generator import generate_sql
    from dataconnect.models import QueryResult
    from dataconnect.router import route_query
    from dataconnect.storage import StorageBackend
    from dataconnect.tuning import TuningError, get_profile
    from dataconnect.verifier import verify_sql
    from dataconnect.verifier.retry import retry_with_fixes

    # Step 0: Load tuning profile
    try:
        tuning = get_profile(profile)
    except TuningError as exc:
        click.echo(f"Invalid profile: {exc}", err=True)
        raise SystemExit(1) from exc

    if profile is not None:
        click.echo(f"Using tuning profile: {tuning.name}")

    # Step 1: Load scan result
    try:
        storage = StorageBackend(ctx.obj["storage_dir"])
        scan_result = storage.load_scan(db)
    except StorageError as exc:
        click.echo(f"Storage error: {exc}", err=True)
        raise SystemExit(1) from exc

    if scan_result is None:
        click.echo(
            f"No scan found for database '{db}'. Run 'dataconnect scan' first.",
            err=True,
        )
        raise SystemExit(1)

    start = time.monotonic()

    # Step 2: Route query to relevant tables
    try:
        route_result = route_query(
            question,
            scan_result,
            llm_model=model,
            llm_api_key=api_key,
            top_k=tuning.router_top_k,
        )
    except RoutingError as exc:
        click.echo(f"Routing failed: {exc}", err=True)
        raise SystemExit(1) from exc

    table_names = [m.table_name for m in route_result.matched_tables]
    click.echo(f"Selected tables: {', '.join(table_names)}")

    # Step 3: Generate SQL via LLM
    try:
        sql = generate_sql(
            question,
            scan_result,
            route_result,
            model=model,
            api_key=api_key,
        )
    except (GenerationError, LLMError) as exc:
        click.echo(f"SQL generation failed: {exc}", err=True)
        raise SystemExit(1) from exc

    # Step 4: Verify (and optionally retry)
    context = {"scan_result": scan_result, "route_result": route_result}

    try:
        if no_retry:
            verification = verify_sql(sql, context, profile=tuning)
        else:
            verification = retry_with_fixes(
                sql,
                question,
                context,
                model=model,
                api_key=api_key,
                max_attempts=tuning.max_retry_attempts,
                profile=tuning,
            )
    except DataConnectError as exc:
        click.echo(f"Verification failed: {exc}", err=True)
        raise SystemExit(1) from exc

    elapsed_ms = (time.monotonic() - start) * 1000

    # Step 5: Build and display result
    result = QueryResult(
        query=question,
        sql=verification.sql,
        verification=verification,
        route=route_result,
        execution_time_ms=elapsed_ms,
    )

    label = _confidence_label(verification.confidence_score)
    click.echo(f"\n-- Generated SQL ({label} confidence: "
               f"{verification.confidence_score:.0f}%) --")
    click.echo(verification.sql)

    # Show check details
    click.echo(f"\nVerification ({verification.attempt_number} "
               f"attempt{'s' if verification.attempt_number > 1 else ''}):")
    for check in verification.checks:
        click.echo(f"  [{check.status.value.upper():8s}] {check.check_name}")
        if check.message:
            click.echo(f"             {check.message}")

    if not verification.is_verified:
        click.echo(
            "\nWARNING: Query did not reach verified confidence threshold.",
            err=True,
        )

    click.echo(f"\nTime: {elapsed_ms:.0f}ms")


@cli.command(name="list")
@click.pass_context
def list_databases(ctx: click.Context) -> None:
    """List all scanned databases."""
    from dataconnect.storage import StorageBackend

    try:
        storage = StorageBackend(ctx.obj["storage_dir"])
        databases = storage.list_databases()
    except StorageError as exc:
        click.echo(f"Storage error: {exc}", err=True)
        raise SystemExit(1) from exc

    if not databases:
        click.echo("No scanned databases found.")
        return

    click.echo(f"Scanned databases ({len(databases)}):")
    for name in sorted(databases):
        click.echo(f"  - {name}")


@cli.command()
@click.argument("db_name")
@click.pass_context
def info(ctx: click.Context, db_name: str) -> None:
    """Show details about a scanned database.

    DB_NAME is the database name from 'dataconnect list'.
    """
    from dataconnect.storage import StorageBackend

    try:
        storage = StorageBackend(ctx.obj["storage_dir"])
        result = storage.load_scan(db_name)
    except StorageError as exc:
        click.echo(f"Storage error: {exc}", err=True)
        raise SystemExit(1) from exc

    if result is None:
        click.echo(f"No scan found for '{db_name}'.", err=True)
        raise SystemExit(1)

    click.echo(f"Database: {result.database_name}")
    click.echo(f"Scanned: {result.scanned_at.isoformat()}")
    click.echo(f"Tables: {len(result.tables)}")
    click.echo(f"Relationships: {len(result.relationships)}")
    click.echo(f"Token estimate: {result.token_estimate:,}")

    if result.tables:
        click.echo("\nTables:")
        for table in result.tables:
            cols = len(table.columns)
            rows = table.row_count_estimate
            click.echo(f"  {table.name} ({cols} columns, ~{rows:,} rows)")


def _register_commands() -> None:
    """Register additional CLI commands from submodules."""
    from dataconnect.cli_benchmark import register_benchmark

    register_benchmark(cli)


_register_commands()


def main() -> None:
    """Entry point for console_scripts."""
    cli()
