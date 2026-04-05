"""Scanner layer — schema extraction, profiling, relationship discovery.

Top-level scan_database() orchestrates the full pipeline:
1. Schema extraction (tables + declared FKs)
2. Data profiling (sampling + column statistics)
3. Relationship discovery (name matching + value overlap)
4. Token estimation
5. ScanResult assembly
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from sqlalchemy.engine import Engine

from dataconnect.config import DEFAULT_SAMPLE_PERCENT, MAX_SAMPLE_ROWS
from dataconnect.exceptions import ScanError
from dataconnect.models import (
    RelationshipInfo,
    ScanResult,
    TableInfo,
)
from dataconnect.scanner.profiler import profile_tables
from dataconnect.scanner.relationships import discover_relationships
from dataconnect.scanner.schema import extract_schema

logger = logging.getLogger(__name__)

# Rough token multipliers (chars per token ~ 4 for English text)
_CHARS_PER_TOKEN = 4


def _estimate_tokens(tables: list[TableInfo], relationships: list[RelationshipInfo]) -> int:
    """Estimate token count for this scan result when serialized for LLM context.

    Uses a rough char-to-token ratio. The estimate covers table names,
    column names/types, descriptions, and relationship summaries — the
    information that gets injected into LLM prompts.

    Args:
        tables: Scanned tables with columns and profiles.
        relationships: All discovered relationships.

    Returns:
        Estimated token count.
    """
    char_count = 0

    for table in tables:
        # Table header: name + description
        char_count += len(table.name) + len(table.description) + 20

        for col in table.columns:
            # Column: name, type, flags, description
            char_count += len(col.name) + len(col.data_type) + 30
            if col.foreign_key_target:
                char_count += len(col.foreign_key_target) + 10
            if col.description:
                char_count += len(col.description)

        for profile in table.profiles:
            # Profile: column name + stats summary
            char_count += len(profile.column_name) + 40
            char_count += sum(len(v) for v in profile.sample_values)

    for rel in relationships:
        # Relationship: source.col -> target.col (type, confidence)
        char_count += (
            len(rel.source_table) + len(rel.source_column)
            + len(rel.target_table) + len(rel.target_column) + 30
        )

    return max(1, char_count // _CHARS_PER_TOKEN)


def _extract_database_name(engine: Engine) -> str:
    """Extract a human-readable database name from the engine URL.

    Args:
        engine: SQLAlchemy engine.

    Returns:
        Database name string. Falls back to dialect name if not extractable.
    """
    url = engine.url
    db_name = url.database
    if db_name:
        # Strip path components for file-based DBs (SQLite)
        if "/" in db_name:
            db_name = db_name.rsplit("/", 1)[-1]
        # Strip .db extension for cleaner names
        if db_name.endswith(".db"):
            db_name = db_name[:-3]
        return db_name
    return url.drivername or "unknown"


def scan_database(
    engine: Engine,
    database_name: str | None = None,
    schema: str | None = None,
    sample_pct: float = DEFAULT_SAMPLE_PERCENT,
    max_sample_rows: int = MAX_SAMPLE_ROWS,
) -> ScanResult:
    """Run the full scanner pipeline on a database.

    Orchestrates schema extraction, data profiling, and relationship
    discovery into a single ScanResult ready for storage and routing.

    Args:
        engine: SQLAlchemy engine connected to the target database.
        database_name: Override name for the database. Auto-detected if None.
        schema: Database schema to scan (None for default).
        sample_pct: Bernoulli sample percentage for profiling (0-100).
        max_sample_rows: Hard cap on profiling sample size per table.

    Returns:
        Complete ScanResult with tables, profiles, and relationships.

    Raises:
        ScanError: If schema extraction fails (profiling/relationship
            errors are logged and skipped gracefully).
    """
    db_name = database_name or _extract_database_name(engine)
    logger.info("Starting scan of database: %s", db_name)

    # Step 1: Schema extraction (tables + declared FKs)
    tables, fk_relationships = extract_schema(engine, schema=schema)
    logger.info(
        "Schema: %d tables, %d declared FKs",
        len(tables), len(fk_relationships),
    )

    if not tables:
        logger.warning("No tables found in database %s", db_name)
        return ScanResult(
            database_name=db_name,
            tables=[],
            relationships=[],
            token_estimate=0,
        )

    # Step 2: Data profiling (sampling + column statistics)
    tables = profile_tables(
        engine, tables,
        sample_pct=sample_pct,
        max_rows=max_sample_rows,
    )
    profiled_count = sum(1 for t in tables if t.profiles)
    logger.info("Profiled %d/%d tables", profiled_count, len(tables))

    # Step 3: Relationship discovery (name matching + value overlap)
    discovered = discover_relationships(tables, fk_relationships)
    all_relationships = fk_relationships + discovered
    logger.info(
        "Relationships: %d total (%d FK, %d discovered)",
        len(all_relationships), len(fk_relationships), len(discovered),
    )

    # Step 4: Token estimation
    token_estimate = _estimate_tokens(tables, all_relationships)
    logger.info("Estimated token count: %d", token_estimate)

    # Step 5: Assemble result
    result = ScanResult(
        database_name=db_name,
        scanned_at=datetime.now(UTC),
        tables=tables,
        relationships=all_relationships,
        token_estimate=token_estimate,
    )

    logger.info("Scan complete for %s", db_name)
    return result
