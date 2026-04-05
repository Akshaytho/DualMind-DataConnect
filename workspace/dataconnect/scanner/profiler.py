"""Data sampling and statistical profiling.

Samples rows from each table and computes per-column statistics
(null_fraction, distinct_count, sample_values, min/max). Uses
TABLESAMPLE BERNOULLI for PostgreSQL; ORDER BY RANDOM() LIMIT N
fallback for SQLite and other dialects.
"""

from __future__ import annotations

import logging

from sqlalchemy import text
from sqlalchemy.engine import Engine

from dataconnect.config import DEFAULT_SAMPLE_PERCENT, MAX_SAMPLE_ROWS
from dataconnect.exceptions import ProfilingError
from dataconnect.models import ColumnProfile, TableInfo

logger = logging.getLogger(__name__)

# Max sample values stored per column
_MAX_SAMPLE_VALUES = 10


def _get_row_count(engine: Engine, table_name: str) -> int:
    """Estimate row count for a table via COUNT(*).

    Args:
        engine: SQLAlchemy engine.
        table_name: Table to count.

    Returns:
        Row count (0 if table is empty or query fails).
    """
    # Use quoted identifier to handle reserved words
    stmt = text(f'SELECT COUNT(*) FROM "{table_name}"')
    with engine.connect() as conn:
        result = conn.execute(stmt)
        row = result.fetchone()
        return int(row[0]) if row else 0


def _build_sample_query(
    engine: Engine,
    table_name: str,
    row_count: int,
    sample_pct: float,
    max_rows: int,
) -> str:
    """Build a dialect-appropriate sampling query.

    PostgreSQL: TABLESAMPLE BERNOULLI(pct)
    SQLite/others: ORDER BY RANDOM() LIMIT N

    Args:
        engine: SQLAlchemy engine (used to detect dialect).
        table_name: Table to sample from.
        row_count: Known row count of the table.
        sample_pct: Percentage to sample (0-100).
        max_rows: Hard cap on returned rows.

    Returns:
        SQL query string for sampling.
    """
    dialect_name = engine.dialect.name

    if dialect_name == "postgresql":
        return (
            f'SELECT * FROM "{table_name}" '
            f"TABLESAMPLE BERNOULLI({sample_pct}) "
            f"LIMIT {max_rows}"
        )

    # Fallback: calculate limit from percentage
    limit = min(max(int(row_count * sample_pct / 100), 1), max_rows)
    return f'SELECT * FROM "{table_name}" ORDER BY RANDOM() LIMIT {limit}'


def _profile_column(
    column_name: str,
    values: list[object],
    total_rows: int,
) -> ColumnProfile:
    """Compute statistics for a single column from sampled values.

    Args:
        column_name: Name of the column.
        values: Raw values from the sample (may contain None).
        total_rows: Total rows in sample (for null fraction).

    Returns:
        Populated ColumnProfile.
    """
    if total_rows == 0:
        return ColumnProfile(column_name=column_name)

    null_count = sum(1 for v in values if v is None)
    null_fraction = round(null_count / total_rows, 4)

    non_null = [v for v in values if v is not None]
    distinct_count = len(set(str(v) for v in non_null))

    # Sample values: unique, stringified, capped
    seen: set[str] = set()
    sample_values: list[str] = []
    for v in non_null:
        s = str(v)
        if s not in seen and len(sample_values) < _MAX_SAMPLE_VALUES:
            seen.add(s)
            sample_values.append(s)

    # Min/max on sortable string representations
    min_value: str | None = None
    max_value: str | None = None
    if non_null:
        str_vals = [str(v) for v in non_null]
        min_value = min(str_vals)
        max_value = max(str_vals)

    return ColumnProfile(
        column_name=column_name,
        null_fraction=null_fraction,
        distinct_count=distinct_count,
        sample_values=sample_values,
        min_value=min_value,
        max_value=max_value,
    )


def profile_table(
    engine: Engine,
    table: TableInfo,
    sample_pct: float = DEFAULT_SAMPLE_PERCENT,
    max_rows: int = MAX_SAMPLE_ROWS,
) -> TableInfo:
    """Profile a single table: row count + per-column statistics.

    Mutates table in-place (sets row_count_estimate and profiles).

    Args:
        engine: SQLAlchemy engine.
        table: TableInfo with columns already populated.
        sample_pct: Bernoulli sample percentage (0-100).
        max_rows: Hard cap on sample size.

    Returns:
        The same TableInfo with profiles and row_count_estimate set.

    Raises:
        ProfilingError: If profiling fails for this table.
    """
    try:
        row_count = _get_row_count(engine, table.name)
    except Exception as exc:
        raise ProfilingError(
            f"Failed to count rows in {table.name}: {exc}"
        ) from exc

    table.row_count_estimate = row_count

    if row_count == 0:
        table.profiles = [
            ColumnProfile(column_name=col.name) for col in table.columns
        ]
        return table

    query = _build_sample_query(engine, table.name, row_count, sample_pct, max_rows)

    try:
        with engine.connect() as conn:
            result = conn.execute(text(query))
            col_names = list(result.keys())
            rows = result.fetchall()
    except Exception as exc:
        raise ProfilingError(
            f"Failed to sample {table.name}: {exc}"
        ) from exc

    total = len(rows)
    logger.info("Sampled %d rows from %s (total: %d)", total, table.name, row_count)

    # Build column index for fast lookup
    col_idx = {name: i for i, name in enumerate(col_names)}

    profiles: list[ColumnProfile] = []
    for col in table.columns:
        if col.name in col_idx:
            idx = col_idx[col.name]
            values = [row[idx] for row in rows]
            profiles.append(_profile_column(col.name, values, total))
        else:
            profiles.append(ColumnProfile(column_name=col.name))

    table.profiles = profiles
    return table


def profile_tables(
    engine: Engine,
    tables: list[TableInfo],
    sample_pct: float = DEFAULT_SAMPLE_PERCENT,
    max_rows: int = MAX_SAMPLE_ROWS,
) -> list[TableInfo]:
    """Profile all tables. Skips tables that fail with a warning.

    Args:
        engine: SQLAlchemy engine.
        tables: List of TableInfo (columns must be populated).
        sample_pct: Bernoulli sample percentage (0-100).
        max_rows: Hard cap on sample size.

    Returns:
        Same list with profiles populated (failed tables are skipped).
    """
    for table in tables:
        try:
            profile_table(engine, table, sample_pct, max_rows)
        except ProfilingError as exc:
            logger.warning("Skipping profiling for %s: %s", table.name, exc)

    return tables
