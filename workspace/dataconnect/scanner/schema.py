"""Schema extraction via SQLAlchemy inspect().

Pulls table metadata, column details, and declared foreign keys
from a connected database engine. Returns structured models ready
for profiling and relationship discovery.
"""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import inspect as sa_inspect
from sqlalchemy.engine import Engine

from dataconnect.exceptions import ScanError
from dataconnect.models import (
    ColumnInfo,
    RelationshipInfo,
    RelationshipType,
    TableInfo,
)

logger = logging.getLogger(__name__)


def _extract_column(col: dict[str, Any], pk_columns: set[str]) -> ColumnInfo:
    """Build ColumnInfo from a SQLAlchemy column dict.

    Args:
        col: Column metadata dict from inspector.get_columns().
        pk_columns: Set of primary key column names for this table.

    Returns:
        Populated ColumnInfo model.
    """
    return ColumnInfo(
        name=col["name"],
        data_type=str(col.get("type", "UNKNOWN")),
        nullable=col.get("nullable", True),
        is_primary_key=col["name"] in pk_columns,
    )


def _extract_foreign_keys(
    inspector: Any,
    table_name: str,
    schema: str | None,
) -> list[tuple[ColumnInfo, RelationshipInfo]]:
    """Extract FK info and build column patches + relationships.

    Args:
        inspector: SQLAlchemy Inspector instance.
        table_name: Table to inspect.
        schema: Schema name (None for default).

    Returns:
        List of (column_patch, relationship) tuples. Column patches
        carry FK metadata to merge into the column list.
    """
    results: list[tuple[ColumnInfo, RelationshipInfo]] = []
    fks = inspector.get_foreign_keys(table_name, schema=schema)

    for fk in fks:
        ref_table = fk.get("referred_table", "")
        ref_schema = fk.get("referred_schema")
        constrained = fk.get("constrained_columns", [])
        referred = fk.get("referred_columns", [])

        for src_col, tgt_col in zip(constrained, referred):
            target_qualified = (
                f"{ref_schema}.{ref_table}.{tgt_col}"
                if ref_schema and ref_schema != schema
                else f"{ref_table}.{tgt_col}"
            )
            col_patch = ColumnInfo(
                name=src_col,
                data_type="",
                is_foreign_key=True,
                foreign_key_target=target_qualified,
            )
            rel = RelationshipInfo(
                source_table=table_name,
                source_column=src_col,
                target_table=ref_table,
                target_column=tgt_col,
                relationship_type=RelationshipType.DECLARED_FK,
                confidence=1.0,
            )
            results.append((col_patch, rel))

    return results


def extract_schema(
    engine: Engine,
    schema: str | None = None,
) -> tuple[list[TableInfo], list[RelationshipInfo]]:
    """Extract full schema metadata from a database engine.

    Uses SQLAlchemy inspect() to pull table names, columns, primary
    keys, and foreign keys. Returns structured models without touching
    actual data rows.

    Args:
        engine: SQLAlchemy engine to inspect.
        schema: Database schema to scan (None for default).

    Returns:
        Tuple of (tables, relationships).

    Raises:
        ScanError: If schema extraction fails.
    """
    try:
        inspector = sa_inspect(engine)
    except Exception as exc:
        raise ScanError(f"Failed to create inspector: {exc}") from exc

    tables: list[TableInfo] = []
    relationships: list[RelationshipInfo] = []

    try:
        table_names = inspector.get_table_names(schema=schema)
    except Exception as exc:
        raise ScanError(f"Failed to list tables: {exc}") from exc

    logger.info("Found %d tables in schema=%s", len(table_names), schema)

    for table_name in table_names:
        try:
            table_info, table_rels = _extract_table(
                inspector, table_name, schema
            )
            tables.append(table_info)
            relationships.extend(table_rels)
        except Exception as exc:
            logger.warning(
                "Skipping table %s due to error: %s", table_name, exc
            )

    logger.info(
        "Extracted %d tables, %d relationships",
        len(tables),
        len(relationships),
    )
    return tables, relationships


def _extract_table(
    inspector: Any,
    table_name: str,
    schema: str | None,
) -> tuple[TableInfo, list[RelationshipInfo]]:
    """Extract metadata for a single table.

    Args:
        inspector: SQLAlchemy Inspector instance.
        table_name: Name of the table.
        schema: Schema name (None for default).

    Returns:
        Tuple of (TableInfo, list of RelationshipInfo).

    Raises:
        ScanError: If extraction fails for this table.
    """
    try:
        raw_columns = inspector.get_columns(table_name, schema=schema)
        pk_constraint = inspector.get_pk_constraint(table_name, schema=schema)
        pk_columns = set(pk_constraint.get("constrained_columns", []))
    except Exception as exc:
        raise ScanError(
            f"Failed to inspect table {table_name}: {exc}"
        ) from exc

    # Build column list
    columns = [_extract_column(col, pk_columns) for col in raw_columns]

    # Extract foreign keys and merge into columns
    fk_results = _extract_foreign_keys(inspector, table_name, schema)
    fk_map: dict[str, ColumnInfo] = {
        patch.name: patch for patch, _ in fk_results
    }
    rels = [rel for _, rel in fk_results]

    # Merge FK flags into existing columns
    for col in columns:
        if col.name in fk_map:
            col.is_foreign_key = True
            col.foreign_key_target = fk_map[col.name].foreign_key_target

    schema_name = schema or "public"
    table = TableInfo(
        name=table_name,
        schema_name=schema_name,
        columns=columns,
    )

    return table, rels
