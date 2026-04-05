"""Non-FK relationship discovery via name matching and value overlap.

Discovers implicit relationships between tables by:
1. Fuzzy name matching — column naming conventions (e.g., user_id → users.id)
2. Value overlap — Jaccard similarity of sample values between candidate columns

Runs AFTER profiling, since it needs sample_values from ColumnProfile.
"""

from __future__ import annotations

import logging
import re

from dataconnect.models import (
    ColumnProfile,
    RelationshipInfo,
    RelationshipType,
    TableInfo,
)

logger = logging.getLogger(__name__)

# Confidence thresholds
_NAME_MATCH_CONFIDENCE = 0.75
_VALUE_OVERLAP_CONFIDENCE_HIGH = 0.85
_VALUE_OVERLAP_CONFIDENCE_LOW = 0.55
_JACCARD_THRESHOLD = 0.3  # Minimum overlap to consider a relationship


def _singularize(name: str) -> str:
    """Naive English singularization — strips trailing 's'/'es'.

    Handles ~90% of table naming conventions. Not a full NLP stemmer.
    """
    lower = name.lower()
    if lower.endswith("ies") and len(lower) > 3:
        return lower[:-3] + "y"
    if lower.endswith("ses") or lower.endswith("xes"):
        return lower[:-2]
    if lower.endswith("s") and not lower.endswith("ss"):
        return lower[:-1]
    return lower


def _build_pk_index(
    tables: list[TableInfo],
) -> dict[str, list[str]]:
    """Map table_name -> list of primary key column names.

    Returns:
        Dict keyed by lowercase table name.
    """
    index: dict[str, list[str]] = {}
    for table in tables:
        pks = [col.name for col in table.columns if col.is_primary_key]
        index[table.name.lower()] = pks
    return index


def _build_profile_index(
    tables: list[TableInfo],
) -> dict[tuple[str, str], ColumnProfile]:
    """Map (table_name, column_name) -> ColumnProfile for fast lookup."""
    index: dict[tuple[str, str], ColumnProfile] = {}
    for table in tables:
        for profile in table.profiles:
            index[(table.name.lower(), profile.column_name.lower())] = profile
    return index


def _existing_pair_set(
    relationships: list[RelationshipInfo],
) -> set[tuple[str, str, str, str]]:
    """Build set of (src_table, src_col, tgt_table, tgt_col) for dedup."""
    pairs: set[tuple[str, str, str, str]] = set()
    for rel in relationships:
        pairs.add((
            rel.source_table.lower(),
            rel.source_column.lower(),
            rel.target_table.lower(),
            rel.target_column.lower(),
        ))
    return pairs


def _jaccard_similarity(a: list[str], b: list[str]) -> float:
    """Jaccard index between two sample value lists.

    Returns 0.0 if either list is empty.
    """
    if not a or not b:
        return 0.0
    set_a = set(a)
    set_b = set(b)
    intersection = len(set_a & set_b)
    union = len(set_a | set_b)
    return intersection / union if union > 0 else 0.0


# Common FK naming patterns: table_id, tableId, table_fk
_FK_PATTERN = re.compile(
    r"^(.+?)(?:_id|_fk|id)$",
    re.IGNORECASE,
)


def discover_name_matches(
    tables: list[TableInfo],
    existing: list[RelationshipInfo],
) -> list[RelationshipInfo]:
    """Find relationships via column naming conventions.

    Looks for patterns like `user_id` in table `orders` that likely
    references `users.id`. Matches against both plural and singular
    table name forms.

    Args:
        tables: Profiled tables with columns populated.
        existing: Already-known relationships (FKs) to skip.

    Returns:
        New RelationshipInfo entries with type NAME_MATCH.
    """
    pk_index = _build_pk_index(tables)
    table_names = {t.name.lower() for t in tables}
    known = _existing_pair_set(existing)
    results: list[RelationshipInfo] = []

    for table in tables:
        for col in table.columns:
            # Skip PKs and already-known FKs
            if col.is_primary_key or col.is_foreign_key:
                continue

            match = _FK_PATTERN.match(col.name)
            if not match:
                continue

            prefix = match.group(1).lower()

            # Try matching prefix to table names (plural and singular)
            candidates: list[str] = []
            for tname in table_names:
                if tname == table.name.lower():
                    continue  # skip self-references
                tname_lower = tname.lower()
                singular = _singularize(tname_lower)
                if prefix == tname_lower or prefix == singular:
                    candidates.append(tname)

            for target_table in candidates:
                # Match against PKs in target table
                pks = pk_index.get(target_table, [])
                target_col = pks[0] if pks else "id"

                pair = (
                    table.name.lower(),
                    col.name.lower(),
                    target_table,
                    target_col.lower(),
                )
                if pair in known:
                    continue

                results.append(RelationshipInfo(
                    source_table=table.name,
                    source_column=col.name,
                    target_table=target_table,
                    target_column=target_col,
                    relationship_type=RelationshipType.NAME_MATCH,
                    confidence=_NAME_MATCH_CONFIDENCE,
                ))
                known.add(pair)
                logger.info(
                    "Name match: %s.%s -> %s.%s",
                    table.name, col.name, target_table, target_col,
                )

    return results


def discover_value_overlaps(
    tables: list[TableInfo],
    existing: list[RelationshipInfo],
) -> list[RelationshipInfo]:
    """Find relationships via sample value overlap (Jaccard similarity).

    Compares sample_values between columns across tables. High overlap
    between a non-PK column and a PK column suggests a FK relationship.

    Args:
        tables: Profiled tables (must have profiles with sample_values).
        existing: Already-known relationships to skip.

    Returns:
        New RelationshipInfo entries with type VALUE_OVERLAP.
    """
    pk_index = _build_pk_index(tables)
    profile_index = _build_profile_index(tables)
    known = _existing_pair_set(existing)
    results: list[RelationshipInfo] = []

    # Collect PK columns with their profiles
    pk_profiles: list[tuple[str, str, ColumnProfile]] = []
    for table in tables:
        for pk_name in pk_index.get(table.name.lower(), []):
            key = (table.name.lower(), pk_name.lower())
            if key in profile_index:
                profile = profile_index[key]
                if profile.sample_values:
                    pk_profiles.append((table.name, pk_name, profile))

    # Compare each non-PK, non-FK column against PK columns
    for table in tables:
        for col in table.columns:
            if col.is_primary_key or col.is_foreign_key:
                continue

            src_key = (table.name.lower(), col.name.lower())
            src_profile = profile_index.get(src_key)
            if not src_profile or not src_profile.sample_values:
                continue

            for tgt_table, tgt_col, tgt_profile in pk_profiles:
                if tgt_table.lower() == table.name.lower():
                    continue  # skip self

                pair = (
                    table.name.lower(),
                    col.name.lower(),
                    tgt_table.lower(),
                    tgt_col.lower(),
                )
                if pair in known:
                    continue

                jaccard = _jaccard_similarity(
                    src_profile.sample_values,
                    tgt_profile.sample_values,
                )

                if jaccard < _JACCARD_THRESHOLD:
                    continue

                # Scale confidence with overlap strength
                confidence = (
                    _VALUE_OVERLAP_CONFIDENCE_HIGH
                    if jaccard >= 0.6
                    else _VALUE_OVERLAP_CONFIDENCE_LOW
                )

                results.append(RelationshipInfo(
                    source_table=table.name,
                    source_column=col.name,
                    target_table=tgt_table,
                    target_column=tgt_col,
                    relationship_type=RelationshipType.VALUE_OVERLAP,
                    confidence=confidence,
                ))
                known.add(pair)
                logger.info(
                    "Value overlap: %s.%s -> %s.%s (jaccard=%.2f)",
                    table.name, col.name, tgt_table, tgt_col, jaccard,
                )

    return results


def discover_relationships(
    tables: list[TableInfo],
    existing_relationships: list[RelationshipInfo] | None = None,
) -> list[RelationshipInfo]:
    """Run all non-FK relationship discovery strategies.

    Combines name matching and value overlap results, deduplicating
    against existing (declared FK) relationships.

    Args:
        tables: Profiled tables with columns and profiles populated.
        existing_relationships: Already-known relationships (e.g., FKs).

    Returns:
        List of newly discovered relationships (does NOT include existing).
    """
    existing = existing_relationships or []

    name_matches = discover_name_matches(tables, existing)
    # Value overlap uses existing + name_matches to avoid duplicates
    all_known = existing + name_matches
    value_matches = discover_value_overlaps(tables, all_known)

    total = name_matches + value_matches
    logger.info(
        "Discovered %d relationships (%d name, %d overlap)",
        len(total), len(name_matches), len(value_matches),
    )
    return total
