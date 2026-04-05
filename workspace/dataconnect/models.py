"""Pydantic models for all layer interfaces.

Organized by layer. Split into models/ package when this exceeds 300 lines.
Dependency: models <- scanner <- router <- verifier <- cli
"""

from __future__ import annotations

from datetime import UTC, datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


# ── Scanner Models ──────────────────────────────────────────────────

class ColumnInfo(BaseModel):
    """Single column metadata from schema scan."""

    name: str
    data_type: str
    nullable: bool = True
    is_primary_key: bool = False
    is_foreign_key: bool = False
    foreign_key_target: str | None = None
    description: str = ""


class ColumnProfile(BaseModel):
    """Statistical profile of a column from data sampling."""

    column_name: str
    null_fraction: float = Field(ge=0.0, le=1.0, default=0.0)
    distinct_count: int = Field(ge=0, default=0)
    sample_values: list[str] = Field(default_factory=list, max_length=10)
    min_value: str | None = None
    max_value: str | None = None


class TableInfo(BaseModel):
    """Full table metadata: schema + profiling + description."""

    name: str
    schema_name: str = "public"
    columns: list[ColumnInfo] = Field(default_factory=list)
    row_count_estimate: int = Field(ge=0, default=0)
    profiles: list[ColumnProfile] = Field(default_factory=list)
    description: str = ""


class RelationshipType(str, Enum):
    """How a relationship was discovered."""

    DECLARED_FK = "declared_fk"
    NAME_MATCH = "name_match"
    VALUE_OVERLAP = "value_overlap"
    AI_INFERRED = "ai_inferred"


class RelationshipInfo(BaseModel):
    """Discovered relationship between two columns."""

    source_table: str
    source_column: str
    target_table: str
    target_column: str
    relationship_type: RelationshipType
    confidence: float = Field(ge=0.0, le=1.0)


class ScanResult(BaseModel):
    """Complete scanner output. Stored in SQLite index."""

    database_name: str
    scanned_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    tables: list[TableInfo] = Field(default_factory=list)
    relationships: list[RelationshipInfo] = Field(default_factory=list)
    token_estimate: int = Field(ge=0, default=0)


# ── Router Models ───────────────────────────────────────────────────

class MatchMethod(str, Enum):
    """How a table was matched to a query."""

    EMBEDDING = "embedding"
    GRAPH_WALK = "graph_walk"
    LLM_CROSSCHECK = "llm_crosscheck"


class TableMatch(BaseModel):
    """A table selected by the router with match metadata."""

    table_name: str
    methods: list[MatchMethod] = Field(default_factory=list)
    relevance_score: float = Field(ge=0.0, le=1.0, default=0.0)
    reasoning: str = ""


class RouteResult(BaseModel):
    """Router output: selected tables + reasoning for a query."""

    query: str
    matched_tables: list[TableMatch] = Field(default_factory=list)
    total_candidates: int = Field(ge=0, default=0)


# ── Verifier Models ─────────────────────────────────────────────────

class CheckStatus(str, Enum):
    """Outcome of a single verification check."""

    PASSED = "passed"
    WARNING = "warning"
    FAILED = "failed"
    SKIPPED = "skipped"


class CheckResult(BaseModel):
    """Result from one verification check."""

    check_name: str
    status: CheckStatus
    message: str = ""
    details: dict[str, Any] = Field(default_factory=dict)


class VerificationResult(BaseModel):
    """Aggregated verification output for a generated SQL query."""

    sql: str
    checks: list[CheckResult] = Field(default_factory=list)
    confidence_score: float = Field(ge=0.0, le=100.0, default=0.0)
    is_verified: bool = False
    attempt_number: int = Field(ge=1, default=1)


# ── Final Output ────────────────────────────────────────────────────

class QueryResult(BaseModel):
    """Final output returned to user: SQL + verification + confidence."""

    query: str
    sql: str
    verification: VerificationResult
    route: RouteResult
    execution_time_ms: float = Field(ge=0.0, default=0.0)
