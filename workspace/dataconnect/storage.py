"""SQLite storage interface for the scanner index.

Shared infrastructure — both scanner and router import this.
Scanner writes scan results; router reads them for table selection.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

from sqlalchemy import Column, DateTime, Integer, String, Text, create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from dataconnect.config import STORAGE_DB_NAME
from dataconnect.exceptions import StorageError
from dataconnect.models import ScanResult

logger = logging.getLogger(__name__)


class Base(DeclarativeBase):
    """SQLAlchemy declarative base for storage tables."""


class ScanRecord(Base):
    """Persisted scan result row."""

    __tablename__ = "scan_records"

    id = Column(Integer, primary_key=True, autoincrement=True)
    database_name = Column(String(255), nullable=False, unique=True)
    scanned_at = Column(DateTime, nullable=False)
    data_json = Column(Text, nullable=False)
    token_estimate = Column(Integer, nullable=False, default=0)


class StorageBackend:
    """SQLite-backed storage for scan results."""

    def __init__(self, storage_dir: Path | str) -> None:
        """Initialize storage in the given directory.

        Args:
            storage_dir: Directory where the SQLite file will live.
        """
        self._storage_dir = Path(storage_dir)
        self._storage_dir.mkdir(parents=True, exist_ok=True)
        db_path = self._storage_dir / STORAGE_DB_NAME
        self._engine: Engine = create_engine(f"sqlite:///{db_path}")
        Base.metadata.create_all(self._engine)
        self._session_factory = sessionmaker(bind=self._engine)
        logger.info("Storage initialized at %s", db_path)

    def save_scan(self, result: ScanResult) -> None:
        """Save or update a scan result.

        Args:
            result: The scan result to persist.

        Raises:
            StorageError: If save fails.
        """
        try:
            with self._session_factory() as session:
                existing = (
                    session.query(ScanRecord)
                    .filter(ScanRecord.database_name == result.database_name)
                    .first()
                )
                data_json = result.model_dump_json()
                if existing:
                    existing.scanned_at = result.scanned_at
                    existing.data_json = data_json
                    existing.token_estimate = result.token_estimate
                else:
                    session.add(ScanRecord(
                        database_name=result.database_name,
                        scanned_at=result.scanned_at,
                        data_json=data_json,
                        token_estimate=result.token_estimate,
                    ))
                session.commit()
        except Exception as exc:
            raise StorageError(
                f"Failed to save scan for {result.database_name}: {exc}"
            ) from exc

    def load_scan(self, database_name: str) -> ScanResult | None:
        """Load a scan result by database name.

        Args:
            database_name: Name of the scanned database.

        Returns:
            ScanResult if found, None otherwise.

        Raises:
            StorageError: If load fails.
        """
        try:
            with self._session_factory() as session:
                record = (
                    session.query(ScanRecord)
                    .filter(ScanRecord.database_name == database_name)
                    .first()
                )
                if record is None:
                    return None
                return ScanResult.model_validate_json(record.data_json)
        except Exception as exc:
            raise StorageError(
                f"Failed to load scan for {database_name}: {exc}"
            ) from exc

    def list_databases(self) -> list[str]:
        """List all scanned database names.

        Returns:
            List of database names with stored scans.

        Raises:
            StorageError: If query fails.
        """
        try:
            with self._session_factory() as session:
                rows = session.query(ScanRecord.database_name).all()
                return [row[0] for row in rows]
        except Exception as exc:
            raise StorageError(f"Failed to list databases: {exc}") from exc

    def delete_scan(self, database_name: str) -> bool:
        """Delete a scan result.

        Args:
            database_name: Name of the database to remove.

        Returns:
            True if deleted, False if not found.

        Raises:
            StorageError: If delete fails.
        """
        try:
            with self._session_factory() as session:
                deleted = (
                    session.query(ScanRecord)
                    .filter(ScanRecord.database_name == database_name)
                    .delete()
                )
                session.commit()
                return deleted > 0
        except Exception as exc:
            raise StorageError(
                f"Failed to delete scan for {database_name}: {exc}"
            ) from exc
