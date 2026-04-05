"""Tests for storage module — save, load, list, delete scan results."""

from __future__ import annotations

from pathlib import Path

import pytest

from dataconnect.exceptions import StorageError
from dataconnect.models import ScanResult, TableInfo
from dataconnect.storage import StorageBackend


class TestStorageBackend:
    """Tests for StorageBackend CRUD operations."""

    def test_save_and_load(
        self, storage_backend: StorageBackend, sample_scan_result: ScanResult
    ) -> None:
        storage_backend.save_scan(sample_scan_result)
        loaded = storage_backend.load_scan("test_db")
        assert loaded is not None
        assert loaded.database_name == "test_db"
        assert len(loaded.tables) == 2
        assert len(loaded.relationships) == 1

    def test_load_missing(self, storage_backend: StorageBackend) -> None:
        result = storage_backend.load_scan("nonexistent")
        assert result is None

    def test_update_existing(
        self, storage_backend: StorageBackend, sample_scan_result: ScanResult
    ) -> None:
        storage_backend.save_scan(sample_scan_result)

        updated = sample_scan_result.model_copy(
            update={"token_estimate": 999}
        )
        storage_backend.save_scan(updated)

        loaded = storage_backend.load_scan("test_db")
        assert loaded is not None
        assert loaded.token_estimate == 999

    def test_list_databases(
        self, storage_backend: StorageBackend, sample_scan_result: ScanResult
    ) -> None:
        assert storage_backend.list_databases() == []

        storage_backend.save_scan(sample_scan_result)
        assert storage_backend.list_databases() == ["test_db"]

        scan2 = ScanResult(database_name="other_db", tables=[], relationships=[])
        storage_backend.save_scan(scan2)
        dbs = storage_backend.list_databases()
        assert len(dbs) == 2
        assert "test_db" in dbs
        assert "other_db" in dbs

    def test_delete_existing(
        self, storage_backend: StorageBackend, sample_scan_result: ScanResult
    ) -> None:
        storage_backend.save_scan(sample_scan_result)
        assert storage_backend.delete_scan("test_db") is True
        assert storage_backend.load_scan("test_db") is None

    def test_delete_missing(self, storage_backend: StorageBackend) -> None:
        assert storage_backend.delete_scan("nonexistent") is False

    def test_roundtrip_preserves_relationships(
        self, storage_backend: StorageBackend, sample_scan_result: ScanResult
    ) -> None:
        storage_backend.save_scan(sample_scan_result)
        loaded = storage_backend.load_scan("test_db")
        assert loaded is not None
        rel = loaded.relationships[0]
        assert rel.source_table == "orders"
        assert rel.target_table == "users"
        assert rel.confidence == 1.0

    def test_creates_storage_dir(self, tmp_path: Path) -> None:
        new_dir = tmp_path / "nested" / "deep" / "storage"
        backend = StorageBackend(new_dir)
        assert new_dir.exists()
