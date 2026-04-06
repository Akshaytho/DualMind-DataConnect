"""Tests for CLI interface."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from dataconnect.cli import _confidence_label, cli
from dataconnect.models import (
    CheckResult,
    CheckStatus,
    ColumnInfo,
    MatchMethod,
    RelationshipInfo,
    RelationshipType,
    RouteResult,
    ScanResult,
    TableInfo,
    TableMatch,
    VerificationResult,
)


# ── Fixtures ─────────────────────────────────────────────────────────


@pytest.fixture
def runner() -> CliRunner:
    """Click test runner."""
    return CliRunner()


@pytest.fixture
def sample_scan_result() -> ScanResult:
    """Sample scan result for testing."""
    return ScanResult(
        database_name="testdb",
        scanned_at=datetime(2024, 1, 1, tzinfo=UTC),
        tables=[
            TableInfo(
                name="users",
                columns=[
                    ColumnInfo(name="id", data_type="INTEGER"),
                    ColumnInfo(name="name", data_type="VARCHAR"),
                ],
                row_count_estimate=100,
            ),
            TableInfo(
                name="orders",
                columns=[
                    ColumnInfo(name="id", data_type="INTEGER"),
                    ColumnInfo(name="user_id", data_type="INTEGER"),
                ],
                row_count_estimate=500,
            ),
        ],
        relationships=[
            RelationshipInfo(
                source_table="orders",
                source_column="user_id",
                target_table="users",
                target_column="id",
                relationship_type=RelationshipType.DECLARED_FK,
                confidence=1.0,
            ),
        ],
        token_estimate=1200,
    )


@pytest.fixture
def sample_route_result() -> RouteResult:
    """Sample route result."""
    return RouteResult(
        query="How many users?",
        matched_tables=[
            TableMatch(
                table_name="users",
                methods=[MatchMethod.EMBEDDING],
                relevance_score=0.9,
            ),
        ],
        total_candidates=2,
    )


@pytest.fixture
def sample_verification() -> VerificationResult:
    """Sample verification result."""
    return VerificationResult(
        sql="SELECT COUNT(*) FROM users",
        checks=[
            CheckResult(
                check_name="schema_conformity",
                status=CheckStatus.PASSED,
                message="All tables and columns exist",
            ),
            CheckResult(
                check_name="join_validation",
                status=CheckStatus.PASSED,
                message="No joins to validate",
            ),
        ],
        confidence_score=95.0,
        is_verified=True,
        attempt_number=1,
    )


# ── _confidence_label ─────────────────────────────────────────────


class TestConfidenceLabel:
    """Tests for confidence score labeling."""

    def test_high(self) -> None:
        """90+ is HIGH."""
        assert _confidence_label(95.0) == "HIGH"
        assert _confidence_label(90.0) == "HIGH"

    def test_medium(self) -> None:
        """70-89 is MEDIUM."""
        assert _confidence_label(80.0) == "MEDIUM"
        assert _confidence_label(70.0) == "MEDIUM"

    def test_low(self) -> None:
        """50-69 is LOW."""
        assert _confidence_label(60.0) == "LOW"
        assert _confidence_label(50.0) == "LOW"

    def test_unverified(self) -> None:
        """Below 50 is UNVERIFIED."""
        assert _confidence_label(49.9) == "UNVERIFIED"
        assert _confidence_label(0.0) == "UNVERIFIED"


# ── CLI group ─────────────────────────────────────────────────────


class TestCliGroup:
    """Tests for the main CLI group."""

    def test_help(self, runner: CliRunner) -> None:
        """--help shows usage."""
        result = runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "DataConnect" in result.output

    def test_no_command(self, runner: CliRunner) -> None:
        """No subcommand shows help."""
        result = runner.invoke(cli, [])
        assert result.exit_code == 0

    def test_verbose_flag(self, runner: CliRunner) -> None:
        """--verbose flag accepted."""
        result = runner.invoke(cli, ["--verbose", "--help"])
        assert result.exit_code == 0


# ── scan command ─────────────────────────────────────────────────


class TestScanCommand:
    """Tests for the scan command."""

    def test_scan_help(self, runner: CliRunner) -> None:
        """scan --help works."""
        result = runner.invoke(cli, ["scan", "--help"])
        assert result.exit_code == 0
        assert "CONNECTION_STRING" in result.output

    @patch("dataconnect.storage.StorageBackend")
    @patch("dataconnect.scanner.scan_database")
    @patch("dataconnect.database.create_readonly_engine")
    def test_scan_success(
        self,
        mock_engine: MagicMock,
        mock_scan: MagicMock,
        mock_storage_cls: MagicMock,
        runner: CliRunner,
        sample_scan_result: ScanResult,
    ) -> None:
        """Successful scan prints summary."""
        mock_engine.return_value = MagicMock()
        mock_scan.return_value = sample_scan_result
        mock_storage = MagicMock()
        mock_storage_cls.return_value = mock_storage

        result = runner.invoke(
            cli, ["scan", "sqlite:///test.db"],
        )

        assert result.exit_code == 0
        assert "testdb" in result.output
        assert "Tables: 2" in result.output
        assert "Relationships: 1" in result.output

    @patch("dataconnect.database.create_readonly_engine")
    def test_scan_connection_failure(
        self,
        mock_engine: MagicMock,
        runner: CliRunner,
    ) -> None:
        """Connection failure shows error."""
        from dataconnect.exceptions import DatabaseConnectionError
        mock_engine.side_effect = DatabaseConnectionError("bad url")

        result = runner.invoke(cli, ["scan", "bad://url"])
        assert result.exit_code == 1

    @patch("dataconnect.storage.StorageBackend")
    @patch("dataconnect.scanner.scan_database")
    @patch("dataconnect.database.create_readonly_engine")
    def test_scan_with_name_override(
        self,
        mock_engine: MagicMock,
        mock_scan: MagicMock,
        mock_storage_cls: MagicMock,
        runner: CliRunner,
        sample_scan_result: ScanResult,
    ) -> None:
        """--name flag passed to scan_database."""
        mock_engine.return_value = MagicMock()
        mock_scan.return_value = sample_scan_result
        mock_storage_cls.return_value = MagicMock()

        runner.invoke(
            cli, ["scan", "sqlite:///test.db", "--name", "mydb"],
        )

        mock_scan.assert_called_once()
        assert mock_scan.call_args.kwargs.get("database_name") == "mydb"

    @patch("dataconnect.storage.StorageBackend")
    @patch("dataconnect.scanner.scan_database")
    @patch("dataconnect.database.create_readonly_engine")
    def test_scan_sanitizes_connection_string(
        self,
        mock_engine: MagicMock,
        mock_scan: MagicMock,
        mock_storage_cls: MagicMock,
        runner: CliRunner,
        sample_scan_result: ScanResult,
    ) -> None:
        """Password in connection string is masked in output."""
        mock_engine.return_value = MagicMock()
        mock_scan.return_value = sample_scan_result
        mock_storage_cls.return_value = MagicMock()

        result = runner.invoke(
            cli, ["scan", "postgresql://user:secret123@host/db"],
        )

        assert "secret123" not in result.output
        assert "***" in result.output


# ── ask command ──────────────────────────────────────────────────


class TestAskCommand:
    """Tests for the ask command."""

    def test_ask_help(self, runner: CliRunner) -> None:
        """ask --help works."""
        result = runner.invoke(cli, ["ask", "--help"])
        assert result.exit_code == 0
        assert "QUESTION" in result.output

    def test_ask_requires_db(self, runner: CliRunner) -> None:
        """--db is required."""
        result = runner.invoke(
            cli, ["ask", "question", "--model", "m", "--api-key", "k"],
        )
        assert result.exit_code != 0

    def test_ask_requires_model(self, runner: CliRunner) -> None:
        """--model is required."""
        result = runner.invoke(
            cli, ["ask", "question", "--db", "db", "--api-key", "k"],
        )
        assert result.exit_code != 0

    @patch("dataconnect.verifier.retry.retry_with_fixes")
    @patch("dataconnect.generator.generate_sql")
    @patch("dataconnect.router.route_query")
    @patch("dataconnect.storage.StorageBackend")
    def test_ask_success(
        self,
        mock_storage_cls: MagicMock,
        mock_route: MagicMock,
        mock_generate: MagicMock,
        mock_retry: MagicMock,
        runner: CliRunner,
        sample_scan_result: ScanResult,
        sample_route_result: RouteResult,
        sample_verification: VerificationResult,
    ) -> None:
        """Successful ask pipeline."""
        mock_storage = MagicMock()
        mock_storage.load_scan.return_value = sample_scan_result
        mock_storage_cls.return_value = mock_storage
        mock_route.return_value = sample_route_result
        mock_generate.return_value = "SELECT COUNT(*) FROM users"
        mock_retry.return_value = sample_verification

        result = runner.invoke(
            cli,
            ["ask", "How many users?", "--db", "testdb",
             "--model", "gpt-4o", "--api-key", "test-key"],
        )

        assert result.exit_code == 0
        assert "SELECT COUNT(*) FROM users" in result.output
        assert "HIGH" in result.output
        assert "95%" in result.output

    @patch("dataconnect.storage.StorageBackend")
    def test_ask_no_scan_result(
        self,
        mock_storage_cls: MagicMock,
        runner: CliRunner,
    ) -> None:
        """Missing scan result shows error."""
        mock_storage = MagicMock()
        mock_storage.load_scan.return_value = None
        mock_storage_cls.return_value = mock_storage

        result = runner.invoke(
            cli,
            ["ask", "q", "--db", "nonexistent",
             "--model", "m", "--api-key", "k"],
        )

        assert result.exit_code == 1
        assert "No scan found" in result.output

    @patch("dataconnect.verifier.verify_sql")
    @patch("dataconnect.generator.generate_sql")
    @patch("dataconnect.router.route_query")
    @patch("dataconnect.storage.StorageBackend")
    def test_ask_no_retry_flag(
        self,
        mock_storage_cls: MagicMock,
        mock_route: MagicMock,
        mock_generate: MagicMock,
        mock_verify: MagicMock,
        runner: CliRunner,
        sample_scan_result: ScanResult,
        sample_route_result: RouteResult,
        sample_verification: VerificationResult,
    ) -> None:
        """--no-retry skips retry loop."""
        mock_storage = MagicMock()
        mock_storage.load_scan.return_value = sample_scan_result
        mock_storage_cls.return_value = mock_storage
        mock_route.return_value = sample_route_result
        mock_generate.return_value = "SELECT 1"
        mock_verify.return_value = sample_verification

        result = runner.invoke(
            cli,
            ["ask", "q", "--db", "testdb",
             "--model", "m", "--api-key", "k", "--no-retry"],
        )

        assert result.exit_code == 0
        mock_verify.assert_called_once()

    @patch("dataconnect.verifier.retry.retry_with_fixes")
    @patch("dataconnect.generator.generate_sql")
    @patch("dataconnect.router.route_query")
    @patch("dataconnect.storage.StorageBackend")
    def test_ask_unverified_warning(
        self,
        mock_storage_cls: MagicMock,
        mock_route: MagicMock,
        mock_generate: MagicMock,
        mock_retry: MagicMock,
        runner: CliRunner,
        sample_scan_result: ScanResult,
        sample_route_result: RouteResult,
    ) -> None:
        """Unverified result shows WARNING."""
        mock_storage = MagicMock()
        mock_storage.load_scan.return_value = sample_scan_result
        mock_storage_cls.return_value = mock_storage
        mock_route.return_value = sample_route_result
        mock_generate.return_value = "SELECT 1"
        mock_retry.return_value = VerificationResult(
            sql="SELECT 1",
            checks=[
                CheckResult(
                    check_name="schema_conformity",
                    status=CheckStatus.FAILED,
                    message="Table not found",
                ),
            ],
            confidence_score=30.0,
            is_verified=False,
        )

        result = runner.invoke(
            cli,
            ["ask", "q", "--db", "testdb",
             "--model", "m", "--api-key", "k"],
        )

        assert result.exit_code == 0
        assert "WARNING" in result.output or "UNVERIFIED" in result.output

    @patch("dataconnect.router.route_query")
    @patch("dataconnect.storage.StorageBackend")
    def test_ask_routing_failure(
        self,
        mock_storage_cls: MagicMock,
        mock_route: MagicMock,
        runner: CliRunner,
        sample_scan_result: ScanResult,
    ) -> None:
        """Routing failure shows error."""
        from dataconnect.exceptions import RoutingError
        mock_storage = MagicMock()
        mock_storage.load_scan.return_value = sample_scan_result
        mock_storage_cls.return_value = mock_storage
        mock_route.side_effect = RoutingError("No tables")

        result = runner.invoke(
            cli,
            ["ask", "q", "--db", "testdb",
             "--model", "m", "--api-key", "k"],
        )

        assert result.exit_code == 1
        assert "Routing failed" in result.output


# ── list command ─────────────────────────────────────────────────


class TestListCommand:
    """Tests for the list command."""

    def test_list_help(self, runner: CliRunner) -> None:
        """list --help works."""
        result = runner.invoke(cli, ["list", "--help"])
        assert result.exit_code == 0

    @patch("dataconnect.storage.StorageBackend")
    def test_list_databases(
        self,
        mock_storage_cls: MagicMock,
        runner: CliRunner,
    ) -> None:
        """Lists scanned databases."""
        mock_storage = MagicMock()
        mock_storage.list_databases.return_value = ["db1", "db2"]
        mock_storage_cls.return_value = mock_storage

        result = runner.invoke(cli, ["list"])

        assert result.exit_code == 0
        assert "db1" in result.output
        assert "db2" in result.output
        assert "2" in result.output

    @patch("dataconnect.storage.StorageBackend")
    def test_list_empty(
        self,
        mock_storage_cls: MagicMock,
        runner: CliRunner,
    ) -> None:
        """Empty database list shows message."""
        mock_storage = MagicMock()
        mock_storage.list_databases.return_value = []
        mock_storage_cls.return_value = mock_storage

        result = runner.invoke(cli, ["list"])

        assert result.exit_code == 0
        assert "No scanned databases" in result.output


# ── info command ─────────────────────────────────────────────────


class TestInfoCommand:
    """Tests for the info command."""

    def test_info_help(self, runner: CliRunner) -> None:
        """info --help works."""
        result = runner.invoke(cli, ["info", "--help"])
        assert result.exit_code == 0

    @patch("dataconnect.storage.StorageBackend")
    def test_info_success(
        self,
        mock_storage_cls: MagicMock,
        runner: CliRunner,
        sample_scan_result: ScanResult,
    ) -> None:
        """Shows database info."""
        mock_storage = MagicMock()
        mock_storage.load_scan.return_value = sample_scan_result
        mock_storage_cls.return_value = mock_storage

        result = runner.invoke(cli, ["info", "testdb"])

        assert result.exit_code == 0
        assert "testdb" in result.output
        assert "Tables: 2" in result.output
        assert "Relationships: 1" in result.output
        assert "users" in result.output
        assert "orders" in result.output

    @patch("dataconnect.storage.StorageBackend")
    def test_info_not_found(
        self,
        mock_storage_cls: MagicMock,
        runner: CliRunner,
    ) -> None:
        """Missing database shows error."""
        mock_storage = MagicMock()
        mock_storage.load_scan.return_value = None
        mock_storage_cls.return_value = mock_storage

        result = runner.invoke(cli, ["info", "nonexistent"])

        assert result.exit_code == 1
        assert "No scan found" in result.output

    @patch("dataconnect.storage.StorageBackend")
    def test_info_shows_table_details(
        self,
        mock_storage_cls: MagicMock,
        runner: CliRunner,
        sample_scan_result: ScanResult,
    ) -> None:
        """Info shows column count and row estimates."""
        mock_storage = MagicMock()
        mock_storage.load_scan.return_value = sample_scan_result
        mock_storage_cls.return_value = mock_storage

        result = runner.invoke(cli, ["info", "testdb"])

        assert "2 columns" in result.output
        assert "100" in result.output  # row count


# ── benchmark command ────────────────────────────────────────────


@pytest.fixture
def cases_json_file(tmp_path: Path) -> Path:
    """Write sample BIRD-format cases to a temp JSON file."""
    data = [
        {
            "question": "How many users?",
            "SQL": "SELECT COUNT(*) FROM users",
            "db_id": "testdb",
            "difficulty": "simple",
        },
        {
            "question": "Average order total?",
            "SQL": "SELECT AVG(total) FROM orders",
            "db_id": "testdb",
            "difficulty": "moderate",
        },
    ]
    path = tmp_path / "cases.json"
    path.write_text(json.dumps(data), encoding="utf-8")
    return path


class TestBenchmarkCommand:
    """Tests for the benchmark command."""

    def test_benchmark_help(self, runner: CliRunner) -> None:
        """benchmark --help works."""
        result = runner.invoke(cli, ["benchmark", "--help"])
        assert result.exit_code == 0
        assert "CASES_FILE" in result.output

    def test_benchmark_requires_db(
        self,
        runner: CliRunner,
        cases_json_file: Path,
    ) -> None:
        """--db is required."""
        result = runner.invoke(
            cli,
            ["benchmark", str(cases_json_file),
             "--model", "m", "--api-key", "k"],
        )
        assert result.exit_code != 0

    def test_benchmark_requires_model(
        self,
        runner: CliRunner,
        cases_json_file: Path,
    ) -> None:
        """--model is required."""
        result = runner.invoke(
            cli,
            ["benchmark", str(cases_json_file),
             "--db", "testdb", "--api-key", "k"],
        )
        assert result.exit_code != 0

    @patch("dataconnect.storage.StorageBackend")
    def test_benchmark_no_scan(
        self,
        mock_storage_cls: MagicMock,
        runner: CliRunner,
        cases_json_file: Path,
    ) -> None:
        """Missing scan result shows error."""
        mock_storage = MagicMock()
        mock_storage.load_scan.return_value = None
        mock_storage_cls.return_value = mock_storage

        result = runner.invoke(
            cli,
            ["benchmark", str(cases_json_file),
             "--db", "testdb", "--model", "m", "--api-key", "k"],
        )

        assert result.exit_code == 1
        assert "No scan found" in result.output

    @patch("dataconnect.benchmark.run_benchmark")
    @patch("dataconnect.benchmark.load_cases")
    @patch("dataconnect.storage.StorageBackend")
    def test_benchmark_success(
        self,
        mock_storage_cls: MagicMock,
        mock_load: MagicMock,
        mock_run: MagicMock,
        runner: CliRunner,
        sample_scan_result: ScanResult,
        cases_json_file: Path,
    ) -> None:
        """Successful benchmark prints report."""
        from dataconnect.benchmark import (
            BenchmarkCase,
            BenchmarkReport,
            Difficulty,
            DifficultyStats,
        )

        mock_storage = MagicMock()
        mock_storage.load_scan.return_value = sample_scan_result
        mock_storage_cls.return_value = mock_storage

        mock_load.return_value = [
            BenchmarkCase(
                question="q", golden_sql="SELECT 1",
                db_id="testdb", difficulty=Difficulty.SIMPLE,
            ),
        ]
        mock_run.return_value = BenchmarkReport(
            total_cases=1,
            correct=1,
            incorrect=0,
            errored=0,
            execution_accuracy=100.0,
            avg_confidence=95.0,
            avg_elapsed_ms=150.0,
            by_difficulty={
                "simple": DifficultyStats(
                    total=1, correct=1, accuracy=100.0,
                ),
            },
            high_confidence_correct=1,
            high_confidence_total=1,
            calibration_accuracy=100.0,
        )

        result = runner.invoke(
            cli,
            ["benchmark", str(cases_json_file),
             "--db", "testdb", "--model", "gpt-4o", "--api-key", "k"],
        )

        assert result.exit_code == 0
        assert "Benchmark Report" in result.output
        assert "100.0%" in result.output
        assert "simple" in result.output

    @patch("dataconnect.benchmark.run_benchmark")
    @patch("dataconnect.benchmark.load_cases")
    @patch("dataconnect.storage.StorageBackend")
    def test_benchmark_output_file(
        self,
        mock_storage_cls: MagicMock,
        mock_load: MagicMock,
        mock_run: MagicMock,
        runner: CliRunner,
        sample_scan_result: ScanResult,
        cases_json_file: Path,
        tmp_path: Path,
    ) -> None:
        """--output writes JSON report."""
        from dataconnect.benchmark import (
            BenchmarkCase,
            BenchmarkReport,
        )

        mock_storage = MagicMock()
        mock_storage.load_scan.return_value = sample_scan_result
        mock_storage_cls.return_value = mock_storage

        mock_load.return_value = [
            BenchmarkCase(
                question="q", golden_sql="SELECT 1",
                db_id="testdb",
            ),
        ]
        mock_run.return_value = BenchmarkReport(
            total_cases=1, correct=1, execution_accuracy=100.0,
            avg_confidence=90.0, avg_elapsed_ms=100.0,
        )

        output_path = tmp_path / "report.json"
        result = runner.invoke(
            cli,
            ["benchmark", str(cases_json_file),
             "--db", "testdb", "--model", "m", "--api-key", "k",
             "--output", str(output_path)],
        )

        assert result.exit_code == 0
        assert output_path.exists()
        report_data = json.loads(output_path.read_text())
        assert report_data["total_cases"] == 1
        assert "Report saved" in result.output

    @patch("dataconnect.benchmark.run_benchmark")
    @patch("dataconnect.benchmark.load_cases")
    @patch("dataconnect.database.create_readonly_engine")
    @patch("dataconnect.storage.StorageBackend")
    def test_benchmark_with_connect(
        self,
        mock_storage_cls: MagicMock,
        mock_engine: MagicMock,
        mock_load: MagicMock,
        mock_run: MagicMock,
        runner: CliRunner,
        sample_scan_result: ScanResult,
        cases_json_file: Path,
    ) -> None:
        """--connect enables execution comparison."""
        from dataconnect.benchmark import (
            BenchmarkCase,
            BenchmarkReport,
        )

        mock_storage = MagicMock()
        mock_storage.load_scan.return_value = sample_scan_result
        mock_storage_cls.return_value = mock_storage
        mock_engine.return_value = MagicMock()

        mock_load.return_value = [
            BenchmarkCase(
                question="q", golden_sql="SELECT 1",
                db_id="testdb",
            ),
        ]
        mock_run.return_value = BenchmarkReport(
            total_cases=1, correct=1, execution_accuracy=100.0,
            avg_confidence=90.0, avg_elapsed_ms=100.0,
        )

        result = runner.invoke(
            cli,
            ["benchmark", str(cases_json_file),
             "--db", "testdb", "--model", "m", "--api-key", "k",
             "--connect", "sqlite:///test.db"],
        )

        assert result.exit_code == 0
        assert "Execution comparison enabled" in result.output
        # Engine passed to run_benchmark
        call_kwargs = mock_run.call_args
        assert call_kwargs.kwargs.get("engines") is not None

    @patch("dataconnect.benchmark.run_benchmark")
    @patch("dataconnect.benchmark.load_cases")
    @patch("dataconnect.database.create_readonly_engine")
    @patch("dataconnect.storage.StorageBackend")
    def test_benchmark_connect_sanitizes_password(
        self,
        mock_storage_cls: MagicMock,
        mock_engine: MagicMock,
        mock_load: MagicMock,
        mock_run: MagicMock,
        runner: CliRunner,
        sample_scan_result: ScanResult,
        cases_json_file: Path,
    ) -> None:
        """--connect masks password in output."""
        from dataconnect.benchmark import (
            BenchmarkCase,
            BenchmarkReport,
        )

        mock_storage = MagicMock()
        mock_storage.load_scan.return_value = sample_scan_result
        mock_storage_cls.return_value = mock_storage
        mock_engine.return_value = MagicMock()

        mock_load.return_value = [
            BenchmarkCase(
                question="q", golden_sql="SELECT 1",
                db_id="testdb",
            ),
        ]
        mock_run.return_value = BenchmarkReport(
            total_cases=1, avg_confidence=90.0, avg_elapsed_ms=100.0,
        )

        result = runner.invoke(
            cli,
            ["benchmark", str(cases_json_file),
             "--db", "testdb", "--model", "m", "--api-key", "k",
             "--connect", "postgresql://user:secret@host/db"],
        )

        assert "secret" not in result.output
        assert "***" in result.output

    @patch("dataconnect.benchmark.load_cases")
    @patch("dataconnect.storage.StorageBackend")
    def test_benchmark_bad_cases_file(
        self,
        mock_storage_cls: MagicMock,
        mock_load: MagicMock,
        runner: CliRunner,
        sample_scan_result: ScanResult,
        cases_json_file: Path,
    ) -> None:
        """Invalid cases file shows error."""
        from dataconnect.exceptions import BenchmarkError

        mock_storage = MagicMock()
        mock_storage.load_scan.return_value = sample_scan_result
        mock_storage_cls.return_value = mock_storage
        mock_load.side_effect = BenchmarkError("bad format")

        result = runner.invoke(
            cli,
            ["benchmark", str(cases_json_file),
             "--db", "testdb", "--model", "m", "--api-key", "k"],
        )

        assert result.exit_code == 1
        assert "Failed to load cases" in result.output

    @patch("dataconnect.benchmark.load_cases")
    @patch("dataconnect.storage.StorageBackend")
    def test_benchmark_empty_cases(
        self,
        mock_storage_cls: MagicMock,
        mock_load: MagicMock,
        runner: CliRunner,
        sample_scan_result: ScanResult,
        cases_json_file: Path,
    ) -> None:
        """Empty cases file shows error."""
        mock_storage = MagicMock()
        mock_storage.load_scan.return_value = sample_scan_result
        mock_storage_cls.return_value = mock_storage
        mock_load.return_value = []

        result = runner.invoke(
            cli,
            ["benchmark", str(cases_json_file),
             "--db", "testdb", "--model", "m", "--api-key", "k"],
        )

        assert result.exit_code == 1
        assert "No benchmark cases" in result.output

    @patch("dataconnect.benchmark.run_benchmark")
    @patch("dataconnect.benchmark.load_cases")
    @patch("dataconnect.storage.StorageBackend")
    def test_benchmark_calibration_display(
        self,
        mock_storage_cls: MagicMock,
        mock_load: MagicMock,
        mock_run: MagicMock,
        runner: CliRunner,
        sample_scan_result: ScanResult,
        cases_json_file: Path,
    ) -> None:
        """Calibration stats shown when high-conf cases exist."""
        from dataconnect.benchmark import (
            BenchmarkCase,
            BenchmarkReport,
        )

        mock_storage = MagicMock()
        mock_storage.load_scan.return_value = sample_scan_result
        mock_storage_cls.return_value = mock_storage

        mock_load.return_value = [
            BenchmarkCase(
                question="q", golden_sql="SELECT 1",
                db_id="testdb",
            ),
        ]
        mock_run.return_value = BenchmarkReport(
            total_cases=2, correct=1,
            execution_accuracy=50.0,
            avg_confidence=85.0, avg_elapsed_ms=200.0,
            high_confidence_correct=1,
            high_confidence_total=2,
            calibration_accuracy=50.0,
        )

        result = runner.invoke(
            cli,
            ["benchmark", str(cases_json_file),
             "--db", "testdb", "--model", "m", "--api-key", "k"],
        )

        assert result.exit_code == 0
        assert "Calibration" in result.output
        assert "1/2" in result.output

    @patch("dataconnect.benchmark.run_benchmark")
    @patch("dataconnect.benchmark.load_cases")
    @patch("dataconnect.storage.StorageBackend")
    def test_benchmark_run_error(
        self,
        mock_storage_cls: MagicMock,
        mock_load: MagicMock,
        mock_run: MagicMock,
        runner: CliRunner,
        sample_scan_result: ScanResult,
        cases_json_file: Path,
    ) -> None:
        """Benchmark runtime error shows error message."""
        from dataconnect.benchmark import BenchmarkCase
        from dataconnect.exceptions import BenchmarkError

        mock_storage = MagicMock()
        mock_storage.load_scan.return_value = sample_scan_result
        mock_storage_cls.return_value = mock_storage

        mock_load.return_value = [
            BenchmarkCase(
                question="q", golden_sql="SELECT 1",
                db_id="testdb",
            ),
        ]
        mock_run.side_effect = BenchmarkError("missing scan")

        result = runner.invoke(
            cli,
            ["benchmark", str(cases_json_file),
             "--db", "testdb", "--model", "m", "--api-key", "k"],
        )

        assert result.exit_code == 1
        assert "Benchmark failed" in result.output
