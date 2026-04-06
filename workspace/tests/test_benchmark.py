"""Tests for the benchmark harness — loading, comparison, pipeline, reporting."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from dataconnect.benchmark import (
    BenchmarkCase,
    BenchmarkReport,
    CaseResult,
    Difficulty,
    DifficultyStats,
    compare_execution,
    compute_report,
    load_cases,
    normalize_sql,
    run_benchmark,
    run_case,
)
from dataconnect.exceptions import BenchmarkError, RoutingError
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


# ── Fixtures ──────────────────────────────────────────────────────


@pytest.fixture()
def sample_cases_data() -> list[dict]:
    """Raw JSON data for benchmark cases."""
    return [
        {
            "question": "How many employees are there?",
            "SQL": "SELECT COUNT(*) FROM employees",
            "db_id": "testdb",
            "difficulty": "simple",
        },
        {
            "question": "What is the average salary by department?",
            "SQL": (
                "SELECT department, AVG(salary) "
                "FROM employees GROUP BY department"
            ),
            "db_id": "testdb",
            "difficulty": "moderate",
        },
        {
            "question": "Find top earners in each dept with tenure > 5 years",
            "SQL": (
                "SELECT e.name, e.salary, d.name "
                "FROM employees e JOIN departments d "
                "ON e.dept_id = d.id "
                "WHERE e.tenure > 5 "
                "ORDER BY e.salary DESC"
            ),
            "db_id": "testdb",
            "difficulty": "challenging",
        },
    ]


@pytest.fixture()
def cases_file(tmp_path: Path, sample_cases_data: list[dict]) -> Path:
    """Write sample cases to a JSON file."""
    path = tmp_path / "cases.json"
    path.write_text(json.dumps(sample_cases_data), encoding="utf-8")
    return path


@pytest.fixture()
def sample_scan() -> ScanResult:
    """Minimal scan result for benchmark tests."""
    return ScanResult(
        database_name="testdb",
        scanned_at=datetime(2024, 1, 1, tzinfo=UTC),
        tables=[
            TableInfo(
                name="employees",
                columns=[
                    ColumnInfo(
                        name="id",
                        data_type="INTEGER",
                        is_primary_key=True,
                    ),
                    ColumnInfo(name="name", data_type="VARCHAR"),
                    ColumnInfo(name="salary", data_type="DECIMAL"),
                    ColumnInfo(name="department", data_type="VARCHAR"),
                ],
                row_count_estimate=100,
            ),
        ],
        relationships=[],
        token_estimate=500,
    )


@pytest.fixture()
def sample_route() -> RouteResult:
    """Sample route result."""
    return RouteResult(
        query="How many employees?",
        matched_tables=[
            TableMatch(
                table_name="employees",
                methods=[MatchMethod.EMBEDDING],
                relevance_score=0.9,
            ),
        ],
        total_candidates=1,
    )


@pytest.fixture()
def sample_verification() -> VerificationResult:
    """Sample verification result."""
    return VerificationResult(
        sql="SELECT COUNT(*) FROM employees",
        checks=[
            CheckResult(
                check_name="schema_conformity",
                status=CheckStatus.PASSED,
                message="OK",
            ),
        ],
        confidence_score=92.0,
        is_verified=True,
        attempt_number=1,
    )


# ── load_cases Tests ──────────────────────────────────────────────


class TestLoadCases:
    """Tests for loading benchmark cases from JSON."""

    def test_load_valid_file(self, cases_file: Path) -> None:
        """Loads all cases from valid JSON."""
        cases = load_cases(cases_file)
        assert len(cases) == 3
        assert cases[0].question == "How many employees are there?"
        assert cases[0].golden_sql == "SELECT COUNT(*) FROM employees"
        assert cases[0].db_id == "testdb"
        assert cases[0].difficulty == Difficulty.SIMPLE

    def test_load_with_difficulties(self, cases_file: Path) -> None:
        """Parses difficulty levels correctly."""
        cases = load_cases(cases_file)
        assert cases[0].difficulty == Difficulty.SIMPLE
        assert cases[1].difficulty == Difficulty.MODERATE
        assert cases[2].difficulty == Difficulty.CHALLENGING

    def test_file_not_found(self, tmp_path: Path) -> None:
        """Raises BenchmarkError for missing file."""
        with pytest.raises(BenchmarkError, match="not found"):
            load_cases(tmp_path / "nonexistent.json")

    def test_invalid_json(self, tmp_path: Path) -> None:
        """Raises BenchmarkError for malformed JSON."""
        path = tmp_path / "bad.json"
        path.write_text("{not valid json", encoding="utf-8")
        with pytest.raises(BenchmarkError, match="Failed to load"):
            load_cases(path)

    def test_not_array(self, tmp_path: Path) -> None:
        """Raises BenchmarkError if root is not array."""
        path = tmp_path / "obj.json"
        path.write_text('{"key": "value"}', encoding="utf-8")
        with pytest.raises(BenchmarkError, match="JSON array"):
            load_cases(path)

    def test_case_not_object(self, tmp_path: Path) -> None:
        """Raises BenchmarkError if case is not a dict."""
        path = tmp_path / "arr.json"
        path.write_text('["not an object"]', encoding="utf-8")
        with pytest.raises(BenchmarkError, match="not an object"):
            load_cases(path)

    def test_missing_required_field(self, tmp_path: Path) -> None:
        """Raises BenchmarkError if required field missing."""
        path = tmp_path / "missing.json"
        data = [{"question": "test", "db_id": "db"}]
        path.write_text(json.dumps(data), encoding="utf-8")
        with pytest.raises(BenchmarkError, match="missing or invalid"):
            load_cases(path)

    def test_default_difficulty(self, tmp_path: Path) -> None:
        """Missing difficulty defaults to simple."""
        path = tmp_path / "no_diff.json"
        data = [
            {
                "question": "test",
                "SQL": "SELECT 1",
                "db_id": "db",
            },
        ]
        path.write_text(json.dumps(data), encoding="utf-8")
        cases = load_cases(path)
        assert cases[0].difficulty == Difficulty.SIMPLE

    def test_empty_array(self, tmp_path: Path) -> None:
        """Empty array returns empty list."""
        path = tmp_path / "empty.json"
        path.write_text("[]", encoding="utf-8")
        cases = load_cases(path)
        assert cases == []


# ── normalize_sql Tests ───────────────────────────────────────────


class TestNormalizeSql:
    """Tests for SQL normalization."""

    def test_lowercase(self) -> None:
        """Converts to lowercase."""
        assert normalize_sql("SELECT * FROM Users") == "select * from users"

    def test_strip_semicolon(self) -> None:
        """Removes trailing semicolons."""
        assert normalize_sql("SELECT 1;") == "select 1"

    def test_collapse_whitespace(self) -> None:
        """Collapses multiple whitespace to single space."""
        assert normalize_sql("SELECT  *\n  FROM\ttable") == (
            "select * from table"
        )

    def test_strip_outer_whitespace(self) -> None:
        """Strips leading/trailing whitespace."""
        assert normalize_sql("  SELECT 1  ") == "select 1"

    def test_multiple_semicolons(self) -> None:
        """Strips trailing semicolons."""
        result = normalize_sql("SELECT 1;;")
        assert not result.endswith(";")


# ── compare_execution Tests ───────────────────────────────────────


class TestCompareExecution:
    """Tests for execution-based SQL comparison."""

    def test_matching_results(self) -> None:
        """Returns True when both queries produce same results."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__ = (
            lambda s: mock_conn
        )
        mock_engine.connect.return_value.__exit__ = (
            lambda s, *a: None
        )

        rows = [(1,), (2,), (3,)]
        mock_conn.execute.return_value.fetchall.return_value = rows

        assert compare_execution("SELECT a", "SELECT b", mock_engine)

    def test_different_results(self) -> None:
        """Returns False when queries produce different results."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__ = (
            lambda s: mock_conn
        )
        mock_engine.connect.return_value.__exit__ = (
            lambda s, *a: None
        )

        call_count = 0

        def side_effect(*_args, **_kwargs):
            nonlocal call_count
            call_count += 1
            result = MagicMock()
            if call_count == 1:
                result.fetchall.return_value = [(1,)]
            else:
                result.fetchall.return_value = [(2,)]
            return result

        mock_conn.execute.side_effect = side_effect

        assert not compare_execution(
            "SELECT a", "SELECT b", mock_engine,
        )

    def test_execution_error(self) -> None:
        """Raises BenchmarkError on SQL execution failure."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__ = (
            lambda s: mock_conn
        )
        mock_engine.connect.return_value.__exit__ = (
            lambda s, *a: None
        )
        mock_conn.execute.side_effect = RuntimeError("syntax error")

        with pytest.raises(BenchmarkError, match="execution failed"):
            compare_execution("BAD SQL", "SELECT 1", mock_engine)

    def test_order_independent(self) -> None:
        """Matches even if row order differs."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__ = (
            lambda s: mock_conn
        )
        mock_engine.connect.return_value.__exit__ = (
            lambda s, *a: None
        )

        call_count = 0

        def side_effect(*_args, **_kwargs):
            nonlocal call_count
            call_count += 1
            result = MagicMock()
            if call_count == 1:
                result.fetchall.return_value = [(3,), (1,), (2,)]
            else:
                result.fetchall.return_value = [(1,), (2,), (3,)]
            return result

        mock_conn.execute.side_effect = side_effect

        assert compare_execution(
            "SELECT a", "SELECT b", mock_engine,
        )


# ── run_case Tests ────────────────────────────────────────────────


class TestRunCase:
    """Tests for running a single benchmark case."""

    def test_success_without_engine(
        self,
        sample_scan: ScanResult,
        sample_route: RouteResult,
        sample_verification: VerificationResult,
    ) -> None:
        """Runs pipeline and returns result without execution match."""
        case = BenchmarkCase(
            question="How many employees?",
            golden_sql="SELECT COUNT(*) FROM employees",
            db_id="testdb",
        )

        with (
            patch(
                "dataconnect.router.route_query",
                return_value=sample_route,
            ),
            patch(
                "dataconnect.generator.generate_sql",
                return_value="SELECT COUNT(*) FROM employees",
            ),
            patch(
                "dataconnect.verifier.verify_sql",
                return_value=sample_verification,
            ),
        ):
            result = run_case(
                case, sample_scan, "gpt-4o", "sk-test",
            )

        assert result.generated_sql == "SELECT COUNT(*) FROM employees"
        assert result.confidence_score == 92.0
        assert result.confidence_label == "HIGH"
        assert result.is_verified is True
        assert result.execution_match is None
        assert result.error is None
        assert result.elapsed_ms >= 0

    def test_success_with_engine_match(
        self,
        sample_scan: ScanResult,
        sample_route: RouteResult,
        sample_verification: VerificationResult,
    ) -> None:
        """Execution comparison runs when engine provided."""
        case = BenchmarkCase(
            question="How many employees?",
            golden_sql="SELECT COUNT(*) FROM employees",
            db_id="testdb",
        )

        with (
            patch(
                "dataconnect.router.route_query",
                return_value=sample_route,
            ),
            patch(
                "dataconnect.generator.generate_sql",
                return_value="SELECT COUNT(*) FROM employees",
            ),
            patch(
                "dataconnect.verifier.verify_sql",
                return_value=sample_verification,
            ),
            patch(
                "dataconnect.benchmark.compare_execution",
                return_value=True,
            ),
        ):
            result = run_case(
                case,
                sample_scan,
                "gpt-4o",
                "sk-test",
                engine=MagicMock(),
            )

        assert result.execution_match is True

    def test_success_with_engine_mismatch(
        self,
        sample_scan: ScanResult,
        sample_route: RouteResult,
        sample_verification: VerificationResult,
    ) -> None:
        """Execution mismatch sets execution_match=False."""
        case = BenchmarkCase(
            question="How many employees?",
            golden_sql="SELECT COUNT(*) FROM employees",
            db_id="testdb",
        )

        with (
            patch(
                "dataconnect.router.route_query",
                return_value=sample_route,
            ),
            patch(
                "dataconnect.generator.generate_sql",
                return_value="SELECT 1",
            ),
            patch(
                "dataconnect.verifier.verify_sql",
                return_value=sample_verification,
            ),
            patch(
                "dataconnect.benchmark.compare_execution",
                return_value=False,
            ),
        ):
            result = run_case(
                case,
                sample_scan,
                "gpt-4o",
                "sk-test",
                engine=MagicMock(),
            )

        assert result.execution_match is False

    def test_execution_comparison_failure_graceful(
        self,
        sample_scan: ScanResult,
        sample_route: RouteResult,
        sample_verification: VerificationResult,
    ) -> None:
        """Execution failure logged but doesn't crash the case."""
        case = BenchmarkCase(
            question="How many employees?",
            golden_sql="SELECT COUNT(*) FROM employees",
            db_id="testdb",
        )

        with (
            patch(
                "dataconnect.router.route_query",
                return_value=sample_route,
            ),
            patch(
                "dataconnect.generator.generate_sql",
                return_value="SELECT COUNT(*) FROM employees",
            ),
            patch(
                "dataconnect.verifier.verify_sql",
                return_value=sample_verification,
            ),
            patch(
                "dataconnect.benchmark.compare_execution",
                side_effect=BenchmarkError("exec failed"),
            ),
        ):
            result = run_case(
                case,
                sample_scan,
                "gpt-4o",
                "sk-test",
                engine=MagicMock(),
            )

        assert result.execution_match is None
        assert result.error is None

    def test_pipeline_error(
        self,
        sample_scan: ScanResult,
    ) -> None:
        """Pipeline errors captured in CaseResult.error."""
        case = BenchmarkCase(
            question="How many employees?",
            golden_sql="SELECT COUNT(*) FROM employees",
            db_id="testdb",
        )

        with patch(
            "dataconnect.router.route_query",
            side_effect=RoutingError("routing failed"),
        ):
            result = run_case(
                case, sample_scan, "gpt-4o", "sk-test",
            )

        assert result.error == "routing failed"
        assert result.generated_sql == ""
        assert result.elapsed_ms >= 0

    def test_confidence_labels(
        self,
        sample_scan: ScanResult,
        sample_route: RouteResult,
    ) -> None:
        """Different confidence scores produce correct labels."""
        case = BenchmarkCase(
            question="test",
            golden_sql="SELECT 1",
            db_id="testdb",
        )

        labels_by_score = {
            95.0: "HIGH",
            80.0: "MEDIUM",
            55.0: "LOW",
            30.0: "UNVERIFIED",
        }

        for score, expected_label in labels_by_score.items():
            verification = VerificationResult(
                sql="SELECT 1",
                checks=[],
                confidence_score=score,
                is_verified=score >= 90,
            )

            with (
                patch(
                    "dataconnect.router.route_query",
                    return_value=sample_route,
                ),
                patch(
                    "dataconnect.generator.generate_sql",
                    return_value="SELECT 1",
                ),
                patch(
                    "dataconnect.verifier.verify_sql",
                    return_value=verification,
                ),
            ):
                result = run_case(
                    case, sample_scan, "gpt-4o", "sk-test",
                )

            assert result.confidence_label == expected_label, (
                f"Score {score} should be {expected_label}"
            )


# ── compute_report Tests ──────────────────────────────────────────


class TestComputeReport:
    """Tests for aggregate report computation."""

    def test_empty_results(self) -> None:
        """Empty results produce zero-valued report."""
        report = compute_report([])
        assert report.total_cases == 0
        assert report.execution_accuracy == 0.0

    def test_all_correct(self) -> None:
        """All matching cases produce 100% accuracy."""
        case = BenchmarkCase(
            question="q",
            golden_sql="SELECT 1",
            db_id="db",
        )
        results = [
            CaseResult(
                case=case,
                generated_sql="SELECT 1",
                confidence_score=95.0,
                execution_match=True,
                elapsed_ms=100,
            ),
            CaseResult(
                case=case,
                generated_sql="SELECT 1",
                confidence_score=92.0,
                execution_match=True,
                elapsed_ms=200,
            ),
        ]

        report = compute_report(results)
        assert report.total_cases == 2
        assert report.correct == 2
        assert report.incorrect == 0
        assert report.errored == 0
        assert report.execution_accuracy == 100.0

    def test_mixed_results(self) -> None:
        """Mixed correct/incorrect/errored cases."""
        case = BenchmarkCase(
            question="q",
            golden_sql="SELECT 1",
            db_id="db",
        )
        results = [
            CaseResult(
                case=case,
                execution_match=True,
                confidence_score=95.0,
                elapsed_ms=100,
            ),
            CaseResult(
                case=case,
                execution_match=False,
                confidence_score=80.0,
                elapsed_ms=150,
            ),
            CaseResult(
                case=case,
                error="routing failed",
                elapsed_ms=50,
            ),
        ]

        report = compute_report(results)
        assert report.total_cases == 3
        assert report.correct == 1
        assert report.incorrect == 1
        assert report.errored == 1
        assert report.execution_accuracy == pytest.approx(33.3, abs=0.1)

    def test_difficulty_breakdown(self) -> None:
        """Stats computed per difficulty level."""
        simple_case = BenchmarkCase(
            question="q1",
            golden_sql="SELECT 1",
            db_id="db",
            difficulty=Difficulty.SIMPLE,
        )
        moderate_case = BenchmarkCase(
            question="q2",
            golden_sql="SELECT 1",
            db_id="db",
            difficulty=Difficulty.MODERATE,
        )
        results = [
            CaseResult(
                case=simple_case,
                execution_match=True,
                confidence_score=95.0,
                elapsed_ms=100,
            ),
            CaseResult(
                case=simple_case,
                execution_match=True,
                confidence_score=90.0,
                elapsed_ms=120,
            ),
            CaseResult(
                case=moderate_case,
                execution_match=False,
                confidence_score=75.0,
                elapsed_ms=200,
            ),
        ]

        report = compute_report(results)
        assert "simple" in report.by_difficulty
        assert "moderate" in report.by_difficulty
        assert report.by_difficulty["simple"].total == 2
        assert report.by_difficulty["simple"].correct == 2
        assert report.by_difficulty["simple"].accuracy == 100.0
        assert report.by_difficulty["moderate"].total == 1
        assert report.by_difficulty["moderate"].correct == 0
        assert report.by_difficulty["moderate"].accuracy == 0.0

    def test_confidence_calibration(self) -> None:
        """High-confidence calibration computed correctly."""
        case = BenchmarkCase(
            question="q",
            golden_sql="SELECT 1",
            db_id="db",
        )
        results = [
            CaseResult(
                case=case,
                execution_match=True,
                confidence_score=95.0,
                elapsed_ms=100,
            ),
            CaseResult(
                case=case,
                execution_match=False,
                confidence_score=92.0,
                elapsed_ms=150,
            ),
            CaseResult(
                case=case,
                execution_match=True,
                confidence_score=60.0,
                elapsed_ms=200,
            ),
        ]

        report = compute_report(results)
        assert report.high_confidence_total == 2
        assert report.high_confidence_correct == 1
        assert report.calibration_accuracy == 50.0

    def test_avg_confidence_excludes_errors(self) -> None:
        """Average confidence only counts non-errored cases."""
        case = BenchmarkCase(
            question="q",
            golden_sql="SELECT 1",
            db_id="db",
        )
        results = [
            CaseResult(
                case=case,
                confidence_score=90.0,
                elapsed_ms=100,
            ),
            CaseResult(
                case=case,
                error="boom",
                confidence_score=0.0,
                elapsed_ms=50,
            ),
        ]

        report = compute_report(results)
        assert report.avg_confidence == 90.0

    def test_avg_elapsed(self) -> None:
        """Average elapsed includes all cases."""
        case = BenchmarkCase(
            question="q",
            golden_sql="SELECT 1",
            db_id="db",
        )
        results = [
            CaseResult(case=case, elapsed_ms=100),
            CaseResult(case=case, elapsed_ms=200),
        ]

        report = compute_report(results)
        assert report.avg_elapsed_ms == 150.0

    def test_no_execution_match_data(self) -> None:
        """Cases without execution_match count as incorrect."""
        case = BenchmarkCase(
            question="q",
            golden_sql="SELECT 1",
            db_id="db",
        )
        results = [
            CaseResult(
                case=case,
                execution_match=None,
                confidence_score=80.0,
                elapsed_ms=100,
            ),
        ]

        report = compute_report(results)
        assert report.correct == 0
        assert report.incorrect == 1


# ── run_benchmark Tests ───────────────────────────────────────────


class TestRunBenchmark:
    """Tests for the full benchmark orchestrator."""

    def test_missing_scan_result(self) -> None:
        """Raises BenchmarkError if db_id has no scan result."""
        case = BenchmarkCase(
            question="test",
            golden_sql="SELECT 1",
            db_id="unknown_db",
        )

        with pytest.raises(BenchmarkError, match="No scan result"):
            run_benchmark(
                [case], {}, "gpt-4o", "sk-test",
            )

    def test_runs_all_cases(
        self,
        sample_scan: ScanResult,
        sample_route: RouteResult,
        sample_verification: VerificationResult,
    ) -> None:
        """Runs every case and returns report."""
        cases = [
            BenchmarkCase(
                question=f"Question {i}",
                golden_sql="SELECT 1",
                db_id="testdb",
            )
            for i in range(3)
        ]

        with (
            patch(
                "dataconnect.router.route_query",
                return_value=sample_route,
            ),
            patch(
                "dataconnect.generator.generate_sql",
                return_value="SELECT 1",
            ),
            patch(
                "dataconnect.verifier.verify_sql",
                return_value=sample_verification,
            ),
        ):
            report = run_benchmark(
                cases,
                {"testdb": sample_scan},
                "gpt-4o",
                "sk-test",
            )

        assert report.total_cases == 3
        assert len(report.results) == 3

    def test_with_execution_engine(
        self,
        sample_scan: ScanResult,
        sample_route: RouteResult,
        sample_verification: VerificationResult,
    ) -> None:
        """Passes engine to run_case when provided."""
        case = BenchmarkCase(
            question="test",
            golden_sql="SELECT 1",
            db_id="testdb",
        )

        with (
            patch(
                "dataconnect.router.route_query",
                return_value=sample_route,
            ),
            patch(
                "dataconnect.generator.generate_sql",
                return_value="SELECT 1",
            ),
            patch(
                "dataconnect.verifier.verify_sql",
                return_value=sample_verification,
            ),
            patch(
                "dataconnect.benchmark.compare_execution",
                return_value=True,
            ) as mock_compare,
        ):
            mock_engine = MagicMock()
            report = run_benchmark(
                [case],
                {"testdb": sample_scan},
                "gpt-4o",
                "sk-test",
                engines={"testdb": mock_engine},
            )

        mock_compare.assert_called_once()
        assert report.correct == 1

    def test_partial_errors(
        self,
        sample_scan: ScanResult,
        sample_route: RouteResult,
        sample_verification: VerificationResult,
    ) -> None:
        """Some cases can error while others succeed."""
        cases = [
            BenchmarkCase(
                question="good",
                golden_sql="SELECT 1",
                db_id="testdb",
            ),
            BenchmarkCase(
                question="bad",
                golden_sql="SELECT 1",
                db_id="testdb",
            ),
        ]

        call_count = 0

        def mock_route(q, *args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                raise RoutingError("failed")
            return sample_route

        with (
            patch(
                "dataconnect.router.route_query",
                side_effect=mock_route,
            ),
            patch(
                "dataconnect.generator.generate_sql",
                return_value="SELECT 1",
            ),
            patch(
                "dataconnect.verifier.verify_sql",
                return_value=sample_verification,
            ),
        ):
            report = run_benchmark(
                cases,
                {"testdb": sample_scan},
                "gpt-4o",
                "sk-test",
            )

        assert report.total_cases == 2
        assert report.errored == 1
        assert report.results[0].error is None
        assert report.results[1].error == "failed"


# ── Model Tests ───────────────────────────────────────────────────


class TestBenchmarkModels:
    """Tests for benchmark Pydantic models."""

    def test_benchmark_case_defaults(self) -> None:
        """Default difficulty is SIMPLE."""
        case = BenchmarkCase(
            question="test",
            golden_sql="SELECT 1",
            db_id="db",
        )
        assert case.difficulty == Difficulty.SIMPLE

    def test_difficulty_enum_values(self) -> None:
        """All difficulty levels have correct values."""
        assert Difficulty.SIMPLE == "simple"
        assert Difficulty.MODERATE == "moderate"
        assert Difficulty.CHALLENGING == "challenging"

    def test_case_result_defaults(self) -> None:
        """CaseResult has sensible defaults."""
        case = BenchmarkCase(
            question="q",
            golden_sql="SELECT 1",
            db_id="db",
        )
        result = CaseResult(case=case)
        assert result.generated_sql == ""
        assert result.confidence_score == 0.0
        assert result.execution_match is None
        assert result.error is None

    def test_benchmark_report_defaults(self) -> None:
        """BenchmarkReport defaults to zeros."""
        report = BenchmarkReport()
        assert report.total_cases == 0
        assert report.execution_accuracy == 0.0
        assert report.results == []

    def test_difficulty_stats(self) -> None:
        """DifficultyStats holds per-level metrics."""
        stats = DifficultyStats(
            total=10, correct=8, errored=1, accuracy=80.0,
        )
        assert stats.total == 10
        assert stats.accuracy == 80.0
