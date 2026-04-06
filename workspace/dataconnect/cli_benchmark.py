"""CLI benchmark command — extracted to keep cli.py under 400 lines.

Registers the 'benchmark' subcommand on the main CLI group.
"""

from __future__ import annotations

from pathlib import Path

import click

from dataconnect.config import sanitize_connection_string
from dataconnect.exceptions import (
    BenchmarkError,
    DataConnectError,
    StorageError,
)


def register_benchmark(cli: click.Group) -> None:
    """Register the benchmark command on the CLI group.

    Args:
        cli: The main Click CLI group.
    """

    @cli.command()
    @click.argument("cases_file", type=click.Path(exists=True))
    @click.option(
        "--db", "-d", required=True,
        help="Database name (from scan).",
    )
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
    @click.option(
        "--connect", "-c",
        default=None,
        help="Connection string for execution accuracy comparison.",
    )
    @click.option(
        "--output", "-o",
        type=click.Path(),
        default=None,
        help="Write JSON report to file.",
    )
    @click.pass_context
    def benchmark(
        ctx: click.Context,
        cases_file: str,
        db: str,
        model: str,
        api_key: str,
        connect: str | None,
        output: str | None,
    ) -> None:
        """Run benchmark cases against a scanned database.

        CASES_FILE is a BIRD-format JSON file with question-SQL pairs.
        """
        from dataconnect.benchmark import load_cases, run_benchmark
        from dataconnect.storage import StorageBackend

        # Load scan result
        try:
            storage = StorageBackend(ctx.obj["storage_dir"])
            scan_result = storage.load_scan(db)
        except StorageError as exc:
            click.echo(f"Storage error: {exc}", err=True)
            raise SystemExit(1) from exc

        if scan_result is None:
            click.echo(
                f"No scan found for database '{db}'. "
                "Run 'dataconnect scan' first.",
                err=True,
            )
            raise SystemExit(1)

        # Load benchmark cases
        try:
            cases = load_cases(Path(cases_file))
        except BenchmarkError as exc:
            click.echo(f"Failed to load cases: {exc}", err=True)
            raise SystemExit(1) from exc

        if not cases:
            click.echo("No benchmark cases found in file.", err=True)
            raise SystemExit(1)

        click.echo(
            f"Running {len(cases)} benchmark cases against '{db}'...",
        )

        # Optional execution engine
        engine = None
        engines = None
        if connect:
            from dataconnect.database import create_readonly_engine

            safe_conn = sanitize_connection_string(connect)
            click.echo(f"Execution comparison enabled: {safe_conn}")
            try:
                engine = create_readonly_engine(connect)
                engines = {db: engine}
            except DataConnectError as exc:
                click.echo(f"Connection failed: {exc}", err=True)
                raise SystemExit(1) from exc

        try:
            report = run_benchmark(
                cases,
                {db: scan_result},
                model,
                api_key,
                engines=engines,
            )
        except BenchmarkError as exc:
            click.echo(f"Benchmark failed: {exc}", err=True)
            raise SystemExit(1) from exc
        finally:
            if engine is not None:
                engine.dispose()

        _display_report(report, output)


def _display_report(
    report: object,
    output: str | None,
) -> None:
    """Display benchmark report to stdout.

    Args:
        report: BenchmarkReport instance.
        output: Optional path for JSON output file.
    """
    click.echo(f"\n{'='*50}")
    click.echo(f"Benchmark Report — {report.total_cases} cases")
    click.echo(f"{'='*50}")
    click.echo(f"  Execution accuracy: {report.execution_accuracy}%")
    click.echo(f"  Correct: {report.correct}")
    click.echo(f"  Incorrect: {report.incorrect}")
    click.echo(f"  Errored: {report.errored}")
    click.echo(f"  Avg confidence: {report.avg_confidence}%")
    click.echo(f"  Avg latency: {report.avg_elapsed_ms:.0f}ms")

    if report.high_confidence_total > 0:
        click.echo(
            f"  Calibration: {report.calibration_accuracy}% "
            f"({report.high_confidence_correct}/"
            f"{report.high_confidence_total} high-conf correct)"
        )

    if report.by_difficulty:
        click.echo("\nBy difficulty:")
        for level, stats in sorted(report.by_difficulty.items()):
            click.echo(
                f"  {level:12s}: {stats.accuracy:5.1f}% "
                f"({stats.correct}/{stats.total})"
            )

    if output:
        report_path = Path(output)
        report_path.write_text(
            report.model_dump_json(indent=2),
            encoding="utf-8",
        )
        click.echo(f"\nReport saved to {report_path}")
