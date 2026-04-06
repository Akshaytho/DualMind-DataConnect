"""Tests for packaging configuration — pyproject.toml validation."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

# ── Path constants ──────────────────────────────────────────────

WORKSPACE = Path(__file__).resolve().parent.parent
PYPROJECT = WORKSPACE / "pyproject.toml"


# ── pyproject.toml existence and basic structure ────────────────


class TestPyprojectExists:
    """Verify pyproject.toml exists and is parseable."""

    def test_file_exists(self) -> None:
        """pyproject.toml exists in workspace root."""
        assert PYPROJECT.is_file()

    def test_parseable(self) -> None:
        """pyproject.toml is valid TOML."""
        if sys.version_info >= (3, 11):
            import tomllib
        else:
            pytest.skip("tomllib requires Python 3.11+")
        with open(PYPROJECT, "rb") as f:
            data = tomllib.load(f)
        assert isinstance(data, dict)


# ── Metadata validation ─────────────────────────────────────────


def _load_pyproject() -> dict:
    """Load pyproject.toml as a dict."""
    if sys.version_info >= (3, 11):
        import tomllib
    else:
        pytest.skip("tomllib requires Python 3.11+")
    with open(PYPROJECT, "rb") as f:
        return tomllib.load(f)


class TestProjectMetadata:
    """Validate [project] section."""

    def test_name_is_dataconnect(self) -> None:
        """Package name matches project."""
        data = _load_pyproject()
        assert data["project"]["name"] == "dataconnect"

    def test_version_matches_init(self) -> None:
        """Version in pyproject.toml matches __init__.__version__."""
        data = _load_pyproject()
        from dataconnect import __version__

        assert data["project"]["version"] == __version__

    def test_requires_python_311(self) -> None:
        """Requires Python 3.11+."""
        data = _load_pyproject()
        assert ">=3.11" in data["project"]["requires-python"]

    def test_has_description(self) -> None:
        """Has a non-empty description."""
        data = _load_pyproject()
        assert len(data["project"]["description"]) > 10

    def test_has_license(self) -> None:
        """License field is set."""
        data = _load_pyproject()
        assert data["project"]["license"] == "MIT"


# ── Dependencies ─────────────────────────────────────────────────


class TestDependencies:
    """Validate dependency pinning."""

    def test_all_deps_pinned(self) -> None:
        """Every dependency uses exact version pinning (==)."""
        data = _load_pyproject()
        for dep in data["project"]["dependencies"]:
            assert "==" in dep, f"Dependency not pinned: {dep}"

    def test_core_deps_present(self) -> None:
        """Required runtime dependencies are listed."""
        data = _load_pyproject()
        dep_names = [d.split("==")[0] for d in data["project"]["dependencies"]]
        required = [
            "pydantic",
            "sqlalchemy",
            "numpy",
            "networkx",
            "litellm",
            "sqlparse",
            "click",
            "fastapi",
            "uvicorn",
        ]
        for name in required:
            assert name in dep_names, f"Missing dependency: {name}"

    def test_dev_deps_pinned(self) -> None:
        """Dev dependencies also use exact version pinning."""
        data = _load_pyproject()
        for dep in data["project"]["optional-dependencies"]["dev"]:
            assert "==" in dep, f"Dev dependency not pinned: {dep}"

    def test_dev_deps_include_pytest(self) -> None:
        """Dev dependencies include pytest."""
        data = _load_pyproject()
        dev_names = [
            d.split("==")[0]
            for d in data["project"]["optional-dependencies"]["dev"]
        ]
        assert "pytest" in dev_names

    def test_no_test_deps_in_core(self) -> None:
        """Test-only deps (pytest, hypothesis, httpx) not in core deps."""
        data = _load_pyproject()
        dep_names = [d.split("==")[0] for d in data["project"]["dependencies"]]
        test_only = ["pytest", "hypothesis", "httpx"]
        for name in test_only:
            assert name not in dep_names, f"Test dep in core: {name}"


# ── Entry points ─────────────────────────────────────────────────


class TestEntryPoints:
    """Validate console_scripts entry point."""

    def test_has_console_script(self) -> None:
        """Defines a dataconnect console_scripts entry."""
        data = _load_pyproject()
        scripts = data["project"]["scripts"]
        assert "dataconnect" in scripts

    def test_entry_point_target(self) -> None:
        """Console script points to cli:cli."""
        data = _load_pyproject()
        assert data["project"]["scripts"]["dataconnect"] == "dataconnect.cli:cli"

    def test_cli_module_importable(self) -> None:
        """The CLI entry point module is importable."""
        mod = importlib.import_module("dataconnect.cli")
        assert hasattr(mod, "cli")
        assert callable(mod.cli)


# ── Build system ─────────────────────────────────────────────────


class TestBuildSystem:
    """Validate [build-system] section."""

    def test_has_build_backend(self) -> None:
        """Build backend is specified."""
        data = _load_pyproject()
        assert "build-backend" in data["build-system"]

    def test_has_build_requires(self) -> None:
        """Build requirements are specified."""
        data = _load_pyproject()
        requires = data["build-system"]["requires"]
        assert len(requires) > 0

    def test_setuptools_package_discovery(self) -> None:
        """Setuptools finds dataconnect packages."""
        data = _load_pyproject()
        includes = data["tool"]["setuptools"]["packages"]["find"]["include"]
        assert "dataconnect*" in includes


# ── Pytest config ────────────────────────────────────────────────


class TestPytestConfig:
    """Validate [tool.pytest.ini_options]."""

    def test_testpaths_set(self) -> None:
        """Test paths configured."""
        data = _load_pyproject()
        assert "tests" in data["tool"]["pytest"]["ini_options"]["testpaths"]

    def test_pythonpath_set(self) -> None:
        """Python path includes workspace root."""
        data = _load_pyproject()
        assert "." in data["tool"]["pytest"]["ini_options"]["pythonpath"]
