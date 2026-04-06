"""Tests for py.typed marker, __main__.py, and packaging refinements."""

from __future__ import annotations

import importlib
import subprocess
import sys
from pathlib import Path

import pytest

WORKSPACE = Path(__file__).resolve().parent.parent
PACKAGE_DIR = WORKSPACE / "dataconnect"
PYPROJECT = WORKSPACE / "pyproject.toml"


# ── py.typed marker ──────────────────────────────────────────────


class TestPyTyped:
    """Verify py.typed marker file for PEP 561 compliance."""

    def test_py_typed_exists(self) -> None:
        """py.typed marker file exists in package root."""
        assert (PACKAGE_DIR / "py.typed").is_file()

    def test_py_typed_is_empty(self) -> None:
        """py.typed marker file is empty (PEP 561 spec)."""
        content = (PACKAGE_DIR / "py.typed").read_text()
        assert content.strip() == ""

    def test_typing_classifier_in_pyproject(self) -> None:
        """Typing :: Typed classifier is declared in pyproject.toml."""
        if sys.version_info >= (3, 11):
            import tomllib
        else:
            pytest.skip("tomllib requires Python 3.11+")
        with open(PYPROJECT, "rb") as f:
            data = tomllib.load(f)
        classifiers = data["project"]["classifiers"]
        assert "Typing :: Typed" in classifiers


# ── __main__.py ──────────────────────────────────────────────────


class TestMainModule:
    """Verify python -m dataconnect support."""

    def test_main_module_exists(self) -> None:
        """__main__.py exists in package root."""
        assert (PACKAGE_DIR / "__main__.py").is_file()

    def test_main_module_importable(self) -> None:
        """__main__ module can be imported without side effects."""
        mod = importlib.import_module("dataconnect.__main__")
        # cli is imported at module level
        assert hasattr(mod, "cli")
        assert callable(mod.cli)

    def test_main_module_calls_cli(self) -> None:
        """__main__.py imports cli from dataconnect.cli."""
        import ast

        source = (PACKAGE_DIR / "__main__.py").read_text()
        tree = ast.parse(source)
        # Check there's an ImportFrom node importing 'cli'
        imports = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
            and node.module == "dataconnect.cli"
        ]
        assert len(imports) == 1
        names = [alias.name for alias in imports[0].names]
        assert "cli" in names

    def test_python_m_dataconnect_help(self) -> None:
        """python -m dataconnect --help exits cleanly."""
        result = subprocess.run(
            [sys.executable, "-m", "dataconnect", "--help"],
            capture_output=True,
            text=True,
            cwd=str(WORKSPACE),
            timeout=30,
        )
        assert result.returncode == 0
        assert "dataconnect" in result.stdout.lower() or "usage" in result.stdout.lower()


# ── Build backend ────────────────────────────────────────────────


class TestBuildBackend:
    """Verify build-backend uses public API."""

    def test_uses_setuptools_build_meta(self) -> None:
        """Build backend is setuptools.build_meta (not private _legacy)."""
        if sys.version_info >= (3, 11):
            import tomllib
        else:
            pytest.skip("tomllib requires Python 3.11+")
        with open(PYPROJECT, "rb") as f:
            data = tomllib.load(f)
        backend = data["build-system"]["build-backend"]
        assert backend == "setuptools.build_meta"
        assert "_legacy" not in backend


# ── Optional dependencies ────────────────────────────────────────


class TestOptionalDependencies:
    """Verify optional dependency groups."""

    def test_embeddings_extra_exists(self) -> None:
        """[embeddings] optional dependency group exists."""
        if sys.version_info >= (3, 11):
            import tomllib
        else:
            pytest.skip("tomllib requires Python 3.11+")
        with open(PYPROJECT, "rb") as f:
            data = tomllib.load(f)
        assert "embeddings" in data["project"]["optional-dependencies"]

    def test_embeddings_has_sentence_transformers(self) -> None:
        """sentence-transformers is in [embeddings] extras."""
        if sys.version_info >= (3, 11):
            import tomllib
        else:
            pytest.skip("tomllib requires Python 3.11+")
        with open(PYPROJECT, "rb") as f:
            data = tomllib.load(f)
        deps = data["project"]["optional-dependencies"]["embeddings"]
        dep_names = [d.split("==")[0] for d in deps]
        assert "sentence-transformers" in dep_names

    def test_embeddings_deps_pinned(self) -> None:
        """All embeddings dependencies use exact version pinning."""
        if sys.version_info >= (3, 11):
            import tomllib
        else:
            pytest.skip("tomllib requires Python 3.11+")
        with open(PYPROJECT, "rb") as f:
            data = tomllib.load(f)
        for dep in data["project"]["optional-dependencies"]["embeddings"]:
            assert "==" in dep, f"Embeddings dep not pinned: {dep}"

    def test_sentence_transformers_not_in_core(self) -> None:
        """sentence-transformers is NOT in core deps (it's heavy, 500MB+)."""
        if sys.version_info >= (3, 11):
            import tomllib
        else:
            pytest.skip("tomllib requires Python 3.11+")
        with open(PYPROJECT, "rb") as f:
            data = tomllib.load(f)
        dep_names = [d.split("==")[0] for d in data["project"]["dependencies"]]
        assert "sentence-transformers" not in dep_names

    def test_dev_extra_still_exists(self) -> None:
        """[dev] optional dependency group still exists."""
        if sys.version_info >= (3, 11):
            import tomllib
        else:
            pytest.skip("tomllib requires Python 3.11+")
        with open(PYPROJECT, "rb") as f:
            data = tomllib.load(f)
        assert "dev" in data["project"]["optional-dependencies"]
