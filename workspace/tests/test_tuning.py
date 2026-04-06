"""Tests for tuning profiles — presets, loading, validation, integration."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from dataconnect.exceptions import TuningError
from dataconnect.models import CheckResult, CheckStatus
from dataconnect.tuning import (
    PRESETS,
    TuningProfile,
    get_profile,
    load_profile,
)
from dataconnect.verifier import compute_confidence, verify_sql


# ── TuningProfile model tests ───────────────────────────────────


class TestTuningProfileModel:
    """Pydantic model validation for TuningProfile."""

    def test_default_profile_valid(self) -> None:
        """Default constructor produces a valid profile."""
        p = TuningProfile()
        assert p.name == "default"
        assert abs(sum(p.check_weights.values()) - 1.0) < 0.05

    def test_weights_must_sum_to_one(self) -> None:
        """Rejects weights that don't sum to ~1.0."""
        with pytest.raises(ValueError, match="sum to"):
            TuningProfile(check_weights={"a": 0.1, "b": 0.1})

    def test_threshold_range_low(self) -> None:
        """Rejects negative threshold."""
        with pytest.raises(ValueError, match="0-100"):
            TuningProfile(verified_threshold=-1.0)

    def test_threshold_range_high(self) -> None:
        """Rejects threshold > 100."""
        with pytest.raises(ValueError, match="0-100"):
            TuningProfile(verified_threshold=101.0)

    def test_retries_non_negative(self) -> None:
        """Rejects negative retry count."""
        with pytest.raises(ValueError, match=">="):
            TuningProfile(max_retry_attempts=-1)

    def test_top_k_positive(self) -> None:
        """Rejects zero top_k."""
        with pytest.raises(ValueError, match=">="):
            TuningProfile(router_top_k=0)

    def test_custom_name(self) -> None:
        """Custom profile name is stored."""
        p = TuningProfile(name="custom")
        assert p.name == "custom"

    def test_custom_status_scores(self) -> None:
        """Custom status scores are stored."""
        scores = {"PASSED": 100.0, "WARNING": 50.0, "FAILED": 0.0, "SKIPPED": 25.0}
        p = TuningProfile(status_scores=scores)
        assert p.status_scores["WARNING"] == 50.0

    def test_zero_retries_allowed(self) -> None:
        """Zero retries is valid (no retry loop)."""
        p = TuningProfile(max_retry_attempts=0)
        assert p.max_retry_attempts == 0


# ── Preset tests ─────────────────────────────────────────────────


class TestPresets:
    """Built-in preset profiles."""

    def test_default_preset_exists(self) -> None:
        assert "default" in PRESETS

    def test_strict_preset_exists(self) -> None:
        assert "strict" in PRESETS

    def test_lenient_preset_exists(self) -> None:
        assert "lenient" in PRESETS

    def test_all_presets_valid(self) -> None:
        """All presets pass model validation."""
        for name, p in PRESETS.items():
            assert p.name == name
            assert abs(sum(p.check_weights.values()) - 1.0) < 0.05

    def test_strict_higher_threshold(self) -> None:
        """Strict profile has higher verified threshold."""
        assert PRESETS["strict"].verified_threshold > PRESETS["default"].verified_threshold

    def test_lenient_lower_threshold(self) -> None:
        """Lenient profile has lower verified threshold."""
        assert PRESETS["lenient"].verified_threshold < PRESETS["default"].verified_threshold

    def test_strict_lower_warning_score(self) -> None:
        """Strict profile scores warnings lower."""
        strict_warn = PRESETS["strict"].status_scores["WARNING"]
        default_warn = PRESETS["default"].status_scores["WARNING"]
        assert strict_warn < default_warn

    def test_lenient_higher_warning_score(self) -> None:
        """Lenient profile scores warnings higher."""
        lenient_warn = PRESETS["lenient"].status_scores["WARNING"]
        default_warn = PRESETS["default"].status_scores["WARNING"]
        assert lenient_warn > default_warn


# ── load_profile / get_profile tests ─────────────────────────────


class TestLoadProfile:
    """Loading profiles from presets and JSON files."""

    def test_load_preset_by_name(self) -> None:
        """Load 'strict' by name."""
        p = load_profile("strict")
        assert p.name == "strict"

    def test_load_returns_copy(self) -> None:
        """Loading a preset returns a copy, not the original."""
        p1 = load_profile("default")
        p2 = load_profile("default")
        assert p1 is not p2

    def test_unknown_preset_raises(self) -> None:
        """Unknown name and non-existent path raises TuningError."""
        with pytest.raises(TuningError, match="Unknown profile"):
            load_profile("nonexistent_preset_name")

    def test_load_from_json_file(self, tmp_path: Path) -> None:
        """Load profile from a JSON file."""
        data = {
            "name": "from_file",
            "verified_threshold": 60.0,
            "max_retry_attempts": 2,
            "router_top_k": 5,
        }
        path = tmp_path / "profile.json"
        path.write_text(json.dumps(data))
        p = load_profile(str(path))
        assert p.name == "from_file"
        assert p.verified_threshold == 60.0

    def test_load_invalid_json_file(self, tmp_path: Path) -> None:
        """Invalid JSON file raises TuningError."""
        path = tmp_path / "bad.json"
        path.write_text("not json {{{")
        with pytest.raises(TuningError, match="Cannot read"):
            load_profile(str(path))

    def test_load_non_dict_json(self, tmp_path: Path) -> None:
        """JSON array (not object) raises TuningError."""
        path = tmp_path / "arr.json"
        path.write_text("[1, 2, 3]")
        with pytest.raises(TuningError, match="must be a JSON object"):
            load_profile(str(path))

    def test_load_invalid_profile_values(self, tmp_path: Path) -> None:
        """Valid JSON but invalid profile values raises TuningError."""
        data = {"verified_threshold": 200.0}
        path = tmp_path / "invalid.json"
        path.write_text(json.dumps(data))
        with pytest.raises(TuningError, match="Invalid tuning"):
            load_profile(str(path))

    def test_get_profile_none_returns_default(self) -> None:
        """get_profile(None) returns default profile."""
        p = get_profile(None)
        assert p.name == "default"

    def test_get_profile_with_name(self) -> None:
        """get_profile('lenient') returns lenient profile."""
        p = get_profile("lenient")
        assert p.name == "lenient"


# ── Integration: profile → compute_confidence ────────────────────


class TestProfileConfidenceIntegration:
    """TuningProfile affecting confidence scoring."""

    @pytest.fixture()
    def all_passed_checks(self) -> list[CheckResult]:
        return [
            CheckResult(
                check_name="schema_conformity",
                status=CheckStatus.PASSED,
                message="ok",
            ),
            CheckResult(
                check_name="join_validation",
                status=CheckStatus.PASSED,
                message="ok",
            ),
        ]

    @pytest.fixture()
    def mixed_checks(self) -> list[CheckResult]:
        return [
            CheckResult(
                check_name="schema_conformity",
                status=CheckStatus.PASSED,
                message="ok",
            ),
            CheckResult(
                check_name="join_validation",
                status=CheckStatus.WARNING,
                message="type mismatch",
            ),
        ]

    def test_default_weights_produce_expected_score(
        self, all_passed_checks: list[CheckResult],
    ) -> None:
        """Default weights: all PASSED → 100.0."""
        score = compute_confidence(all_passed_checks)
        assert score == 100.0

    def test_custom_weights_change_score(
        self, mixed_checks: list[CheckResult],
    ) -> None:
        """Custom weights alter the confidence score."""
        default_score = compute_confidence(mixed_checks)

        # Heavy weight on the WARNING check
        custom_weights = {
            "schema_conformity": 0.10,
            "join_validation": 0.90,
        }
        custom_score = compute_confidence(
            mixed_checks, weights=custom_weights,
        )
        assert custom_score != default_score

    def test_custom_status_scores_change_result(
        self, mixed_checks: list[CheckResult],
    ) -> None:
        """Custom status scores change confidence."""
        default_score = compute_confidence(mixed_checks)

        harsher = {
            CheckStatus.PASSED: 100.0,
            CheckStatus.WARNING: 20.0,
            CheckStatus.FAILED: 0.0,
            CheckStatus.SKIPPED: 0.0,
        }
        harsh_score = compute_confidence(
            mixed_checks, status_scores=harsher,
        )
        assert harsh_score < default_score

    def test_strict_profile_lowers_mixed_score(
        self, mixed_checks: list[CheckResult],
    ) -> None:
        """Strict profile should yield lower score for mixed results."""
        profile = PRESETS["strict"]
        strict_scores = {
            CheckStatus[k]: v for k, v in profile.status_scores.items()
        }
        strict_score = compute_confidence(
            mixed_checks,
            weights=profile.check_weights,
            status_scores=strict_scores,
        )
        default_score = compute_confidence(mixed_checks)
        assert strict_score <= default_score
