from __future__ import annotations

import json
from pathlib import Path

import pytest

from tinybird_sdk.cli.config import _resolve_branch_data_mode, load_config


def test_branch_data_mode_last_partition() -> None:
    mode, explicit = _resolve_branch_data_mode({"branch_data_mode": "last_partition"})
    assert mode == "last_partition"
    assert explicit is True


def test_branch_data_mode_missing_defaults_to_last_partition() -> None:
    mode, explicit = _resolve_branch_data_mode({})
    assert mode == "last_partition"
    assert explicit is False


def test_branch_data_mode_empty_defaults_to_last_partition() -> None:
    mode, explicit = _resolve_branch_data_mode({"branch_data_mode": "   "})
    assert mode == "last_partition"
    assert explicit is False


def test_branch_data_mode_rejects_legacy_key() -> None:
    with pytest.raises(ValueError, match="renamed to `branch_data_mode`"):
        _resolve_branch_data_mode({"branch_data_on_create": "last_partition"})


def test_branch_data_mode_rejects_all_partitions() -> None:
    with pytest.raises(ValueError, match="Invalid branch_data_mode"):
        _resolve_branch_data_mode({"branch_data_mode": "all_partitions"})


def test_branch_data_mode_invalid_value() -> None:
    with pytest.raises(ValueError, match="Invalid branch_data_mode"):
        _resolve_branch_data_mode({"branch_data_mode": "invalid"})


def test_branch_data_mode_non_string() -> None:
    with pytest.raises(ValueError, match="must be a string"):
        _resolve_branch_data_mode({"branch_data_mode": 1})


def test_load_config_warns_when_local_mode_explicit_branch_data_mode(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    project = tmp_path / "project"
    project.mkdir()
    (project / "tinybird.config.json").write_text(
        json.dumps(
            {
                "include": ["lib/datasources.py"],
                "token": "p.test",
                "base_url": "https://api.tinybird.co",
                "dev_mode": "local",
                "branch_data_mode": "last_partition",
            }
        ),
        encoding="utf-8",
    )

    load_config(str(project))
    captured = capsys.readouterr()
    assert "branch_data_mode is set" in captured.out


def test_load_config_does_not_warn_when_branch_data_mode_is_implicit(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    project = tmp_path / "project"
    project.mkdir()
    (project / "tinybird.config.json").write_text(
        json.dumps(
            {
                "include": ["lib/datasources.py"],
                "token": "p.test",
                "base_url": "https://api.tinybird.co",
                "dev_mode": "local",
            }
        ),
        encoding="utf-8",
    )

    load_config(str(project))
    captured = capsys.readouterr()
    assert "branch_data_mode is set" not in captured.out
