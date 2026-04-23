from __future__ import annotations

import json
from pathlib import Path

import pytest

from tinybird_sdk.cli.config import _resolve_branch_data_on_create, load_config


def test_branch_data_on_create_last_partition() -> None:
    assert _resolve_branch_data_on_create({"branch_data_on_create": "last_partition"}) == "last_partition"


def test_branch_data_on_create_missing_returns_none() -> None:
    assert _resolve_branch_data_on_create({}) == "last_partition"


def test_branch_data_on_create_empty_defaults_to_last_partition() -> None:
    assert _resolve_branch_data_on_create({"branch_data_on_create": "   "}) == "last_partition"


def test_branch_data_on_create_all_partitions_disabled() -> None:
    with pytest.raises(ValueError, match="disabled"):
        _resolve_branch_data_on_create({"branch_data_on_create": "all_partitions"})


def test_branch_data_on_create_invalid_value() -> None:
    with pytest.raises(ValueError, match="Invalid branch_data_on_create"):
        _resolve_branch_data_on_create({"branch_data_on_create": "invalid"})


def test_branch_data_on_create_non_string() -> None:
    with pytest.raises(ValueError, match="must be a string"):
        _resolve_branch_data_on_create({"branch_data_on_create": 1})


def test_load_config_warns_when_local_mode_uses_branch_data(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    project = tmp_path / "project"
    project.mkdir()
    (project / "tinybird.config.json").write_text(
        json.dumps(
            {
                "include": ["lib/datasources.py"],
                "token": "p.test",
                "base_url": "https://api.tinybird.co",
                "dev_mode": "local",
                "branch_data_on_create": "last_partition",
            }
        ),
        encoding="utf-8",
    )

    load_config(str(project))
    captured = capsys.readouterr()
    assert "branch_data_on_create is set" in captured.out
