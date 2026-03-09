from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from tinybird_sdk.cli.config import (
    find_config_file,
    get_client_path,
    get_datasources_path,
    get_pipes_path,
    get_relative_tinybird_dir,
    has_src_folder,
    load_config,
    load_config_async,
)


def test_find_config_file_priority_and_parent_walk(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    nested = root / "a" / "b"
    nested.mkdir(parents=True)

    (root / "tinybird.json").write_text("{}", encoding="utf-8")
    (root / "tinybird.config.json").write_text("{}", encoding="utf-8")

    # JSON config should be found (tinybird.config.json has priority over tinybird.json)
    found = find_config_file(str(nested))
    assert found is not None
    assert found["type"] == "tinybird.config.json"
    assert found["path"].endswith("tinybird.config.json")


def test_find_config_file_python_priority(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    root.mkdir(parents=True)

    (root / "tinybird.config.json").write_text("{}", encoding="utf-8")
    (root / "tinybird.config.py").write_text("config = {}", encoding="utf-8")

    # Python config should have priority over JSON config
    found = find_config_file(str(root))
    assert found is not None
    assert found["type"] == "tinybird.config.py"
    assert found["path"].endswith("tinybird.config.py")


def test_load_config_loads_dotenv_local_before_dotenv(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = tmp_path / "project"
    project.mkdir()

    (project / ".env.local").write_text("TINYBIRD_TOKEN=local_token\n", encoding="utf-8")
    (project / ".env").write_text("TINYBIRD_TOKEN=env_token\nTINYBIRD_URL=https://api.us-east-1.aws.tinybird.co\n", encoding="utf-8")
    (project / "tinybird.config.json").write_text(
        json.dumps(
            {
                "include": ["lib/datasources.py", "lib/pipes.py"],
                "token": "${TINYBIRD_TOKEN}",
                "base_url": "${TINYBIRD_URL}",
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.delenv("TINYBIRD_TOKEN", raising=False)
    monkeypatch.delenv("TINYBIRD_URL", raising=False)

    loaded = load_config(str(project))
    assert loaded["token"] == "local_token"
    assert loaded["base_url"] == "https://api.us-east-1.aws.tinybird.co"


def test_load_config_async_supports_python(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = tmp_path / "project"
    project.mkdir()

    (project / ".env.local").write_text("TINYBIRD_TOKEN=from_env\n", encoding="utf-8")
    (project / "tinybird.config.py").write_text(
        "config = {'include': ['lib/datasources.py'], 'token': '${TINYBIRD_TOKEN}', 'base_url': 'https://api.tinybird.co'}\n",
        encoding="utf-8",
    )

    monkeypatch.delenv("TINYBIRD_TOKEN", raising=False)
    loaded = load_config_async(str(project))
    assert loaded["token"] == "from_env"
    assert loaded["include"] == ["lib/datasources.py"]


def test_path_helpers_with_and_without_src_folder(tmp_path: Path) -> None:
    no_src = tmp_path / "no-src"
    no_src.mkdir()
    with_src = tmp_path / "with-src"
    (with_src / "src").mkdir(parents=True)

    assert has_src_folder(str(no_src)) is False
    assert get_relative_tinybird_dir(str(no_src)) == "lib/tinybird.py"
    assert get_datasources_path(str(no_src)).endswith("lib/datasources.py")
    assert get_pipes_path(str(no_src)).endswith("lib/pipes.py")
    assert get_client_path(str(no_src)).endswith("lib/client.py")

    assert has_src_folder(str(with_src)) is True
    assert get_relative_tinybird_dir(str(with_src)) == "src/lib/tinybird.py"
    assert get_datasources_path(str(with_src)).endswith("src/lib/datasources.py")
    assert get_pipes_path(str(with_src)).endswith("src/lib/pipes.py")
    assert get_client_path(str(with_src)).endswith("src/lib/client.py")
