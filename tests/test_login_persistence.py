from __future__ import annotations

import json
from pathlib import Path

import pytest

import tinybird_sdk.cli.commands.login as login_module
from tinybird_sdk.cli.auth import AuthResult
from tinybird_sdk.cli.commands.login import run_login


def test_run_login_persists_token_and_base_url_to_env_local(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        login_module,
        "browser_login",
        lambda *_args, **_kwargs: AuthResult(
            success=True,
            token="p.test-token",
            base_url="https://api.tinybird.co",
            workspace_name="my_workspace",
            user_email="user@example.com",
        ),
    )

    result = run_login({"cwd": str(tmp_path)})

    assert result.success
    assert result.token == "p.test-token"
    assert result.workspace_name == "my_workspace"

    env_local = tmp_path / ".env.local"
    assert env_local.exists()
    content = env_local.read_text(encoding="utf-8")
    assert "TINYBIRD_TOKEN=p.test-token" in content
    assert "TINYBIRD_URL=https://api.tinybird.co" in content


def test_run_login_updates_existing_json_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = tmp_path / "tinybird.config.json"
    config_path.write_text(json.dumps({"base_url": "https://old.example.com"}), encoding="utf-8")

    monkeypatch.setattr(
        login_module,
        "browser_login",
        lambda *_args, **_kwargs: AuthResult(
            success=True, token="p.new-token", base_url="https://api.tinybird.co"
        ),
    )

    result = run_login({"cwd": str(tmp_path)})

    assert result.success
    updated = json.loads(config_path.read_text(encoding="utf-8"))
    assert updated["token"] == "${TINYBIRD_TOKEN}"
    assert updated["base_url"] == "https://api.tinybird.co"


def test_run_login_does_not_persist_when_disabled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        login_module,
        "browser_login",
        lambda *_args, **_kwargs: AuthResult(success=True, token="p.test-token"),
    )

    result = run_login({"cwd": str(tmp_path), "persist": False})

    assert result.success
    assert not (tmp_path / ".env.local").exists()


def test_run_login_propagates_auth_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        login_module,
        "browser_login",
        lambda *_args, **_kwargs: AuthResult(success=False, error="Authentication timed out"),
    )

    result = run_login({})

    assert not result.success
    assert result.error == "Authentication timed out"
