from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

import tinybird_sdk.api.resources as resources
import tinybird_sdk.cli.commands.pull as pull_cmd


class _FakeResponse:
    def __init__(self, status_code: int, payload: dict[str, Any] | None = None, text: str | None = None):
        self.status_code = status_code
        self._payload = payload or {}
        self._text = text if text is not None else ""

    @property
    def ok(self) -> bool:
        return 200 <= self.status_code < 300

    @property
    def text(self) -> str:
        return self._text

    def json(self) -> dict[str, Any]:
        return self._payload


def test_pull_all_resource_files_includes_connections(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_fetch(url: str, **_kwargs: Any) -> _FakeResponse:
        if url.endswith("/v0/datasources"):
            return _FakeResponse(200, {"datasources": [{"name": "events"}]})
        if url.endswith("/v1/pipes"):
            return _FakeResponse(200, {"pipes": [{"name": "top_events"}]})
        if url.endswith("/v0/connectors"):
            return _FakeResponse(200, {"connectors": [{"name": "main_kafka"}]})
        if url.endswith("/v0/datasources/events.datasource"):
            return _FakeResponse(200, text='SCHEMA >\n    id Int32\n\nENGINE "MergeTree"')
        if url.endswith("/v1/pipes/top_events.pipe"):
            return _FakeResponse(200, text="NODE n\nSQL >\n    %\n    SELECT 1")
        if url.endswith("/v0/connectors/main_kafka.connection"):
            return _FakeResponse(200, text="TYPE kafka\nKAFKA_BOOTSTRAP_SERVERS localhost:9092")
        return _FakeResponse(404)

    monkeypatch.setattr(resources, "tinybird_fetch", fake_fetch)
    pulled = resources.pull_all_resource_files({"base_url": "https://api.tinybird.co", "token": "p.token"})

    assert len(pulled["datasources"]) == 1
    assert len(pulled["pipes"]) == 1
    assert len(pulled["connections"]) == 1
    assert pulled["connections"][0].filename == "main_kafka.connection"


def test_list_pipes_v1_falls_back_to_v0(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_fetch(url: str, **_kwargs: Any) -> _FakeResponse:
        if url.endswith("/v1/pipes"):
            return _FakeResponse(404)
        if url.endswith("/v0/pipes"):
            return _FakeResponse(200, {"pipes": [{"name": "top_events"}]})
        return _FakeResponse(404)

    monkeypatch.setattr(resources, "tinybird_fetch", fake_fetch)
    assert resources.list_pipes_v1({"base_url": "https://api.tinybird.co", "token": "p.token"}) == ["top_events"]


def test_run_pull_writes_datasource_pipe_and_connection(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        pull_cmd,
        "load_config_async",
        lambda *_args, **_kwargs: {"base_url": "https://api.tinybird.co", "token": "p.token"},
    )
    monkeypatch.setattr(
        pull_cmd,
        "pull_all_resource_files",
        lambda *_args, **_kwargs: {
            "datasources": [
                pull_cmd.ResourceFile(
                    name="events",
                    type="datasource",
                    filename="events.datasource",
                    content="SCHEMA >\n    id Int32\n",
                )
            ],
            "pipes": [
                pull_cmd.ResourceFile(
                    name="top_events",
                    type="pipe",
                    filename="top_events.pipe",
                    content="NODE n\nSQL >\n    %\n    SELECT 1\n",
                )
            ],
            "connections": [
                pull_cmd.ResourceFile(
                    name="main_kafka",
                    type="connection",
                    filename="main_kafka.connection",
                    content="TYPE kafka\nKAFKA_BOOTSTRAP_SERVERS localhost:9092\n",
                )
            ],
        },
    )

    result = pull_cmd.run_pull({"cwd": str(tmp_path), "output_dir": "out"})
    assert result.success is True
    assert result.stats == {"datasources": 1, "pipes": 1, "connections": 1, "total": 3}

    out = tmp_path / "out"
    assert (out / "events.datasource").exists()
    assert (out / "top_events.pipe").exists()
    assert (out / "main_kafka.connection").exists()


def test_run_pull_respects_overwrite_flag(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        pull_cmd,
        "load_config_async",
        lambda *_args, **_kwargs: {"base_url": "https://api.tinybird.co", "token": "p.token"},
    )
    monkeypatch.setattr(
        pull_cmd,
        "pull_all_resource_files",
        lambda *_args, **_kwargs: {
            "datasources": [
                pull_cmd.ResourceFile(
                    name="events",
                    type="datasource",
                    filename="events.datasource",
                    content="new-content",
                )
            ],
            "pipes": [],
            "connections": [],
        },
    )

    out = tmp_path / "out"
    out.mkdir(parents=True, exist_ok=True)
    target = out / "events.datasource"
    target.write_text("old-content", encoding="utf-8")

    not_overwritten = pull_cmd.run_pull({"cwd": str(tmp_path), "output_dir": "out", "overwrite": False})
    assert not_overwritten.success is False
    assert not_overwritten.error is not None
    assert "File already exists" in not_overwritten.error

    overwritten = pull_cmd.run_pull({"cwd": str(tmp_path), "output_dir": "out", "overwrite": True})
    assert overwritten.success is True
    assert overwritten.files is not None
    assert overwritten.files[0].status == "overwritten"
    assert target.read_text(encoding="utf-8") == "new-content"
