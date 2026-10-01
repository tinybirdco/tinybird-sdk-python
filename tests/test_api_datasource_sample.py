from __future__ import annotations

import json
from typing import Any

import pytest

import tinybird_sdk.api.api as api_module
from tinybird_sdk.api.api import TinybirdApi


class _FakeResponse:
    def __init__(self, status_code: int, payload: dict[str, Any]):
        self.status_code = status_code
        self._payload = payload
        self.text = json.dumps(payload)

    @property
    def ok(self) -> bool:
        return 200 <= self.status_code < 300

    def json(self) -> dict[str, Any]:
        return self._payload


def _capture_fetch(captured: dict[str, Any]) -> Any:
    def fake_fetch(url: str, **kwargs: Any) -> _FakeResponse:
        captured["url"] = url
        captured["method"] = kwargs.get("method")
        captured["headers"] = kwargs.get("headers")
        captured["body"] = kwargs.get("body")
        return _FakeResponse(200, {"job_id": "job-1", "status": "waiting"})

    return fake_fetch


def _make_api() -> TinybirdApi:
    return TinybirdApi({"base_url": "https://api.tinybird.co", "token": "p.test"})


def test_sample_datasource_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}
    monkeypatch.setattr(api_module, "tinybird_fetch", _capture_fetch(captured))

    result = _make_api().sample_datasource("events")

    assert result == {"job_id": "job-1", "status": "waiting"}
    assert captured["method"] == "POST"
    assert captured["url"].endswith("/v0/datasources/events/sample")
    assert captured["headers"]["Content-Type"] == "application/json"
    assert json.loads(captured["body"]) == {"max_files": 1, "full_export": False}


def test_sample_datasource_forwards_max_files(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}
    monkeypatch.setattr(api_module, "tinybird_fetch", _capture_fetch(captured))

    _make_api().sample_datasource("events", {"max_files": 3})

    assert json.loads(captured["body"]) == {"max_files": 3, "full_export": False}


def test_sample_datasource_forwards_dynamodb_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}
    monkeypatch.setattr(api_module, "tinybird_fetch", _capture_fetch(captured))

    _make_api().sample_datasource("ddb_ds", {"rows": 100000})

    assert json.loads(captured["body"]) == {"max_files": 1, "full_export": False, "rows": 100000}


def test_sample_datasource_forwards_dynamodb_max_bytes(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}
    monkeypatch.setattr(api_module, "tinybird_fetch", _capture_fetch(captured))

    _make_api().sample_datasource("ddb_ds", {"max_bytes": "1GB"})

    assert json.loads(captured["body"]) == {
        "max_files": 1,
        "full_export": False,
        "max_bytes": "1GB",
    }


def test_sample_datasource_forwards_full_export(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}
    monkeypatch.setattr(api_module, "tinybird_fetch", _capture_fetch(captured))

    _make_api().sample_datasource("ddb_ds", {"full_export": True})

    assert json.loads(captured["body"]) == {"max_files": 1, "full_export": True}


def test_sample_datasource_rows_and_max_bytes_mutually_exclusive() -> None:
    with pytest.raises(ValueError, match="mutually exclusive"):
        _make_api().sample_datasource("ddb_ds", {"rows": 10, "max_bytes": "1GB"})
