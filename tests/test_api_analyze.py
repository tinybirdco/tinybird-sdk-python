from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

import tinybird_sdk.api.api as api_module
from tinybird_sdk.api.api import TinybirdApi, TinybirdApiError


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


def _make_api() -> TinybirdApi:
    return TinybirdApi({"base_url": "https://api.tinybird.co", "token": "p.test"})


def test_analyze_requires_either_url_or_file() -> None:
    api = _make_api()
    with pytest.raises(ValueError, match="Either 'url' or 'file'"):
        api.analyze({})


def test_analyze_rejects_both_url_and_file() -> None:
    api = _make_api()
    with pytest.raises(ValueError, match="Only one of 'url' or 'file'"):
        api.analyze({"url": "https://x.y/file.csv", "file": "./file.csv"})


def test_analyze_with_url(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def fake_fetch(url: str, **kwargs: Any) -> _FakeResponse:
        captured["url"] = url
        captured["method"] = kwargs.get("method")
        return _FakeResponse(200, {"analysis": {"schema": "id Int64, name String"}})

    monkeypatch.setattr(api_module, "tinybird_fetch", fake_fetch)

    result = _make_api().analyze({"url": "https://x.y/events.csv"})

    assert captured["method"] == "POST"
    assert captured["url"].startswith("https://api.tinybird.co/v0/analyze?")
    assert "url=https" in captured["url"]
    assert "format=csv" in captured["url"]
    assert result["analysis"]["schema"] == "id Int64, name String"


def test_analyze_with_local_file(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    captured: dict[str, Any] = {}

    def fake_fetch(url: str, **kwargs: Any) -> _FakeResponse:
        captured["url"] = url
        captured["headers"] = kwargs.get("headers")
        captured["body"] = kwargs.get("body")
        return _FakeResponse(
            200,
            {"analysis": {"columns": [{"name": "id", "recommended_type": "Int64"}]}},
        )

    monkeypatch.setattr(api_module, "tinybird_fetch", fake_fetch)

    local_file = tmp_path / "events.csv"
    local_file.write_text("id\n1\n", encoding="utf-8")

    result = _make_api().analyze({"file": str(local_file)})

    assert captured["url"].endswith("/v0/analyze?format=csv")
    assert captured["headers"]["Content-Type"].startswith("multipart/form-data;")
    assert b"id\n1\n" in captured["body"]
    assert result["analysis"]["columns"][0]["name"] == "id"


def test_analyze_raises_on_error_response(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_fetch(_url: str, **_kwargs: Any) -> _FakeResponse:
        return _FakeResponse(422, {"error": "could not analyze file"})

    monkeypatch.setattr(api_module, "tinybird_fetch", fake_fetch)

    with pytest.raises(TinybirdApiError, match="could not analyze file"):
        _make_api().analyze({"url": "https://x.y/events.csv"})
