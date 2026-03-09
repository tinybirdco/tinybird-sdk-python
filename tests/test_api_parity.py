from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pytest

import tinybird_sdk.api.api as api_module
from tinybird_sdk.api.api import TinybirdApi, TinybirdApiError


class _FakeResponse:
    def __init__(
        self,
        status_code: int,
        payload: dict[str, Any] | None = None,
        text: str | None = None,
        headers: dict[str, str] | None = None,
    ):
        self.status_code = status_code
        self._payload = payload or {}
        self._text = text if text is not None else json.dumps(self._payload)
        self.headers = headers or {}

    @property
    def ok(self) -> bool:
        return 200 <= self.status_code < 300

    @property
    def text(self) -> str:
        return self._text

    def json(self) -> dict[str, Any]:
        return self._payload


def test_query_serializes_list_and_dates(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, dict[str, Any]]] = []

    def fake_fetch(url: str, **kwargs: Any) -> _FakeResponse:
        calls.append((url, kwargs))
        return _FakeResponse(200, {"data": []})

    monkeypatch.setattr(api_module, "tinybird_fetch", fake_fetch)
    api = TinybirdApi({"base_url": "https://api.tinybird.co", "token": "p.token"})
    api.query("endpoint_name", {"ids": [1, 2], "day": date(2026, 2, 14)})

    url = calls[0][0]
    assert "/v0/pipes/endpoint_name.json?" in url
    assert "ids=1" in url and "ids=2" in url
    assert "day=2026-02-14" in url


def test_ingest_batch_serializes_events_and_wait_default(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, dict[str, Any]]] = []

    def fake_fetch(url: str, **kwargs: Any) -> _FakeResponse:
        calls.append((url, kwargs))
        return _FakeResponse(200, {"successful_rows": 2, "quarantined_rows": 0})

    monkeypatch.setattr(api_module, "tinybird_fetch", fake_fetch)
    api = TinybirdApi({"base_url": "https://api.tinybird.co", "token": "p.token"})
    result = api.ingest_batch(
        "events",
        [
            {"id": 9007199254740993, "ts": datetime(2026, 2, 14, 12, 0, 0)},
            {"id": 2},
        ],
    )

    assert result["successful_rows"] == 2
    url, request = calls[0]
    assert "/v0/events?name=events&wait=true" in url
    assert request["method"] == "POST"
    body = request["body"]
    assert isinstance(body, str)
    assert "9007199254740993" in body
    assert "2026-02-14T12:00:00" in body


def test_sql_uses_text_plain_and_maps_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    def ok_fetch(_url: str, **kwargs: Any) -> _FakeResponse:
        assert kwargs["headers"]["Content-Type"] == "text/plain"
        return _FakeResponse(200, {"data": [{"x": 1}]})

    monkeypatch.setattr(api_module, "tinybird_fetch", ok_fetch)
    api = TinybirdApi({"base_url": "https://api.tinybird.co", "token": "p.token"})
    assert api.sql("SELECT 1")["data"] == [{"x": 1}]

    def bad_fetch(_url: str, **_kwargs: Any) -> _FakeResponse:
        return _FakeResponse(403, {"error": "denied"})

    monkeypatch.setattr(api_module, "tinybird_fetch", bad_fetch)
    with pytest.raises(TinybirdApiError, match="denied"):
        api.sql("SELECT 2")


def test_append_delete_and_truncate_paths(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    calls: list[tuple[str, dict[str, Any]]] = []

    def fake_fetch(url: str, **kwargs: Any) -> _FakeResponse:
        calls.append((url, kwargs))
        if url.endswith("/truncate"):
            return _FakeResponse(200, text="")
        return _FakeResponse(200, {"ok": True})

    monkeypatch.setattr(api_module, "tinybird_fetch", fake_fetch)
    api = TinybirdApi({"base_url": "https://api.tinybird.co", "token": "p.token"})

    api.append_datasource("events", {"url": "https://x.y/file.csv"})
    assert "mode=append" in calls[0][0]
    assert calls[0][1]["headers"]["Content-Type"] == "application/x-www-form-urlencoded"

    local_file = tmp_path / "events.csv"
    local_file.write_text("id\n1\n", encoding="utf-8")
    api.append_datasource("events", {"file": str(local_file)})
    assert "multipart/form-data;" in calls[1][1]["headers"]["Content-Type"]

    api.delete_datasource("events", {"delete_condition": "id > 0"})
    assert calls[2][0].endswith("/v0/datasources/events/delete")

    assert api.truncate_datasource("events") == {}


def test_append_requires_either_url_or_file() -> None:
    api = TinybirdApi({"base_url": "https://api.tinybird.co", "token": "p.token"})
    with pytest.raises(ValueError, match="Either 'url' or 'file'"):
        api.append_datasource("events", {})


def test_ingest_does_not_retry_503_when_max_retries_is_undefined(monkeypatch: pytest.MonkeyPatch) -> None:
    attempts = 0

    def fake_fetch(_url: str, **_kwargs: Any) -> _FakeResponse:
        nonlocal attempts
        attempts += 1
        return _FakeResponse(503, text="Service unavailable")

    monkeypatch.setattr(api_module, "tinybird_fetch", fake_fetch)
    monkeypatch.setattr(api_module.time, "sleep", lambda *_args, **_kwargs: None)

    api = TinybirdApi({"base_url": "https://api.tinybird.co", "token": "p.token"})
    with pytest.raises(TinybirdApiError) as exc:
        api.ingest("events", {"id": 1})

    assert exc.value.status_code == 503
    assert attempts == 1


def test_ingest_retries_503_with_exponential_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    attempts = 0
    slept: list[float] = []

    def fake_fetch(_url: str, **_kwargs: Any) -> _FakeResponse:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return _FakeResponse(503, text="Service unavailable")
        return _FakeResponse(200, {"successful_rows": 1, "quarantined_rows": 0})

    monkeypatch.setattr(api_module, "tinybird_fetch", fake_fetch)
    monkeypatch.setattr(api_module.time, "sleep", lambda value: slept.append(value))

    api = TinybirdApi({"base_url": "https://api.tinybird.co", "token": "p.token"})
    result = api.ingest("events", {"id": 1}, {"maxRetries": 1})

    assert result == {"successful_rows": 1, "quarantined_rows": 0}
    assert attempts == 2
    assert slept == [0.2]


def test_ingest_retries_429_using_retry_after(monkeypatch: pytest.MonkeyPatch) -> None:
    attempts = 0
    slept: list[float] = []

    def fake_fetch(_url: str, **_kwargs: Any) -> _FakeResponse:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return _FakeResponse(429, text="Rate limited", headers={"Retry-After": "0"})
        return _FakeResponse(200, {"successful_rows": 1, "quarantined_rows": 0})

    monkeypatch.setattr(api_module, "tinybird_fetch", fake_fetch)
    monkeypatch.setattr(api_module.time, "sleep", lambda value: slept.append(value))

    api = TinybirdApi({"base_url": "https://api.tinybird.co", "token": "p.token"})
    result = api.ingest("events", {"id": 1}, {"maxRetries": 1})

    assert result == {"successful_rows": 1, "quarantined_rows": 0}
    assert attempts == 2
    assert slept == []


def test_ingest_does_not_retry_429_without_delay_headers(monkeypatch: pytest.MonkeyPatch) -> None:
    attempts = 0

    def fake_fetch(_url: str, **_kwargs: Any) -> _FakeResponse:
        nonlocal attempts
        attempts += 1
        return _FakeResponse(429, text="Rate limited")

    monkeypatch.setattr(api_module, "tinybird_fetch", fake_fetch)
    monkeypatch.setattr(api_module.time, "sleep", lambda *_args, **_kwargs: None)

    api = TinybirdApi({"base_url": "https://api.tinybird.co", "token": "p.token"})
    with pytest.raises(TinybirdApiError) as exc:
        api.ingest("events", {"id": 1}, {"maxRetries": 3})

    assert exc.value.status_code == 429
    assert attempts == 1


def test_ingest_stops_retrying_after_max_retries_for_503(monkeypatch: pytest.MonkeyPatch) -> None:
    attempts = 0

    def fake_fetch(_url: str, **_kwargs: Any) -> _FakeResponse:
        nonlocal attempts
        attempts += 1
        return _FakeResponse(503, text="Service unavailable")

    monkeypatch.setattr(api_module, "tinybird_fetch", fake_fetch)
    monkeypatch.setattr(api_module.time, "sleep", lambda *_args, **_kwargs: None)

    api = TinybirdApi({"base_url": "https://api.tinybird.co", "token": "p.token"})
    with pytest.raises(TinybirdApiError) as exc:
        api.ingest("events", {"id": 1}, {"maxRetries": 2})

    assert exc.value.status_code == 503
    assert attempts == 3


def test_ingest_raises_for_invalid_max_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(api_module, "tinybird_fetch", lambda *_args, **_kwargs: _FakeResponse(200, {}))

    api = TinybirdApi({"base_url": "https://api.tinybird.co", "token": "p.token"})
    with pytest.raises(ValueError, match="'maxRetries' must be a finite number"):
        api.ingest("events", {"id": 1}, {"maxRetries": "x"})
