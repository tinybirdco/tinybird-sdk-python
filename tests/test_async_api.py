from __future__ import annotations

import asyncio
import json
from datetime import date
from pathlib import Path
from typing import Any

import httpx
import pytest

from tinybird_sdk.api.api import TinybirdApiError
from tinybird_sdk.api.async_api import AsyncTinybirdApi


def _make_transport(handler):
    return httpx.MockTransport(handler)


def _patch_client(monkeypatch: pytest.MonkeyPatch, api: AsyncTinybirdApi, handler) -> None:
    # Force AsyncTinybirdApi's lazily-created httpx.AsyncClient onto a mock
    # transport so no real network I/O happens, while exercising the real
    # async request/response path (headers, retries, error mapping).
    client = httpx.AsyncClient(transport=_make_transport(handler))
    api._client = client


def test_constructor_validation() -> None:
    with pytest.raises(ValueError, match="base_url is required"):
        AsyncTinybirdApi({"base_url": "", "token": "x"})
    with pytest.raises(ValueError, match="token is required"):
        AsyncTinybirdApi({"base_url": "https://api.tinybird.co", "token": ""})


def test_query_serializes_list_and_dates(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["headers"] = request.headers
        return httpx.Response(200, json={"data": []})

    async def run() -> None:
        api = AsyncTinybirdApi({"base_url": "https://api.tinybird.co", "token": "p.token"})
        _patch_client(monkeypatch, api, handler)
        await api.query("endpoint_name", {"ids": [1, 2], "day": date(2026, 2, 14)})
        await api.aclose()

    asyncio.run(run())

    url = captured["url"]
    assert "/v0/pipes/endpoint_name.json?" in url
    assert "ids=1" in url and "ids=2" in url
    assert "day=2026-02-14" in url
    assert captured["headers"]["authorization"] == "Bearer p.token"


def test_ingest_batch_empty_returns_zero_counts() -> None:
    async def run() -> dict[str, Any]:
        api = AsyncTinybirdApi({"base_url": "https://api.tinybird.co", "token": "p.test"})
        return await api.ingest_batch("events", [])

    result = asyncio.run(run())
    assert result == {"successful_rows": 0, "quarantined_rows": 0}


def test_ingest_batch_serializes_events(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["method"] = request.method
        captured["body"] = request.content.decode("utf-8")
        return httpx.Response(200, json={"successful_rows": 1, "quarantined_rows": 0})

    async def run() -> dict[str, Any]:
        api = AsyncTinybirdApi({"base_url": "https://api.tinybird.co", "token": "p.token"})
        _patch_client(monkeypatch, api, handler)
        result = await api.ingest_batch("events", [{"id": 1}])
        await api.aclose()
        return result

    result = asyncio.run(run())
    assert result["successful_rows"] == 1
    assert captured["method"] == "POST"
    assert "/v0/events?name=events&wait=true" in captured["url"]
    assert json.loads(captured["body"]) == {"id": 1}


def test_sql_uses_text_plain_and_maps_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    def ok_handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["content-type"] == "text/plain"
        return httpx.Response(200, json={"data": [{"x": 1}]})

    async def run_ok() -> dict[str, Any]:
        api = AsyncTinybirdApi({"base_url": "https://api.tinybird.co", "token": "p.token"})
        _patch_client(monkeypatch, api, ok_handler)
        result = await api.sql("SELECT 1")
        await api.aclose()
        return result

    assert asyncio.run(run_ok())["data"] == [{"x": 1}]

    def bad_handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(403, json={"error": "denied"})

    async def run_bad() -> None:
        api = AsyncTinybirdApi({"base_url": "https://api.tinybird.co", "token": "p.token"})
        _patch_client(monkeypatch, api, bad_handler)
        try:
            await api.sql("SELECT 2")
        finally:
            await api.aclose()

    with pytest.raises(TinybirdApiError, match="denied"):
        asyncio.run(run_bad())


def test_ingest_batch_retries_on_429_then_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"count": 0}
    sleep_calls: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    def handler(_request: httpx.Request) -> httpx.Response:
        calls["count"] += 1
        if calls["count"] == 1:
            return httpx.Response(429, headers={"Retry-After": "1"}, json={"error": "slow down"})
        return httpx.Response(200, json={"successful_rows": 1, "quarantined_rows": 0})

    async def run() -> dict[str, Any]:
        api = AsyncTinybirdApi({"base_url": "https://api.tinybird.co", "token": "p.token"})
        _patch_client(monkeypatch, api, handler)
        result = await api.ingest_batch("events", [{"id": 1}], {"maxRetries": 3})
        await api.aclose()
        return result

    result = asyncio.run(run())
    assert result == {"successful_rows": 1, "quarantined_rows": 0}
    assert calls["count"] == 2
    assert sleep_calls == [1.0]


def test_append_datasource_from_file_reads_off_thread(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    csv_path = tmp_path / "events.csv"
    csv_path.write_text("id,name\n1,a\n")

    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["content_type"] = request.headers["content-type"]
        captured["body"] = request.content
        return httpx.Response(200, json={"import_id": "abc"})

    async def run() -> dict[str, Any]:
        api = AsyncTinybirdApi({"base_url": "https://api.tinybird.co", "token": "p.token"})
        _patch_client(monkeypatch, api, handler)
        result = await api.append_datasource("events", {"file": str(csv_path)})
        await api.aclose()
        return result

    result = asyncio.run(run())
    assert result == {"import_id": "abc"}
    assert captured["content_type"].startswith("multipart/form-data")
    assert b"1,a" in captured["body"]


def test_append_validation() -> None:
    async def run() -> None:
        api = AsyncTinybirdApi({"base_url": "https://api.tinybird.co", "token": "p.test"})
        with pytest.raises(ValueError, match="Either 'url' or 'file'"):
            await api.append_datasource("events", {})
        with pytest.raises(ValueError, match="Only one of 'url' or 'file'"):
            await api.append_datasource("events", {"url": "https://x", "file": "./x.csv"})

    asyncio.run(run())


def test_async_context_manager_closes_client() -> None:
    async def run() -> httpx.AsyncClient:
        async with AsyncTinybirdApi(
            {"base_url": "https://api.tinybird.co", "token": "p.test"}
        ) as api:
            assert api._client is None  # not opened until first request
            opened_client = api._get_http_client()
        assert api._client is None  # aclose() clears the reference
        return opened_client

    opened_client = asyncio.run(run())
    assert opened_client.is_closed
