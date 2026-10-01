from __future__ import annotations

import json
from typing import Any

import pytest

import tinybird_sdk.api.api as api_module
from tinybird_sdk.client.base import TinybirdClient
from tinybird_sdk.client.types import TinybirdError


class _FakeResponse:
    def __init__(self, status_code: int, payload: Any = None, text: str | None = None):
        self.status_code = status_code
        self._payload = payload
        self.text = (
            text if text is not None else (json.dumps(payload) if payload is not None else "")
        )

    @property
    def ok(self) -> bool:
        return 200 <= self.status_code < 300

    def json(self) -> Any:
        return self._payload


def _make_client() -> TinybirdClient:
    return TinybirdClient({"base_url": "https://api.tinybird.co", "token": "p.workspace"})


def _capture_fetch(captured: dict[str, Any], response: _FakeResponse):
    def fake_fetch(url: str, **kwargs: Any) -> _FakeResponse:
        captured["url"] = url
        captured["method"] = kwargs.get("method")
        captured["headers"] = kwargs.get("headers")
        captured["body"] = kwargs.get("body")
        return response

    return fake_fetch


def test_tokens_list_unwraps_tokens_key(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}
    monkeypatch.setattr(
        api_module,
        "tinybird_fetch",
        _capture_fetch(captured, _FakeResponse(200, {"tokens": [{"name": "t1"}, {"name": "t2"}]})),
    )

    result = _make_client().tokens.list()

    assert result == [{"name": "t1"}, {"name": "t2"}]
    assert captured["method"] == "GET"
    assert captured["url"].endswith("/v0/tokens")


def test_tokens_get_returns_full_token(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}
    payload = {"name": "t1", "token": "p.abc", "scopes": [{"type": "DATASOURCES:READ"}]}
    monkeypatch.setattr(
        api_module, "tinybird_fetch", _capture_fetch(captured, _FakeResponse(200, payload))
    )

    result = _make_client().tokens.get("t1")

    assert result == payload
    assert captured["url"].endswith("/v0/tokens/t1")


def test_tokens_scopes_extracts_scopes_field(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {"name": "t1", "token": "p.abc", "scopes": [{"type": "DATASOURCES:READ"}]}
    monkeypatch.setattr(
        api_module, "tinybird_fetch", _capture_fetch({}, _FakeResponse(200, payload))
    )

    assert _make_client().tokens.scopes("t1") == [{"type": "DATASOURCES:READ"}]


def test_tokens_refresh_posts_to_refresh_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}
    monkeypatch.setattr(
        api_module,
        "tinybird_fetch",
        _capture_fetch(captured, _FakeResponse(200, {"name": "t1", "token": "p.new"})),
    )

    result = _make_client().tokens.refresh("t1")

    assert result == {"name": "t1", "token": "p.new"}
    assert captured["method"] == "POST"
    assert captured["url"].endswith("/v0/tokens/t1/refresh")


def test_tokens_revoke_deletes_and_tolerates_empty_body(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}
    monkeypatch.setattr(
        api_module, "tinybird_fetch", _capture_fetch(captured, _FakeResponse(200, text=""))
    )

    result = _make_client().tokens.revoke("t1")

    assert result == {}
    assert captured["method"] == "DELETE"
    assert captured["url"].endswith("/v0/tokens/t1")


def test_tokens_copy_returns_current_value(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {"name": "t1", "token": "p.secret-value"}
    monkeypatch.setattr(
        api_module, "tinybird_fetch", _capture_fetch({}, _FakeResponse(200, payload))
    )

    assert _make_client().tokens.copy("t1") == "p.secret-value"


def test_tokens_methods_wrap_api_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        api_module,
        "tinybird_fetch",
        _capture_fetch({}, _FakeResponse(403, {"error": "Forbidden"})),
    )

    client = _make_client()
    with pytest.raises(TinybirdError, match="Forbidden"):
        client.tokens.get("t1")
    with pytest.raises(TinybirdError, match="Forbidden"):
        client.tokens.refresh("t1")
    with pytest.raises(TinybirdError, match="Forbidden"):
        client.tokens.revoke("t1")
