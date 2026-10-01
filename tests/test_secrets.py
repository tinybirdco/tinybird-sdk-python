from __future__ import annotations

import json
from typing import Any
from urllib.parse import parse_qs

import pytest

import tinybird_sdk.api.api as api_module
from tinybird_sdk.api.api import TinybirdApi, TinybirdApiError
from tinybird_sdk.client.base import TinybirdClient
from tinybird_sdk.client.types import TinybirdError


class _FakeResponse:
    def __init__(
        self,
        status_code: int,
        payload: dict[str, Any] | None = None,
        text: str | None = None,
    ):
        self.status_code = status_code
        self._payload = payload or {}
        self._text = (
            text if text is not None else (json.dumps(self._payload) if payload is not None else "")
        )

    @property
    def ok(self) -> bool:
        return 200 <= self.status_code < 300

    @property
    def text(self) -> str:
        return self._text

    def json(self) -> dict[str, Any]:
        return self._payload


def _make_api() -> TinybirdApi:
    return TinybirdApi({"base_url": "https://api.tinybird.co", "token": "p.test"})


def test_list_secrets(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, dict[str, Any]]] = []

    def fake_fetch(url: str, **kwargs: Any) -> _FakeResponse:
        calls.append((url, kwargs))
        return _FakeResponse(
            200,
            {
                "variables": [
                    {"name": "API_KEY", "created_at": "2026-01-01", "updated_at": "2026-01-01"}
                ]
            },
        )

    monkeypatch.setattr(api_module, "tinybird_fetch", fake_fetch)
    result = _make_api().list_secrets()

    assert result == [{"name": "API_KEY", "created_at": "2026-01-01", "updated_at": "2026-01-01"}]
    assert calls[0][0].endswith("/v0/variables")
    assert calls[0][1]["method"] == "GET"
    # Secret values are never part of the list response shape.
    assert all("value" not in secret for secret in result)


def test_set_secret_creates_when_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, dict[str, Any]]] = []

    def fake_fetch(url: str, **kwargs: Any) -> _FakeResponse:
        calls.append((url, kwargs))
        if kwargs.get("method") == "GET":
            return _FakeResponse(404, text="not found")
        return _FakeResponse(200, {"name": "API_KEY"})

    monkeypatch.setattr(api_module, "tinybird_fetch", fake_fetch)
    result = _make_api().set_secret("API_KEY", "super-secret-value")

    assert result == {"name": "API_KEY"}
    get_call, create_call = calls
    assert get_call[1]["method"] == "GET"
    assert create_call[1]["method"] == "POST"
    assert create_call[0].endswith("/v0/variables")
    body = parse_qs(create_call[1]["body"])
    assert body["name"] == ["API_KEY"]
    assert body["value"] == ["super-secret-value"]


def test_set_secret_updates_when_found(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, dict[str, Any]]] = []

    def fake_fetch(url: str, **kwargs: Any) -> _FakeResponse:
        calls.append((url, kwargs))
        if kwargs.get("method") == "GET":
            return _FakeResponse(200, {"name": "API_KEY"})
        return _FakeResponse(200, {"name": "API_KEY"})

    monkeypatch.setattr(api_module, "tinybird_fetch", fake_fetch)
    _make_api().set_secret("API_KEY", "rotated-value")

    get_call, update_call = calls
    assert update_call[1]["method"] == "PUT"
    assert update_call[0].endswith("/v0/variables/API_KEY")
    body = parse_qs(update_call[1]["body"])
    assert body["value"] == ["rotated-value"]
    assert "name" not in body


def test_set_secret_propagates_non_404_lookup_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_fetch(url: str, **kwargs: Any) -> _FakeResponse:
        return _FakeResponse(403, text='{"error": "forbidden"}')

    monkeypatch.setattr(api_module, "tinybird_fetch", fake_fetch)
    with pytest.raises(TinybirdApiError, match="forbidden"):
        _make_api().set_secret("API_KEY", "value")


def test_set_secret_value_never_leaks_into_error_message(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_fetch(url: str, **kwargs: Any) -> _FakeResponse:
        if kwargs.get("method") == "GET":
            return _FakeResponse(404, text="not found")
        return _FakeResponse(400, text='{"error": "invalid secret name"}')

    monkeypatch.setattr(api_module, "tinybird_fetch", fake_fetch)
    with pytest.raises(TinybirdApiError) as exc_info:
        _make_api().set_secret("API_KEY", "super-secret-value-should-not-leak")

    assert "super-secret-value-should-not-leak" not in str(exc_info.value)


def test_delete_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, dict[str, Any]]] = []

    def fake_fetch(url: str, **kwargs: Any) -> _FakeResponse:
        calls.append((url, kwargs))
        return _FakeResponse(204, text="")

    monkeypatch.setattr(api_module, "tinybird_fetch", fake_fetch)
    result = _make_api().delete_secret("API_KEY")

    assert result == {}
    assert calls[0][1]["method"] == "DELETE"
    assert calls[0][0].endswith("/v0/variables/API_KEY")


def test_delete_secret_raises_on_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_fetch(url: str, **kwargs: Any) -> _FakeResponse:
        return _FakeResponse(404, text='{"error": "secret not found"}')

    monkeypatch.setattr(api_module, "tinybird_fetch", fake_fetch)
    with pytest.raises(TinybirdApiError, match="secret not found"):
        _make_api().delete_secret("MISSING")


def test_client_secrets_namespace_delegates(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeApi:
        def __init__(self, config: dict[str, Any]):
            self.config = config

        def list_secrets(self, options: dict[str, Any]) -> list[dict[str, Any]]:
            return [{"name": "A"}]

        def set_secret(self, name: str, value: str, options: dict[str, Any]) -> dict[str, Any]:
            return {"name": name}

        def delete_secret(self, name: str, options: dict[str, Any]) -> dict[str, Any]:
            return {}

    import tinybird_sdk.client.base as client_base

    monkeypatch.setattr(client_base, "TinybirdApi", FakeApi)

    client = TinybirdClient({"base_url": "https://api.tinybird.co", "token": "workspace_token"})
    assert client.secrets.list() == [{"name": "A"}]
    assert client.secrets.set("API_KEY", "value") == {"name": "API_KEY"}
    assert client.secrets.remove("API_KEY") == {}


def test_client_secrets_namespace_wraps_api_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeApi:
        def __init__(self, config: dict[str, Any]):
            self.config = config

        def list_secrets(self, options: dict[str, Any]) -> list[dict[str, Any]]:
            raise TinybirdApiError("boom", 500, '{"error":"boom"}', {"error": "boom"})

    import tinybird_sdk.client.base as client_base

    monkeypatch.setattr(client_base, "TinybirdApi", FakeApi)

    client = TinybirdClient({"base_url": "https://api.tinybird.co", "token": "workspace_token"})
    with pytest.raises(TinybirdError, match="boom"):
        client.secrets.list()
