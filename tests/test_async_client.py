from __future__ import annotations

import asyncio
from typing import Any

import pytest

import tinybird_sdk.client.async_base as async_client_base
from tinybird_sdk.api.api import TinybirdApiError
from tinybird_sdk.api.tokens import TokenApiError
from tinybird_sdk.client.async_base import AsyncTinybirdClient
from tinybird_sdk.client.types import TinybirdError


def test_client_constructor_validation() -> None:
    with pytest.raises(ValueError, match="base_url is required"):
        AsyncTinybirdClient({"token": "x"})
    with pytest.raises(ValueError, match="token is required"):
        AsyncTinybirdClient({"base_url": "https://api.tinybird.co"})


class _FakeAsyncApi:
    def __init__(self, config: dict[str, Any]):
        self.config = config
        self.closed = False

    async def query(
        self, pipe_name: str, params: dict[str, Any], options: dict[str, Any]
    ) -> dict[str, Any]:
        if pipe_name == "boom":
            raise TinybirdApiError("boom", 500, '{"error":"boom"}', {"error": "boom"})
        return {"data": [{"ok": True}], "pipe": pipe_name, "params": params}

    async def ingest_batch(
        self, datasource_name: str, events: list[dict[str, Any]], options: dict[str, Any]
    ) -> dict[str, Any]:
        return {"datasource": datasource_name, "count": len(events)}

    async def sql(self, sql: str, options: dict[str, Any]) -> dict[str, Any]:
        return {"sql": sql}

    async def append_datasource(
        self,
        datasource_name: str,
        options: dict[str, Any],
        api_options: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return {"datasource": datasource_name, "mode": (api_options or {}).get("mode", "append")}

    async def aclose(self) -> None:
        self.closed = True


def test_client_query_and_api_error_mapping(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(async_client_base, "AsyncTinybirdApi", _FakeAsyncApi)

    async def run() -> None:
        client = AsyncTinybirdClient({"base_url": "https://api.tinybird.co", "token": "token"})
        ok = await client.query("top_events", {"limit": 1})
        assert ok["pipe"] == "top_events"
        assert ok["params"] == {"limit": 1}

        with pytest.raises(TinybirdError) as exc:
            await client.query("boom")
        assert exc.value.status_code == 500

    asyncio.run(run())


def test_client_datasources_namespace(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(async_client_base, "AsyncTinybirdApi", _FakeAsyncApi)

    async def run() -> None:
        client = AsyncTinybirdClient({"base_url": "https://api.tinybird.co", "token": "token"})
        result = await client.datasources.ingest("events", {"id": 1})
        assert result == {"datasource": "events", "count": 1}

        replaced = await client.datasources.replace("events", {})
        assert replaced == {"datasource": "events", "mode": "replace"}

        appended = await client.datasources.append("events", {})
        assert appended == {"datasource": "events", "mode": "append"}

    asyncio.run(run())


def test_client_sql(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(async_client_base, "AsyncTinybirdApi", _FakeAsyncApi)

    async def run() -> None:
        client = AsyncTinybirdClient({"base_url": "https://api.tinybird.co", "token": "token"})
        result = await client.sql("SELECT 1")
        assert result == {"sql": "SELECT 1"}

    asyncio.run(run())


def test_client_caches_api_per_token_and_closes_all(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(async_client_base, "AsyncTinybirdApi", _FakeAsyncApi)

    async def run() -> _FakeAsyncApi:
        client = AsyncTinybirdClient({"base_url": "https://api.tinybird.co", "token": "token"})
        api1 = await client._get_api("token")
        api2 = await client._get_api("token")
        assert api1 is api2
        await client.aclose()
        return api1

    api = asyncio.run(run())
    assert api.closed is True


def test_resolve_context_does_not_call_branch_api_when_dev_mode_off(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_if_called(*_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("get_or_create_branch should not be called when dev_mode is off")

    monkeypatch.setattr(async_client_base, "get_or_create_branch", fail_if_called)

    async def run() -> dict[str, Any]:
        client = AsyncTinybirdClient({"base_url": "https://api.tinybird.co", "token": "token"})
        return await client.get_context()

    context = asyncio.run(run())
    assert context["token"] == "token"
    assert context["is_branch_token"] is False


def test_resolve_branch_context_offloads_blocking_branch_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[Any, ...]] = []

    def fake_get_or_create_branch(
        config: dict[str, Any], name: str, options: Any = None
    ) -> dict[str, Any]:
        calls.append((config, name, options))
        return {"token": "branch-token", "name": name}

    monkeypatch.setattr(async_client_base, "get_or_create_branch", fake_get_or_create_branch)
    monkeypatch.setattr(
        async_client_base,
        "load_config_async",
        lambda _config_dir: {"is_main_branch": False, "tinybird_branch": "my-branch"},
    )
    monkeypatch.setattr(async_client_base, "is_preview_environment", lambda: False)

    async def run() -> dict[str, Any]:
        client = AsyncTinybirdClient(
            {"base_url": "https://api.tinybird.co", "token": "workspace-token", "dev_mode": True}
        )
        return await client.get_context()

    context = asyncio.run(run())
    assert context["token"] == "branch-token"
    assert context["is_branch_token"] is True
    assert context["branch_name"] == "my-branch"
    assert len(calls) == 1


def test_async_tokens_namespace_create_jwt(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_create_jwt_async(
        config: dict[str, Any], options: dict[str, Any]
    ) -> dict[str, str]:
        assert config["token"] == "token"
        assert options["name"] == "my-jwt"
        return {"token": "signed.jwt"}

    import tinybird_sdk.client.async_tokens as async_tokens_module

    monkeypatch.setattr(async_tokens_module, "create_jwt_async", fake_create_jwt_async)
    monkeypatch.setattr(async_client_base, "AsyncTinybirdApi", _FakeAsyncApi)

    async def run() -> dict[str, str]:
        client = AsyncTinybirdClient({"base_url": "https://api.tinybird.co", "token": "token"})
        return await client.tokens.create_jwt(
            {"name": "my-jwt", "expires_at": "2026-01-01T00:00:00Z"}
        )

    result = asyncio.run(run())
    assert result == {"token": "signed.jwt"}


def test_async_tokens_namespace_wraps_token_api_error(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_create_jwt_async(
        config: dict[str, Any], options: dict[str, Any]
    ) -> dict[str, str]:
        raise TokenApiError("denied", 403, "denied")

    import tinybird_sdk.client.async_tokens as async_tokens_module

    monkeypatch.setattr(async_tokens_module, "create_jwt_async", fake_create_jwt_async)

    async def run() -> None:
        client = AsyncTinybirdClient({"base_url": "https://api.tinybird.co", "token": "token"})
        with pytest.raises(TinybirdError, match="denied"):
            await client.tokens.create_jwt({"name": "x", "expires_at": "2026-01-01T00:00:00Z"})

    asyncio.run(run())
