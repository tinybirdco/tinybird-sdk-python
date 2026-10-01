from __future__ import annotations

import asyncio
from typing import Any

import pytest

from tinybird_sdk import AsyncTinybird, define_datasource, t


def test_async_tinybird_class_is_public() -> None:
    client = AsyncTinybird({"datasources": {}, "pipes": {}})
    assert isinstance(client, AsyncTinybird)


def test_async_datasource_name_does_not_overwrite_internal_client_state() -> None:
    events = define_datasource("events", {"schema": {"id": t.string()}})
    client = AsyncTinybird({"datasources": {"_client": events}, "pipes": {}})

    assert getattr(client, "_client") is not None
    with pytest.raises(ValueError, match="Client not initialized"):
        _ = client.client


class _FakeAsyncClient:
    def __init__(self, config: dict[str, Any]):
        self.config = config

        class _Datasources:
            async def ingest(_self, datasource_name: str, event: dict[str, Any]) -> dict[str, Any]:
                return {"datasource": datasource_name, "event": event}

        self.datasources = _Datasources()

    async def aclose(self) -> None:
        pass


def test_async_datasource_accessor_ingests_through_client(monkeypatch: pytest.MonkeyPatch) -> None:
    import tinybird_sdk.client.async_base as async_client_base_module
    import tinybird_sdk.client.preview as preview_module

    monkeypatch.setattr(
        async_client_base_module, "create_async_client", lambda config: _FakeAsyncClient(config)
    )
    monkeypatch.setattr(
        preview_module, "resolve_token", lambda options: options.get("token") or "resolved"
    )

    events = define_datasource("events", {"schema": {"id": t.string()}})
    client = AsyncTinybird(
        {
            "datasources": {"events": events},
            "pipes": {},
            "base_url": "https://api.tinybird.co",
            "token": "t",
        }
    )

    async def run() -> dict[str, Any]:
        return await client.events.ingest({"id": "1"})

    result = asyncio.run(run())
    assert result == {"datasource": "events", "event": {"id": "1"}}
