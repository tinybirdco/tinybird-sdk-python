from __future__ import annotations

from typing import Any

import pytest

import tinybird_sdk.client.base as client_base
import tinybird_sdk.client.tokens as client_tokens
from tinybird_sdk.api.api import TinybirdApiError
from tinybird_sdk.api.tokens import TokenApiError
from tinybird_sdk.client.base import TinybirdClient
from tinybird_sdk.client.types import TinybirdError


def test_client_constructor_validation() -> None:
    with pytest.raises(ValueError, match="base_url is required"):
        TinybirdClient({"token": "x"})
    with pytest.raises(ValueError, match="token is required"):
        TinybirdClient({"base_url": "https://api.tinybird.co"})


def test_client_query_and_api_error_mapping(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeApi:
        def __init__(self, config: dict[str, Any]):
            self.config = config

        def query(self, pipe_name: str, params: dict[str, Any], options: dict[str, Any]) -> dict[str, Any]:
            if pipe_name == "boom":
                raise TinybirdApiError("boom", 500, '{"error":"boom"}', {"error": "boom"})
            return {"data": [{"ok": True}], "pipe": pipe_name, "params": params}

    monkeypatch.setattr(client_base, "TinybirdApi", FakeApi)

    client = TinybirdClient({"base_url": "https://api.tinybird.co", "token": "token"})
    ok = client.query("top_events", {"limit": 1})
    assert ok["pipe"] == "top_events"
    assert ok["params"] == {"limit": 1}

    with pytest.raises(TinybirdError, match="boom"):
        client.query("boom")


def test_client_branch_context_uses_branch_token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(client_base, "is_preview_environment", lambda: False)
    monkeypatch.setattr(
        client_base,
        "load_config_async",
        lambda *_args, **_kwargs: {
            "git_branch": "feature/alpha",
            "tinybird_branch": "feature_alpha",
            "is_main_branch": False,
        },
    )
    monkeypatch.setattr(
        client_base,
        "get_or_create_branch",
        lambda *_args, **_kwargs: {"token": "branch_token"},
    )

    client = TinybirdClient(
        {
            "base_url": "https://api.tinybird.co",
            "token": "workspace_token",
            "dev_mode": True,
        }
    )

    context = client.get_context()
    assert context["token"] == "branch_token"
    assert context["is_branch_token"] is True
    assert context["branch_name"] == "feature_alpha"


def test_tokens_namespace_wraps_token_api_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        client_tokens,
        "create_jwt",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(TokenApiError("not allowed", 403, {"error": "x"})),
    )

    client = TinybirdClient({"base_url": "https://api.tinybird.co", "token": "workspace_token"})
    with pytest.raises(TinybirdError, match="not allowed"):
        client.tokens.create_jwt({"name": "token", "expires_at": 1, "scopes": []})
