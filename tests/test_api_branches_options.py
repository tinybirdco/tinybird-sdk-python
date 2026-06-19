from __future__ import annotations

from typing import Any
from urllib.parse import parse_qs, urlparse

import pytest

import tinybird_sdk.api.branches as branches_module
from tinybird_sdk.api.branches import CreateBranchOptions, clear_branch, create_branch


class _FakeResponse:
    def __init__(self, status_code: int, payload: dict[str, Any]):
        self.status_code = status_code
        self._payload = payload
        self.text = ""

    @property
    def ok(self) -> bool:
        return 200 <= self.status_code < 300

    def json(self) -> dict[str, Any]:
        return self._payload


def test_create_branch_uses_last_partition_data_query(monkeypatch: pytest.MonkeyPatch) -> None:
    called_urls: list[str] = []

    def fake_fetch(url: str, **_kwargs: Any) -> _FakeResponse:
        called_urls.append(url)
        if "/v1/environments?" in url:
            return _FakeResponse(200, {"job": {"id": "job-1"}})
        if "/v0/jobs/" in url:
            return _FakeResponse(200, {"status": "done"})
        return _FakeResponse(
            200, {"id": "b1", "name": "x", "created_at": "2024-01-01T00:00:00Z", "token": "p.test"}
        )

    monkeypatch.setattr(branches_module, "tinybird_fetch", fake_fetch)
    create_branch(
        {"base_url": "https://api.tinybird.co", "token": "p.test"},
        "x",
        options=CreateBranchOptions(branch_data_mode="last_partition"),
    )

    parsed = urlparse(called_urls[0])
    query = parse_qs(parsed.query)
    assert parsed.path == "/v1/environments"
    assert query == {"name": ["x"], "data": ["last_partition"]}


def test_create_branch_without_options_keeps_default_query(monkeypatch: pytest.MonkeyPatch) -> None:
    called_urls: list[str] = []

    def fake_fetch(url: str, **_kwargs: Any) -> _FakeResponse:
        called_urls.append(url)
        if "/v1/environments?" in url:
            return _FakeResponse(200, {"job": {"id": "job-1"}})
        if "/v0/jobs/" in url:
            return _FakeResponse(200, {"status": "done"})
        return _FakeResponse(
            200, {"id": "b1", "name": "x", "created_at": "2024-01-01T00:00:00Z", "token": "p.test"}
        )

    monkeypatch.setattr(branches_module, "tinybird_fetch", fake_fetch)
    create_branch({"base_url": "https://api.tinybird.co", "token": "p.test"}, "x")

    parsed = urlparse(called_urls[0])
    query = parse_qs(parsed.query)
    assert parsed.path == "/v1/environments"
    assert query == {"name": ["x"]}
    assert "data" not in query
    assert "ignore_datasources" not in query


def test_clear_branch_forwards_create_options(monkeypatch: pytest.MonkeyPatch) -> None:
    captured_options: list[CreateBranchOptions | None] = []

    monkeypatch.setattr(branches_module, "delete_branch", lambda *_args, **_kwargs: None)

    def fake_create_branch(
        _config: dict[str, Any], _name: str, options: CreateBranchOptions | None = None
    ) -> Any:
        captured_options.append(options)
        return {"id": "b1", "name": "x", "created_at": "2024-01-01T00:00:00Z", "token": "p.test"}

    monkeypatch.setattr(branches_module, "create_branch", fake_create_branch)

    clear_branch(
        {"base_url": "https://api.tinybird.co", "token": "p.test"},
        "x",
        options=CreateBranchOptions(branch_data_mode="last_partition"),
    )

    assert len(captured_options) == 1
    assert captured_options[0] is not None
    assert captured_options[0].branch_data_mode == "last_partition"
