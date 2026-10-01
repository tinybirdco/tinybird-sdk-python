from __future__ import annotations

from typing import Any
from urllib.parse import parse_qs, urlparse

import pytest

import tinybird_sdk.api.api as api_module
from tinybird_sdk.api.api import TinybirdApi, TinybirdApiError
from tinybird_sdk.client.base import TinybirdClient
from tinybird_sdk.client.types import TinybirdError


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


def _capture_fetch(captured: dict[str, Any], payload: dict[str, Any]) -> Any:
    def fake_fetch(url: str, **kwargs: Any) -> _FakeResponse:
        captured["url"] = url
        captured["method"] = kwargs.get("method")
        return _FakeResponse(200, payload)

    return fake_fetch


def _make_api() -> TinybirdApi:
    return TinybirdApi({"base_url": "https://api.tinybird.co", "token": "p.test"})


def test_list_jobs_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}
    monkeypatch.setattr(
        api_module, "tinybird_fetch", _capture_fetch(captured, {"jobs": [{"id": "job-1"}]})
    )

    result = _make_api().list_jobs()

    assert result == {"jobs": [{"id": "job-1"}]}
    assert captured["method"] == "GET"
    parsed = urlparse(captured["url"])
    assert parsed.path == "/v0/jobs"
    assert parsed.query == ""


def test_list_jobs_with_filters(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}
    monkeypatch.setattr(api_module, "tinybird_fetch", _capture_fetch(captured, {"jobs": []}))

    _make_api().list_jobs({"status": "error", "kind": "import"})

    parsed = urlparse(captured["url"])
    assert parse_qs(parsed.query) == {"status": ["error"], "kind": ["import"]}


def test_get_job(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}
    monkeypatch.setattr(
        api_module, "tinybird_fetch", _capture_fetch(captured, {"id": "job-1", "status": "done"})
    )

    result = _make_api().get_job("job-1")

    assert result == {"id": "job-1", "status": "done"}
    assert captured["method"] == "GET"
    assert captured["url"].endswith("/v0/jobs/job-1")


def test_cancel_job(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}
    monkeypatch.setattr(
        api_module,
        "tinybird_fetch",
        _capture_fetch(captured, {"id": "job-1", "status": "cancelling"}),
    )

    result = _make_api().cancel_job("job-1")

    assert result == {"id": "job-1", "status": "cancelling"}
    assert captured["method"] == "POST"
    assert captured["url"].endswith("/v0/jobs/job-1/cancel")


def test_retry_job(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}
    monkeypatch.setattr(
        api_module, "tinybird_fetch", _capture_fetch(captured, {"id": "job-2", "status": "waiting"})
    )

    result = _make_api().retry_job("job-1")

    assert result == {"id": "job-2", "status": "waiting"}
    assert captured["method"] == "POST"
    assert captured["url"].endswith("/v0/jobs/job-1/retry")


def test_job_error_response_raises_api_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_fetch(_url: str, **_kwargs: Any) -> _FakeResponse:
        response = _FakeResponse(404, {"error": "Job not found"})
        response.text = '{"error": "Job not found"}'
        return response

    monkeypatch.setattr(api_module, "tinybird_fetch", fake_fetch)

    with pytest.raises(TinybirdApiError, match="Job not found"):
        _make_api().get_job("missing")


def test_client_jobs_namespace_list_and_get(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeApi:
        def __init__(self, _config: dict[str, Any]):
            pass

        def list_jobs(self, options: dict[str, Any]) -> dict[str, Any]:
            return {"jobs": [], "options": options}

        def get_job(self, job_id: str, options: dict[str, Any]) -> dict[str, Any]:
            return {"id": job_id, "options": options}

    import tinybird_sdk.client.base as client_base

    monkeypatch.setattr(client_base, "TinybirdApi", FakeApi)

    client = TinybirdClient({"base_url": "https://api.tinybird.co", "token": "p.test"})

    assert client.jobs.list({"status": "error"}) == {"jobs": [], "options": {"status": "error"}}
    assert client.jobs.get("job-1") == {"id": "job-1", "options": {}}


def test_client_jobs_namespace_cancel_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeApi:
        def __init__(self, _config: dict[str, Any]):
            pass

        def cancel_job(self, job_id: str, options: dict[str, Any]) -> dict[str, Any]:
            return {"id": job_id, "status": "cancelling"}

        def retry_job(self, job_id: str, options: dict[str, Any]) -> dict[str, Any]:
            return {"id": job_id, "status": "waiting"}

    import tinybird_sdk.client.base as client_base

    monkeypatch.setattr(client_base, "TinybirdApi", FakeApi)

    client = TinybirdClient({"base_url": "https://api.tinybird.co", "token": "p.test"})

    assert client.jobs.cancel("job-1") == {"id": "job-1", "status": "cancelling"}
    assert client.jobs.retry("job-1") == {"id": "job-1", "status": "waiting"}


def test_client_jobs_namespace_wraps_api_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeApi:
        def __init__(self, _config: dict[str, Any]):
            pass

        def retry_job(self, job_id: str, options: dict[str, Any]) -> dict[str, Any]:
            raise TinybirdApiError(
                "Job is not eligible for retry",
                400,
                '{"error":"Job is not eligible for retry"}',
                {"error": "Job is not eligible for retry"},
            )

    import tinybird_sdk.client.base as client_base

    monkeypatch.setattr(client_base, "TinybirdApi", FakeApi)

    client = TinybirdClient({"base_url": "https://api.tinybird.co", "token": "p.test"})

    with pytest.raises(TinybirdError, match="not eligible for retry"):
        client.jobs.retry("job-1")
