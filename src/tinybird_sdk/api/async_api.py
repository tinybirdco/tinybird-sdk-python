from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import urlencode, urljoin

from .._http import (
    HTTPClientError,
    HTTPResponse,
    create_multipart_body,
    detect_data_format,
    normalize_base_url,
    serialize_event_value,
    tinybird_fetch_async,
    to_query_value,
)
from ._shared import (
    build_api_error_info,
    get_header,
    resolve_ingest_max_retries,
    resolve_retry_429_delay_ms,
    resolve_retry_503_delay_ms,
)
from .api import (
    DEFAULT_INGEST_RETRY_503_BASE_DELAY_MS,
    DEFAULT_INGEST_RETRY_503_MAX_DELAY_MS,
    DEFAULT_TIMEOUT_MS,
    TinybirdApiConfig,
    TinybirdApiError,
)

if TYPE_CHECKING:
    import httpx


class AsyncTinybirdApi:
    """Async counterpart to `TinybirdApi`. Same method surface and semantics,
    backed by `httpx.AsyncClient` instead of blocking `urllib` calls so it is
    safe to use from async frameworks (FastAPI, aiohttp) without blocking the
    event loop. Retry/error logic is shared with `TinybirdApi` via `_shared.py`
    so the two clients can't drift apart.

    Holds a single `httpx.AsyncClient` for connection pooling across calls;
    close it with `aclose()` or use as an async context manager.
    """

    def __init__(self, config: TinybirdApiConfig | dict[str, Any]):
        normalized = (
            config if isinstance(config, TinybirdApiConfig) else TinybirdApiConfig(**config)
        )

        if not normalized.base_url:
            raise ValueError("base_url is required")
        if not normalized.token:
            raise ValueError("token is required")

        self._base_url = normalize_base_url(normalized.base_url)
        self._default_token = normalized.token
        self._default_timeout = normalized.timeout or DEFAULT_TIMEOUT_MS
        self._client: "httpx.AsyncClient | None" = None

    def _get_http_client(self) -> "httpx.AsyncClient":
        if self._client is None:
            import httpx

            self._client = httpx.AsyncClient()
        return self._client

    async def aclose(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def __aenter__(self) -> "AsyncTinybirdApi":
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.aclose()

    async def request(
        self,
        path: str,
        *,
        method: str = "GET",
        token: str | None = None,
        headers: dict[str, str] | None = None,
        body: bytes | str | None = None,
        timeout: int | None = None,
    ) -> HTTPResponse:
        url = self._resolve_url(path)
        request_headers = dict(headers or {})
        if "Authorization" not in request_headers:
            request_headers["Authorization"] = f"Bearer {token or self._default_token}"

        timeout_seconds = self._timeout_seconds(timeout)

        try:
            return await tinybird_fetch_async(
                self._get_http_client(),
                url,
                method=method,
                headers=request_headers,
                body=body,
                timeout=timeout_seconds,
            )
        except HTTPClientError as error:
            raise TinybirdApiError(str(error), 0) from error

    async def request_json(
        self,
        path: str,
        *,
        method: str = "GET",
        token: str | None = None,
        headers: dict[str, str] | None = None,
        body: bytes | str | None = None,
        timeout: int | None = None,
    ) -> Any:
        response = await self.request(
            path,
            method=method,
            token=token,
            headers=headers,
            body=body,
            timeout=timeout,
        )
        if not response.ok:
            self._raise_for_error(response.status_code, response.text)
        return response.json()

    async def query(
        self,
        endpoint_name: str,
        params: dict[str, Any] | None = None,
        options: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        options = options or {}
        params = params or {}

        query_params: list[tuple[str, str]] = []
        for key, value in params.items():
            if value is None:
                continue
            if isinstance(value, (list, tuple)):
                for item in value:
                    query_params.append((key, to_query_value(item)))
            else:
                query_params.append((key, to_query_value(value)))

        query = urlencode(query_params, doseq=True)
        path = f"/v0/pipes/{endpoint_name}.json"
        if query:
            path = f"{path}?{query}"

        response = await self.request(
            path,
            method="GET",
            token=options.get("token"),
            timeout=options.get("timeout"),
        )
        if not response.ok:
            self._raise_for_error(response.status_code, response.text)
        return response.json()

    async def ingest(
        self,
        datasource_name: str,
        event: dict[str, Any],
        options: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return await self.ingest_batch(datasource_name, [event], options)

    async def ingest_batch(
        self,
        datasource_name: str,
        events: list[dict[str, Any]],
        options: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        options = options or {}

        if not events:
            return {"successful_rows": 0, "quarantined_rows": 0}

        serialized_rows = [json.dumps(self._serialize_event(event)) for event in events]
        ndjson = "\n".join(serialized_rows)

        query = {"name": datasource_name}
        if options.get("wait", True):
            query["wait"] = "true"

        max_retries = resolve_ingest_max_retries(options)
        retry_count = 0

        while True:
            response = await self.request(
                f"/v0/events?{urlencode(query)}",
                method="POST",
                token=options.get("token"),
                headers={"Content-Type": "application/x-ndjson"},
                body=ndjson,
                timeout=options.get("timeout"),
            )
            if response.ok:
                return response.json()

            retry_429_delay_ms = resolve_retry_429_delay_ms(
                response.status_code, response.headers, max_retries, retry_count
            )
            if retry_429_delay_ms is not None:
                await self._sleep_ms(retry_429_delay_ms)
                retry_count += 1
                continue

            retry_503_delay_ms = resolve_retry_503_delay_ms(
                response.status_code,
                max_retries,
                retry_count,
                base_delay_ms=DEFAULT_INGEST_RETRY_503_BASE_DELAY_MS,
                max_delay_ms=DEFAULT_INGEST_RETRY_503_MAX_DELAY_MS,
            )
            if retry_503_delay_ms is not None:
                await self._sleep_ms(retry_503_delay_ms)
                retry_count += 1
                continue

            self._raise_for_error(response.status_code, response.text)

    async def sql(self, sql: str, options: dict[str, Any] | None = None) -> dict[str, Any]:
        options = options or {}
        response = await self.request(
            "/v0/sql",
            method="POST",
            token=options.get("token"),
            headers={"Content-Type": "text/plain"},
            body=sql,
            timeout=options.get("timeout"),
        )
        if not response.ok:
            self._raise_for_error(response.status_code, response.text)
        return response.json()

    async def append_datasource(
        self,
        datasource_name: str,
        options: dict[str, Any],
        api_options: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        api_options = api_options or {}
        source_url = options.get("url")
        file_path = options.get("file")

        if not source_url and not file_path:
            raise ValueError("Either 'url' or 'file' must be provided in options")
        if source_url and file_path:
            raise ValueError("Only one of 'url' or 'file' can be provided, not both")

        query: dict[str, str] = {
            "name": datasource_name,
            "mode": api_options.get("mode", "append"),
        }

        source_url_str = source_url if isinstance(source_url, str) else None
        file_path_str = file_path if isinstance(file_path, str) else None
        source_ref = source_url_str or file_path_str
        detected_format = detect_data_format(source_ref) if source_ref else None
        if detected_format:
            query["format"] = detected_format

        csv_dialect = options.get("csv_dialect") or {}
        if csv_dialect.get("delimiter"):
            query["dialect_delimiter"] = csv_dialect["delimiter"]
        if csv_dialect.get("new_line"):
            query["dialect_new_line"] = csv_dialect["new_line"]
        if csv_dialect.get("escape_char"):
            query["dialect_escapechar"] = csv_dialect["escape_char"]

        timeout = options.get("timeout", api_options.get("timeout"))

        if source_url_str:
            body = urlencode({"url": source_url_str})
            response = await self.request(
                f"/v0/datasources?{urlencode(query)}",
                method="POST",
                token=api_options.get("token"),
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                body=body,
                timeout=timeout,
            )
        else:
            if not file_path_str:
                raise ValueError("'file' must be a valid string path")
            # Local disk read, offloaded to a thread so it never blocks the event loop.
            file_content = await asyncio.to_thread(Path(file_path_str).read_bytes)
            content_type, multipart = create_multipart_body(
                files=[("csv", file_path_str, file_content, None)],
            )
            response = await self.request(
                f"/v0/datasources?{urlencode(query)}",
                method="POST",
                token=api_options.get("token"),
                headers={"Content-Type": content_type},
                body=multipart,
                timeout=timeout,
            )

        if not response.ok:
            self._raise_for_error(response.status_code, response.text)
        return response.json()

    async def delete_datasource(
        self,
        datasource_name: str,
        options: dict[str, Any],
        api_options: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        api_options = api_options or {}
        delete_condition = (options.get("delete_condition") or "").strip()
        if not delete_condition:
            raise ValueError("'delete_condition' must be provided in options")

        body = {"delete_condition": delete_condition}
        dry_run = options.get("dry_run", api_options.get("dry_run"))
        if dry_run is not None:
            body["dry_run"] = str(dry_run).lower()

        response = await self.request(
            f"/v0/datasources/{datasource_name}/delete",
            method="POST",
            token=api_options.get("token"),
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            body=urlencode(body),
            timeout=options.get("timeout", api_options.get("timeout")),
        )
        if not response.ok:
            self._raise_for_error(response.status_code, response.text)
        return response.json()

    async def truncate_datasource(
        self,
        datasource_name: str,
        options: dict[str, Any] | None = None,
        api_options: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        options = options or {}
        api_options = api_options or {}
        response = await self.request(
            f"/v0/datasources/{datasource_name}/truncate",
            method="POST",
            token=api_options.get("token"),
            timeout=options.get("timeout", api_options.get("timeout")),
        )
        if not response.ok:
            self._raise_for_error(response.status_code, response.text)

        if not response.text.strip():
            return {}
        try:
            return response.json()
        except json.JSONDecodeError:
            return {}

    async def create_token(
        self,
        body: dict[str, Any],
        options: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        options = options or {}

        path = "/v0/tokens/"
        expiration_time = options.get("expiration_time")
        if expiration_time is not None:
            path = f"{path}?{urlencode({'expiration_time': str(expiration_time)})}"

        response = await self.request(
            path,
            method="POST",
            token=options.get("token"),
            headers={"Content-Type": "application/json"},
            body=json.dumps(body),
            timeout=options.get("timeout"),
        )
        if not response.ok:
            self._raise_for_error(response.status_code, response.text)
        return response.json()

    def _timeout_seconds(self, timeout_ms: int | None) -> float:
        timeout = timeout_ms if timeout_ms is not None else self._default_timeout
        return max(timeout / 1000.0, 0.001)

    def _resolve_url(self, path: str) -> str:
        if path.startswith("http://") or path.startswith("https://"):
            return path
        return urljoin(f"{self._base_url}/", path.lstrip("/"))

    def _serialize_event(self, event: dict[str, Any]) -> dict[str, Any]:
        return {key: serialize_event_value(value) for key, value in event.items()}

    def _get_header(self, headers: dict[str, str] | Any, header_name: str) -> str | None:
        return get_header(headers, header_name)

    async def _sleep_ms(self, delay_ms: int) -> None:
        if delay_ms <= 0:
            return
        await asyncio.sleep(delay_ms / 1000.0)

    def _raise_for_error(self, status_code: int, body: str) -> None:
        info = build_api_error_info(status_code, body)
        raise TinybirdApiError(
            message=info.message,
            status_code=info.status_code,
            response_body=info.response_body,
            response=info.response,
        )


def create_async_tinybird_api(config: TinybirdApiConfig | dict[str, Any]) -> AsyncTinybirdApi:
    return AsyncTinybirdApi(config)
