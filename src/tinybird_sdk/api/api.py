from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timezone
from email.utils import parsedate_to_datetime
import math
import time
from typing import Any
from urllib.parse import urlencode, urljoin

from .._http import (
    HTTPClientError,
    create_multipart_body,
    detect_data_format,
    normalize_base_url,
    serialize_event_value,
    tinybird_fetch,
    to_query_value,
)

DEFAULT_TIMEOUT_MS = 30_000
DEFAULT_INGEST_RETRY_503_BASE_DELAY_MS = 200
DEFAULT_INGEST_RETRY_503_MAX_DELAY_MS = 3_000


@dataclass(frozen=True, slots=True)
class TinybirdApiConfig:
    base_url: str
    token: str
    timeout: int | None = None


class TinybirdApiError(Exception):
    def __init__(
        self,
        message: str,
        status_code: int,
        response_body: str | None = None,
        response: dict[str, Any] | None = None,
    ):
        super().__init__(message)
        self.status_code = status_code
        self.response_body = response_body
        self.response = response


class TinybirdApi:
    def __init__(self, config: TinybirdApiConfig | dict[str, Any]):
        normalized = config if isinstance(config, TinybirdApiConfig) else TinybirdApiConfig(**config)

        if not normalized.base_url:
            raise ValueError("base_url is required")
        if not normalized.token:
            raise ValueError("token is required")

        self._base_url = normalize_base_url(normalized.base_url)
        self._default_token = normalized.token
        self._default_timeout = normalized.timeout or DEFAULT_TIMEOUT_MS

    def request(
        self,
        path: str,
        *,
        method: str = "GET",
        token: str | None = None,
        headers: dict[str, str] | None = None,
        body: bytes | str | None = None,
        timeout: int | None = None,
    ):
        url = self._resolve_url(path)
        request_headers = dict(headers or {})
        if "Authorization" not in request_headers:
            request_headers["Authorization"] = f"Bearer {token or self._default_token}"

        timeout_seconds = self._timeout_seconds(timeout)

        try:
            return tinybird_fetch(
                url,
                method=method,
                headers=request_headers,
                body=body,
                timeout=timeout_seconds,
            )
        except HTTPClientError as error:
            raise TinybirdApiError(str(error), 0) from error

    def request_json(
        self,
        path: str,
        *,
        method: str = "GET",
        token: str | None = None,
        headers: dict[str, str] | None = None,
        body: bytes | str | None = None,
        timeout: int | None = None,
    ) -> Any:
        response = self.request(
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

    def query(
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
            elif isinstance(value, (datetime, date)):
                query_params.append((key, value.isoformat()))
            else:
                query_params.append((key, to_query_value(value)))

        query = urlencode(query_params, doseq=True)
        path = f"/v0/pipes/{endpoint_name}.json"
        if query:
            path = f"{path}?{query}"

        response = self.request(
            path,
            method="GET",
            token=options.get("token"),
            timeout=options.get("timeout"),
        )
        if not response.ok:
            self._raise_for_error(response.status_code, response.text)
        return response.json()

    def ingest(
        self,
        datasource_name: str,
        event: dict[str, Any],
        options: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self.ingest_batch(datasource_name, [event], options)

    def ingest_batch(
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

        max_retries = self._resolve_ingest_max_retries(options)
        retry_count = 0

        while True:
            response = self.request(
                f"/v0/events?{urlencode(query)}",
                method="POST",
                token=options.get("token"),
                headers={"Content-Type": "application/x-ndjson"},
                body=ndjson,
                timeout=options.get("timeout"),
            )
            if response.ok:
                return response.json()

            retry_429_delay_ms = self._resolve_retry_429_delay_ms(response.status_code, response.headers, max_retries, retry_count)
            if retry_429_delay_ms is not None:
                self._sleep_ms(retry_429_delay_ms)
                retry_count += 1
                continue

            retry_503_delay_ms = self._resolve_retry_503_delay_ms(response.status_code, max_retries, retry_count)
            if retry_503_delay_ms is not None:
                self._sleep_ms(retry_503_delay_ms)
                retry_count += 1
                continue

            self._raise_for_error(response.status_code, response.text)

    def sql(self, sql: str, options: dict[str, Any] | None = None) -> dict[str, Any]:
        options = options or {}
        response = self.request(
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

    def append_datasource(
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

        detected_format = detect_data_format(source_url or file_path)
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

        if source_url:
            body = urlencode({"url": source_url})
            response = self.request(
                f"/v0/datasources?{urlencode(query)}",
                method="POST",
                token=api_options.get("token"),
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                body=body,
                timeout=timeout,
            )
        else:
            with open(file_path, "rb") as fp:
                file_content = fp.read()
            content_type, multipart = create_multipart_body(
                files=[("csv", file_path, file_content, None)],
            )
            response = self.request(
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

    def delete_datasource(
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

        response = self.request(
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

    def truncate_datasource(
        self,
        datasource_name: str,
        options: dict[str, Any] | None = None,
        api_options: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        options = options or {}
        api_options = api_options or {}
        response = self.request(
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

    def create_token(
        self,
        body: dict[str, Any],
        options: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        options = options or {}

        path = "/v0/tokens/"
        expiration_time = options.get("expiration_time")
        if expiration_time is not None:
            path = f"{path}?{urlencode({'expiration_time': str(expiration_time)})}"

        response = self.request(
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

    def _resolve_ingest_max_retries(self, options: dict[str, Any]) -> int | None:
        value = options.get("maxRetries")
        if value is None:
            value = options.get("max_retries")
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
            raise ValueError("'maxRetries' must be a finite number")
        return max(0, math.floor(value))

    def _resolve_retry_429_delay_ms(
        self,
        status_code: int,
        headers: dict[str, str] | Any,
        max_retries: int | None,
        retry_count: int,
    ) -> int | None:
        if max_retries is None or status_code != 429 or retry_count >= max_retries:
            return None
        return self._resolve_retry_delay_from_headers(headers)

    def _resolve_retry_503_delay_ms(self, status_code: int, max_retries: int | None, retry_count: int) -> int | None:
        if max_retries is None or status_code != 503 or retry_count >= max_retries:
            return None
        return self._calculate_retry_503_delay_ms(retry_count)

    def _resolve_retry_delay_from_headers(self, headers: dict[str, str] | Any) -> int | None:
        retry_after = self._get_header(headers, "retry-after")
        retry_after_delay_ms = self._parse_retry_after_delay_ms(retry_after)
        if retry_after_delay_ms is not None:
            return retry_after_delay_ms

        rate_limit_reset = self._get_header(headers, "x-ratelimit-reset")
        return self._parse_rate_limit_reset_delay_ms(rate_limit_reset)

    def _get_header(self, headers: dict[str, str] | Any, header_name: str) -> str | None:
        if hasattr(headers, "get"):
            value = headers.get(header_name)
            if value is None:
                value = headers.get(header_name.lower())
            if value is None:
                value = headers.get(header_name.title())
            if isinstance(value, str):
                return value

        for key, value in dict(headers).items():
            if isinstance(key, str) and key.lower() == header_name.lower() and isinstance(value, str):
                return value
        return None

    def _parse_retry_after_delay_ms(self, value: str | None) -> int | None:
        if not value:
            return None

        trimmed = value.strip()
        try:
            seconds = float(trimmed)
            if math.isfinite(seconds):
                return max(0, math.floor(seconds * 1000))
        except ValueError:
            pass

        try:
            parsed_date = parsedate_to_datetime(trimmed)
        except (TypeError, ValueError):
            return None

        if parsed_date.tzinfo is None:
            parsed_date = parsed_date.replace(tzinfo=timezone.utc)

        return max(0, math.floor((parsed_date.timestamp() - time.time()) * 1000))

    def _parse_rate_limit_reset_delay_ms(self, value: str | None) -> int | None:
        if not value:
            return None
        try:
            numeric_value = float(value.strip())
        except ValueError:
            return None
        if not math.isfinite(numeric_value):
            return None
        return max(0, math.floor(numeric_value * 1000))

    def _calculate_retry_503_delay_ms(self, retry_count: int) -> int:
        return min(
            DEFAULT_INGEST_RETRY_503_MAX_DELAY_MS,
            DEFAULT_INGEST_RETRY_503_BASE_DELAY_MS * (2**retry_count),
        )

    def _sleep_ms(self, delay_ms: int) -> None:
        if delay_ms <= 0:
            return
        time.sleep(delay_ms / 1000.0)

    def _raise_for_error(self, status_code: int, body: str) -> None:
        parsed: dict[str, Any] | None = None
        try:
            parsed = json.loads(body) if body else None
        except json.JSONDecodeError:
            parsed = None

        message = ""
        if parsed and parsed.get("error"):
            message = str(parsed["error"])
        elif body:
            message = f"Request failed with status {status_code}: {body}"
        else:
            message = f"Request failed with status {status_code}"

        raise TinybirdApiError(
            message=message,
            status_code=status_code,
            response_body=body or None,
            response=parsed,
        )


def create_tinybird_api(config: TinybirdApiConfig | dict[str, Any]) -> TinybirdApi:
    return TinybirdApi(config)


create_tinybird_api_wrapper = create_tinybird_api

# Aliases for backwards compatibility
create_tinybird_api = create_tinybird_api
create_tinybird_api_wrapper = create_tinybird_api
