from __future__ import annotations

import json
import mimetypes
import os
import uuid
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Mapping
from urllib.error import HTTPError
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse
from urllib.request import Request, urlopen

TINYBIRD_FROM_PARAM = "ts-sdk"


@dataclass(slots=True)
class HTTPResponse:
    status_code: int
    headers: Mapping[str, str]
    body: bytes

    @property
    def ok(self) -> bool:
        return 200 <= self.status_code < 300

    @property
    def text(self) -> str:
        return self.body.decode("utf-8", errors="replace")

    def json(self) -> Any:
        if not self.body:
            return {}
        return json.loads(self.text)


class HTTPClientError(Exception):
    def __init__(self, message: str, status_code: int | None = None, body: str | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.body = body


def with_tinybird_from_param(url: str) -> str:
    parsed = urlparse(url)
    query_pairs = parse_qsl(parsed.query, keep_blank_values=True)
    query_dict: dict[str, list[str]] = {}
    for key, value in query_pairs:
        query_dict.setdefault(key, []).append(value)
    query_dict["from"] = [TINYBIRD_FROM_PARAM]

    new_query: list[tuple[str, str]] = []
    for key, values in query_dict.items():
        for value in values:
            new_query.append((key, value))

    return urlunparse(parsed._replace(query=urlencode(new_query, doseq=True)))


def tinybird_fetch(
    url: str,
    *,
    method: str = "GET",
    headers: Mapping[str, str] | None = None,
    body: bytes | str | None = None,
    timeout: float | None = None,
) -> HTTPResponse:
    request_body = body.encode("utf-8") if isinstance(body, str) else body
    request = Request(
        with_tinybird_from_param(url),
        method=method,
        headers=dict(headers or {}),
        data=request_body,
    )

    try:
        with urlopen(request, timeout=timeout) as response:
            response_body = response.read()
            return HTTPResponse(
                status_code=response.status,
                headers=dict(response.headers.items()),
                body=response_body,
            )
    except HTTPError as error:
        return HTTPResponse(
            status_code=error.code,
            headers=dict(error.headers.items()) if error.headers else {},
            body=error.read() if error.fp else b"",
        )
    except Exception as error:  # pragma: no cover - network-level failures
        raise HTTPClientError(str(error)) from error


def normalize_base_url(base_url: str) -> str:
    return base_url.rstrip("/")


def to_query_value(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def serialize_event_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, int) and abs(value) > 2**53:
        return str(value)
    if isinstance(value, dict):
        return {k: serialize_event_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [serialize_event_value(v) for v in value]
    if isinstance(value, tuple):
        return [serialize_event_value(v) for v in value]
    if isinstance(value, set):
        return [serialize_event_value(v) for v in value]
    if hasattr(value, "items") and callable(value.items):
        return {k: serialize_event_value(v) for k, v in value.items()}
    return value


def detect_data_format(source: str) -> str | None:
    path_only = source.split("?", 1)[0]
    extension = path_only.rsplit(".", 1)[-1].lower() if "." in path_only else ""
    if extension == "csv":
        return "csv"
    if extension in {"ndjson", "jsonl"}:
        return "ndjson"
    if extension == "parquet":
        return "parquet"
    return None


def create_multipart_body(
    *,
    files: list[tuple[str, str, bytes, str | None]],
    fields: Mapping[str, str] | None = None,
) -> tuple[str, bytes]:
    boundary = f"----tinybird-{uuid.uuid4().hex}"
    lines: list[bytes] = []

    for key, value in (fields or {}).items():
        lines.extend(
            [
                f"--{boundary}".encode(),
                f'Content-Disposition: form-data; name="{key}"'.encode(),
                b"",
                value.encode(),
            ]
        )

    for field_name, filename, content, explicit_content_type in files:
        content_type = explicit_content_type or mimetypes.guess_type(filename)[0] or "application/octet-stream"
        lines.extend(
            [
                f"--{boundary}".encode(),
                (
                    f'Content-Disposition: form-data; name="{field_name}"; '
                    f'filename="{os.path.basename(filename)}"'
                ).encode(),
                f"Content-Type: {content_type}".encode(),
                b"",
                content,
            ]
        )

    lines.extend([f"--{boundary}--".encode(), b""])
    body = b"\r\n".join(lines)
    return f"multipart/form-data; boundary={boundary}", body
