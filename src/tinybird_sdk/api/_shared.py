from __future__ import annotations

import json
import math
import time
from dataclasses import dataclass
from datetime import timezone
from email.utils import parsedate_to_datetime
from typing import Any

"""Transport-agnostic helpers shared by TinybirdApi and AsyncTinybirdApi.

Kept separate from api.py so the sync and async clients can share identical
retry/error semantics without one importing internals from the other.
"""


@dataclass(frozen=True, slots=True)
class ApiErrorInfo:
    message: str
    status_code: int
    response_body: str | None
    response: dict[str, Any] | None


def resolve_ingest_max_retries(options: dict[str, Any]) -> int | None:
    value = options.get("maxRetries")
    if value is None:
        value = options.get("max_retries")
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError("'maxRetries' must be a finite number")
    return max(0, math.floor(value))


def get_header(headers: dict[str, str] | Any, header_name: str) -> str | None:
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


def parse_retry_after_delay_ms(value: str | None) -> int | None:
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


def parse_rate_limit_reset_delay_ms(value: str | None) -> int | None:
    if not value:
        return None
    try:
        numeric_value = float(value.strip())
    except ValueError:
        return None
    if not math.isfinite(numeric_value):
        return None
    return max(0, math.floor(numeric_value * 1000))


def resolve_retry_delay_from_headers(headers: dict[str, str] | Any) -> int | None:
    retry_after = get_header(headers, "retry-after")
    retry_after_delay_ms = parse_retry_after_delay_ms(retry_after)
    if retry_after_delay_ms is not None:
        return retry_after_delay_ms

    rate_limit_reset = get_header(headers, "x-ratelimit-reset")
    return parse_rate_limit_reset_delay_ms(rate_limit_reset)


def resolve_retry_429_delay_ms(
    status_code: int,
    headers: dict[str, str] | Any,
    max_retries: int | None,
    retry_count: int,
) -> int | None:
    if max_retries is None or status_code != 429 or retry_count >= max_retries:
        return None
    return resolve_retry_delay_from_headers(headers)


def calculate_retry_503_delay_ms(
    retry_count: int,
    *,
    base_delay_ms: int,
    max_delay_ms: int,
) -> int:
    return min(max_delay_ms, base_delay_ms * (2**retry_count))


def resolve_retry_503_delay_ms(
    status_code: int,
    max_retries: int | None,
    retry_count: int,
    *,
    base_delay_ms: int,
    max_delay_ms: int,
) -> int | None:
    if max_retries is None or status_code != 503 or retry_count >= max_retries:
        return None
    return calculate_retry_503_delay_ms(
        retry_count, base_delay_ms=base_delay_ms, max_delay_ms=max_delay_ms
    )


def build_api_error_info(status_code: int, body: str) -> ApiErrorInfo:
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

    return ApiErrorInfo(
        message=message,
        status_code=status_code,
        response_body=body or None,
        response=parsed,
    )
