from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .fetcher import tinybird_fetch

DEFAULT_HOST = "https://api.tinybird.co"


@dataclass(frozen=True, slots=True)
class TinybirdRegion:
    name: str
    api_host: str
    provider: str


class RegionsApiError(Exception):
    def __init__(self, message: str, status: int | None = None, body: Any = None):
        super().__init__(message)
        self.status = status
        self.body = body


def fetch_regions() -> list[TinybirdRegion]:
    response = tinybird_fetch(f"{DEFAULT_HOST}/v0/regions", method="GET")
    if not response.ok:
        raise RegionsApiError(
            f"Failed to fetch regions: {response.status_code}",
            response.status_code,
            response.text,
        )

    return [TinybirdRegion(**region) for region in response.json().get("regions", [])]
