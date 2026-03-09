from __future__ import annotations

from .._http import TINYBIRD_FROM_PARAM, tinybird_fetch, with_tinybird_from_param

# TS-style aliases for parity
with_tinybird_from_param = with_tinybird_from_param
tinybird_fetch = tinybird_fetch


def create_tinybird_fetcher():
    return tinybird_fetch


__all__ = [
    "TINYBIRD_FROM_PARAM",
    "with_tinybird_from_param",
    "with_tinybird_from_param",
    "tinybird_fetch",
    "tinybird_fetch",
    "create_tinybird_fetcher",
]
