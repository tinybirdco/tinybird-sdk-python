from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path

from ..api.regions import TinybirdRegion, fetch_regions


FALLBACK_REGIONS: list[TinybirdRegion] = [
    TinybirdRegion(name="EU (GCP)", api_host="https://api.tinybird.co", provider="gcp"),
    TinybirdRegion(name="US East (AWS)", api_host="https://api.us-east-1.aws.tinybird.co", provider="aws"),
]


@dataclass(frozen=True, slots=True)
class RegionSelectionResult:
    success: bool
    api_host: str | None = None
    region_name: str | None = None
    cancelled: bool = False


def select_region(default_api_host: str | None = None) -> RegionSelectionResult:
    try:
        regions = fetch_regions()
    except Exception:
        regions = FALLBACK_REGIONS

    if not regions:
        regions = FALLBACK_REGIONS

    regions = sorted(regions, key=lambda region: (region.provider != "gcp", region.name))

    if default_api_host:
        match = next((region for region in regions if region.api_host.rstrip("/") == default_api_host.rstrip("/")), None)
        if match:
            return RegionSelectionResult(success=True, api_host=match.api_host, region_name=match.name)

    env_region = os.getenv("TINYBIRD_REGION")
    if env_region:
        match = next((region for region in regions if env_region in {region.name, region.api_host}), None)
        if match:
            return RegionSelectionResult(success=True, api_host=match.api_host, region_name=match.name)

    # Non-interactive default selection.
    chosen = regions[0]
    return RegionSelectionResult(success=True, api_host=chosen.api_host, region_name=chosen.name)


def get_api_host_with_region_selection(config_path: str | None) -> dict[str, object] | None:
    existing_base_url: str | None = None

    if config_path and config_path.endswith(".json"):
        try:
            config = json.loads(Path(config_path).read_text(encoding="utf-8"))
            existing_base_url = config.get("base_url")
        except Exception:
            pass

    result = select_region(existing_base_url)
    if not result.success or not result.api_host:
        return None

    return {
        "api_host": result.api_host,
        "from_config": False,
    }


__all__ = ["RegionSelectionResult", "select_region", "get_api_host_with_region_selection"]
