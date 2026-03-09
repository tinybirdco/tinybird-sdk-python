from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlparse


@dataclass(frozen=True, slots=True)
class RegionInfo:
    provider: str
    region: str


_API_REGION_MAP = {
    "api.tinybird.co": RegionInfo(provider="gcp", region="europe-west3"),
    "api.us-east.tinybird.co": RegionInfo(provider="gcp", region="us-east4"),
    "api.eu-central-1.aws.tinybird.co": RegionInfo(provider="aws", region="eu-central-1"),
    "api.us-east-1.aws.tinybird.co": RegionInfo(provider="aws", region="us-east-1"),
    "api.us-west-2.aws.tinybird.co": RegionInfo(provider="aws", region="us-west-2"),
}


def parse_api_url(api_url: str) -> RegionInfo | None:
    try:
        hostname = urlparse(api_url).hostname
    except Exception:
        return None
    if not hostname:
        return None
    return _API_REGION_MAP.get(hostname)


def get_dashboard_url(api_url: str, workspace_name: str) -> str | None:
    region = parse_api_url(api_url)
    if not region:
        return None
    return f"https://cloud.tinybird.co/{region.provider}/{region.region}/{workspace_name}"


def get_branch_dashboard_url(api_url: str, workspace_name: str, branch_name: str) -> str | None:
    region = parse_api_url(api_url)
    if not region:
        return None
    return f"https://cloud.tinybird.co/{region.provider}/{region.region}/{workspace_name}~{branch_name}"


def get_local_dashboard_url(workspace_name: str, port: int = 7181) -> str:
    return f"https://cloud.tinybird.co/local/{port}/{workspace_name}"
