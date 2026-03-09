from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

import tinybird_sdk.cli.region_selector as region_selector
import tinybird_sdk.cli.utils.package_manager as package_manager
import tinybird_sdk.cli.utils.schema_validation as schema_validation
from tinybird_sdk import define_endpoint, define_project, node, t
from tinybird_sdk.api.regions import TinybirdRegion


def test_region_selector_prefers_env_match(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        region_selector,
        "fetch_regions",
        lambda: [
            TinybirdRegion(name="EU (GCP)", api_host="https://api.tinybird.co", provider="gcp"),
            TinybirdRegion(name="US East (AWS)", api_host="https://api.us-east-1.aws.tinybird.co", provider="aws"),
        ],
    )
    monkeypatch.setenv("TINYBIRD_REGION", "US East (AWS)")
    selected = region_selector.select_region()
    assert selected.success is True
    assert selected.api_host == "https://api.us-east-1.aws.tinybird.co"


def test_package_manager_detection_from_lockfile(tmp_path: Path) -> None:
    (tmp_path / "pnpm-lock.yaml").write_text("lockfile_version: 9\n", encoding="utf-8")
    assert package_manager.detect_package_manager(str(tmp_path)) == "pnpm"
    assert package_manager.detect_package_manager_run_cmd(str(tmp_path)) == "pnpm run"


def test_schema_validation_reports_missing_columns(monkeypatch: pytest.MonkeyPatch) -> None:
    endpoint = define_endpoint(
        "top",
        {
            "nodes": [node({"name": "n", "sql": "SELECT 1"})],
            "output": {"id": t.int32()},
        },
    )
    project = define_project({"datasources": {}, "pipes": {"top": endpoint}})

    class FakeTinybirdClient:
        def __init__(self, _config: dict[str, Any]):
            pass

        def query(self, _pipe_name: str, _params: dict[str, Any]) -> dict[str, Any]:
            return {"meta": [{"name": "other", "type": "String"}]}

    monkeypatch.setattr(schema_validation, "TinybirdClient", FakeTinybirdClient)
    result = schema_validation.validate_pipe_schemas(
        {
            "pipe_names": ["top"],
            "base_url": "https://api.tinybird.co",
            "token": "p.token",
            "project": project,
        }
    )
    assert result.valid is False
    assert any("Missing column 'id'" in issue.message for issue in result.issues)
