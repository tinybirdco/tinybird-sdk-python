from __future__ import annotations

import json
from pathlib import Path

import tinybird_sdk as sdk


def test_root_exports_match_contract_fixture() -> None:
    fixture_path = Path("tests/fixtures/parity_contract/root_exports.json")
    expected = json.loads(fixture_path.read_text(encoding="utf-8"))

    assert sorted(sdk.__all__) == expected


def test_root_exports_do_not_include_legacy_aliases() -> None:
    banned = {"create_tinybird_client", "create_kafka_connection"}
    assert banned.isdisjoint(set(sdk.__all__))
