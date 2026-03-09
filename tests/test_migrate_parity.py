from __future__ import annotations

from pathlib import Path

from tinybird_sdk.migrate.discovery import discover_resource_files
from tinybird_sdk.migrate.parse import parse_resource_file
from tinybird_sdk.migrate.runner import run_migrate


def test_discover_resource_files_from_directory_fixture() -> None:
    fixture_dir = Path("tests/fixtures/migrate/input").resolve()
    result = discover_resource_files([str(fixture_dir)], str(fixture_dir))
    assert result.errors == []
    assert [item.kind for item in result.resources] == ["connection", "datasource", "pipe"]
    assert [item.name for item in result.resources] == ["broker", "events", "top_events"]


def test_parse_resource_file_kinds() -> None:
    fixture_dir = Path("tests/fixtures/migrate/input").resolve()
    discovered = discover_resource_files(["*.connection", "*.datasource", "*.pipe"], str(fixture_dir))
    assert discovered.errors == []

    parsed = [parse_resource_file(item) for item in discovered.resources]
    assert [item.kind for item in parsed] == ["connection", "datasource", "pipe"]


def test_run_migrate_dry_run_matches_golden_output() -> None:
    fixture_dir = Path("tests/fixtures/migrate/input").resolve()
    expected_path = Path("tests/fixtures/migrate/expected/migration.py").resolve()
    expected = expected_path.read_text(encoding="utf-8").rstrip() + "\n"

    result = run_migrate(
        {
            "cwd": str(fixture_dir),
            "patterns": ["*.connection", "*.datasource", "*.pipe"],
            "dry_run": True,
        }
    )

    assert result.success is True
    assert result.errors == []
    assert result.output_content == expected


def test_run_migrate_fails_when_patterns_are_missing() -> None:
    result = run_migrate({"patterns": [], "dry_run": True})
    assert result.success is False
    assert len(result.errors) == 1
    assert "At least one file" in result.errors[0].message
