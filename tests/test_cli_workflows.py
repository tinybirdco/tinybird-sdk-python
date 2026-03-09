from __future__ import annotations

from pathlib import Path

import pytest

import tinybird_sdk.cli.commands.deploy as deploy_cmd
import tinybird_sdk.cli.commands.dev as dev_cmd
import tinybird_sdk.cli.commands.pull as pull_cmd
from tinybird_sdk.cli.commands.build import run_build
from tinybird_sdk.cli.commands.deploy import run_deploy
from tinybird_sdk.cli.commands.init import run_init
from tinybird_sdk.cli.commands.migrate import run_migrate
from tinybird_sdk.cli.commands.pull import run_pull


def test_init_build_and_deploy_workflow(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    init_result = run_init({"cwd": str(tmp_path), "skip_login": True})
    assert init_result.success is True

    monkeypatch.setenv("TINYBIRD_TOKEN", "p.workspace")
    monkeypatch.setenv("TINYBIRD_URL", "https://api.tinybird.co")

    build_result = run_build({"cwd": str(tmp_path), "dry_run": True})
    assert build_result.success is True
    assert build_result.build is not None
    assert build_result.build.stats["datasource_count"] >= 1
    assert build_result.build.stats["pipe_count"] >= 1

    monkeypatch.setattr(deploy_cmd, "deploy_to_main", lambda *_args, **_kwargs: {"success": True, "result": "success"})
    deploy_result = run_deploy({"cwd": str(tmp_path), "check": True})
    assert deploy_result.success is True


def test_pull_migrate_and_dev_once_workflow(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TINYBIRD_TOKEN", "p.workspace")
    monkeypatch.setenv("TINYBIRD_URL", "https://api.tinybird.co")

    # Minimal config to enable command-level flows.
    (tmp_path / "tinybird.config.json").write_text(
        '{"include":["lib/*.py"],"token":"${TINYBIRD_TOKEN}","base_url":"${TINYBIRD_URL}"}\n',
        encoding="utf-8",
    )
    (tmp_path / "lib").mkdir()
    (tmp_path / "lib" / "datasources.py").write_text(
        'from tinybird_sdk import define_datasource, t, engine\nevents = define_datasource("events", {"schema":{"id": t.int32()}, "engine": engine.merge_tree({"sorting_key":["id"]})})\n',
        encoding="utf-8",
    )
    (tmp_path / "lib" / "pipes.py").write_text(
        'from tinybird_sdk import define_endpoint, node, t\ntop = define_endpoint("top", {"nodes":[node({"name":"n","sql":"SELECT id FROM events"})], "output":{"id": t.int32()}})\n',
        encoding="utf-8",
    )

    monkeypatch.setattr(
        pull_cmd,
        "pull_all_resource_files",
        lambda *_args, **_kwargs: {
            "datasources": [
                pull_cmd.ResourceFile(
                    name="events",
                    type="datasource",
                    filename="events.datasource",
                    content='SCHEMA >\n    id Int32\n\nENGINE "MergeTree"\nENGINE_SORTING_KEY "id"\n',
                )
            ],
            "pipes": [
                pull_cmd.ResourceFile(
                    name="top",
                    type="pipe",
                    filename="top.pipe",
                    content="NODE n\nSQL >\n    %\n    SELECT id FROM events\n",
                )
            ],
            "connections": [],
        },
    )
    pull_result = run_pull({"cwd": str(tmp_path), "output_dir": "out", "overwrite": True})
    assert pull_result.success is True
    assert pull_result.stats is not None
    assert pull_result.stats["total"] == 2

    (tmp_path / "legacy.datasource").write_text(
        'SCHEMA >\n    id Int32\n\nENGINE "MergeTree"\nENGINE_SORTING_KEY "id"\n',
        encoding="utf-8",
    )
    migrate_result = run_migrate({"cwd": str(tmp_path), "patterns": ["legacy.datasource"], "dry_run": True})
    assert migrate_result["success"] is True
    assert migrate_result["output_content"] is not None

    monkeypatch.setattr(dev_cmd, "run_build", lambda *_args, **_kwargs: type("R", (), {"success": True, "duration_ms": 1})())
    dev_result = dev_cmd.run_dev({"cwd": str(tmp_path), "once": True})
    assert dev_result["success"] is True
