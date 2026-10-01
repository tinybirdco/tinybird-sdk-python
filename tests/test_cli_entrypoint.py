from __future__ import annotations

import builtins
from dataclasses import dataclass
import sys
import types
from types import SimpleNamespace

import pytest

import tinybird_sdk.cli.index as cli_index


def _mute_output(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_index.output, "error", lambda *args, **kwargs: None)


def _deny_delegation(monkeypatch: pytest.MonkeyPatch, command: str) -> None:
    monkeypatch.setattr(
        cli_index,
        "_run_installed_tinybird_cli",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError(f"should not delegate {command}")
        ),
    )


def _install_fake_tinybird_cli(monkeypatch: pytest.MonkeyPatch, main_impl) -> None:
    tinybird_pkg = types.ModuleType("tinybird")
    tinybird_pkg.__path__ = []
    tb_pkg = types.ModuleType("tinybird.tb")
    tb_pkg.__path__ = []
    cli_mod = types.ModuleType("tinybird.tb.cli")

    @dataclass
    class _FakeCLI:
        def main(self, *, args: list[str], prog_name: str) -> None:
            main_impl(args, prog_name)

    cli_mod.cli = _FakeCLI()
    monkeypatch.setitem(sys.modules, "tinybird", tinybird_pkg)
    monkeypatch.setitem(sys.modules, "tinybird.tb", tb_pkg)
    monkeypatch.setitem(sys.modules, "tinybird.tb.cli", cli_mod)


def test_run_installed_tinybird_cli_maps_system_exit_code(monkeypatch: pytest.MonkeyPatch) -> None:
    _mute_output(monkeypatch)

    def fake_main(_args: list[str], _prog_name: str) -> None:
        raise SystemExit(5)

    _install_fake_tinybird_cli(monkeypatch, fake_main)
    assert cli_index._run_installed_tinybird_cli(["build"]) == 5


def test_run_installed_tinybird_cli_errors_when_dependency_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _mute_output(monkeypatch)
    real_import = builtins.__import__

    def fake_import(name: str, *args, **kwargs):
        if name == "tinybird.tb.cli":
            raise ModuleNotFoundError("No module named 'tinybird.tb.cli'")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    assert cli_index._run_installed_tinybird_cli(["build"]) == 1


def test_cli_entrypoint_delegates_non_sdk_commands(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        cli_index,
        "_run_installed_tinybird_cli",
        lambda argv: 7 if argv == ["build", "--dry-run"] else 1,
    )
    monkeypatch.setattr(
        cli_index,
        "run_generate",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("generate should not run")),
    )
    monkeypatch.setattr(
        cli_index,
        "run_migrate",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("migrate should not run")),
    )
    assert cli_index.main(["build", "--dry-run"]) == 7


def test_cli_entrypoint_delegates_empty_argv(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[list[str]] = []
    monkeypatch.setattr(
        cli_index, "_run_installed_tinybird_cli", lambda argv: calls.append(list(argv)) or 0
    )
    assert cli_index.main([]) == 0
    assert calls == [[]]


def test_cli_entrypoint_runs_generate_locally(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(
        cli_index,
        "_run_installed_tinybird_cli",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("should not delegate generate")
        ),
    )
    monkeypatch.setattr(
        cli_index,
        "run_generate",
        lambda *_args, **_kwargs: SimpleNamespace(
            success=True,
            error=None,
            duration_ms=1,
            stats={
                "datasource_count": 1,
                "pipe_count": 1,
                "connection_count": 0,
                "total_count": 2,
            },
            output_dir=None,
        ),
    )

    assert cli_index.main(["generate"]) == 0
    out = capsys.readouterr().out
    assert "Generated 2 resources (1 datasources, 1 pipes, 0 connections)" in out


def test_cli_entrypoint_runs_migrate_locally(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(
        cli_index,
        "_run_installed_tinybird_cli",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("should not delegate migrate")
        ),
    )
    monkeypatch.setattr(
        cli_index,
        "run_migrate",
        lambda *_args, **_kwargs: {
            "success": True,
            "output_path": "/tmp/tinybird.migration.py",
            "migrated": ["a", "b"],
            "errors": [],
            "dry_run": False,
            "output_content": None,
        },
    )

    assert cli_index.main(["migrate", "legacy.datasource"]) == 0
    out = capsys.readouterr().out
    assert "Migrated 2 resources" in out
    assert "Written to: /tmp/tinybird.migration.py" in out


def test_cli_entrypoint_runs_generate_json_output(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(
        cli_index,
        "_run_installed_tinybird_cli",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("should not delegate generate")
        ),
    )
    monkeypatch.setattr(
        cli_index,
        "run_generate",
        lambda *_args, **_kwargs: SimpleNamespace(
            success=True,
            error=None,
            duration_ms=1,
            artifacts=[],
            stats={"datasource_count": 0, "pipe_count": 0, "connection_count": 0, "total_count": 0},
            output_dir=None,
            config_path="/tmp/tinybird.config.json",
        ),
    )
    monkeypatch.setattr(cli_index, "asdict", lambda value: value.__dict__)

    assert cli_index.main(["generate", "--json"]) == 0
    out = capsys.readouterr().out
    assert '"success": true' in out
    assert '"config_path": "/tmp/tinybird.config.json"' in out


def test_cli_entrypoint_generate_failure_returns_error(monkeypatch: pytest.MonkeyPatch) -> None:
    _mute_output(monkeypatch)
    monkeypatch.setattr(
        cli_index,
        "_run_installed_tinybird_cli",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("should not delegate generate")
        ),
    )
    monkeypatch.setattr(
        cli_index,
        "run_generate",
        lambda *_args, **_kwargs: SimpleNamespace(success=False, error="boom", duration_ms=1),
    )
    assert cli_index.main(["generate"]) == 1


def test_cli_entrypoint_migrate_failure_returns_error(monkeypatch: pytest.MonkeyPatch) -> None:
    _mute_output(monkeypatch)
    monkeypatch.setattr(
        cli_index,
        "_run_installed_tinybird_cli",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("should not delegate migrate")
        ),
    )
    monkeypatch.setattr(
        cli_index,
        "run_migrate",
        lambda *_args, **_kwargs: {"success": False, "errors": ["boom"]},
    )
    assert cli_index.main(["migrate", "legacy.datasource"]) == 1


def test_cli_entrypoint_branch_list(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _deny_delegation(monkeypatch, "branch")
    monkeypatch.setattr(
        cli_index,
        "run_branch_list",
        lambda *_args, **_kwargs: SimpleNamespace(
            success=True, branches=[{"name": "main"}, {"name": "feature_x"}], error=None
        ),
    )
    assert cli_index.main(["branch", "list"]) == 0
    out = capsys.readouterr().out
    assert "main" in out
    assert "feature_x" in out


def test_cli_entrypoint_branch_list_failure_returns_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _mute_output(monkeypatch)
    _deny_delegation(monkeypatch, "branch")
    monkeypatch.setattr(
        cli_index,
        "run_branch_list",
        lambda *_args, **_kwargs: SimpleNamespace(success=False, branches=[], error="boom"),
    )
    assert cli_index.main(["branch", "list"]) == 1


def test_cli_entrypoint_branch_status(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _deny_delegation(monkeypatch, "branch")
    calls: list[str | None] = []

    def fake_run_branch_status(name: str | None = None, *_args, **_kwargs) -> SimpleNamespace:
        calls.append(name)
        return SimpleNamespace(success=True, branch={"name": name, "id": "br_1"}, error=None)

    monkeypatch.setattr(cli_index, "run_branch_status", fake_run_branch_status)

    assert cli_index.main(["branch", "status", "feature_x"]) == 0
    assert calls == ["feature_x"]
    out = capsys.readouterr().out
    assert '"name": "feature_x"' in out


def test_cli_entrypoint_branch_status_defaults_to_no_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _deny_delegation(monkeypatch, "branch")
    calls: list[str | None] = []

    def fake_run_branch_status(name: str | None = None, *_args, **_kwargs) -> SimpleNamespace:
        calls.append(name)
        return SimpleNamespace(success=True, branch={"name": "main"}, error=None)

    monkeypatch.setattr(cli_index, "run_branch_status", fake_run_branch_status)

    assert cli_index.main(["branch", "status"]) == 0
    assert calls == [None]


def test_cli_entrypoint_branch_delete(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _deny_delegation(monkeypatch, "branch")
    calls: list[str] = []

    def fake_run_branch_delete(name: str, *_args, **_kwargs) -> SimpleNamespace:
        calls.append(name)
        return SimpleNamespace(success=True, deleted=True, error=None)

    monkeypatch.setattr(cli_index, "run_branch_delete", fake_run_branch_delete)

    assert cli_index.main(["branch", "delete", "feature_x"]) == 0
    assert calls == ["feature_x"]
    out = capsys.readouterr().out
    assert "feature_x" in out


def test_cli_entrypoint_branch_delete_failure_returns_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _mute_output(monkeypatch)
    _deny_delegation(monkeypatch, "branch")
    monkeypatch.setattr(
        cli_index,
        "run_branch_delete",
        lambda *_args, **_kwargs: SimpleNamespace(success=False, deleted=False, error="boom"),
    )
    assert cli_index.main(["branch", "delete", "feature_x"]) == 1


def test_cli_entrypoint_info(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from tinybird_sdk.cli.commands.info import InfoCommandResult

    _deny_delegation(monkeypatch, "info")
    monkeypatch.setattr(
        cli_index,
        "run_info",
        lambda *_args, **_kwargs: InfoCommandResult(
            success=True,
            cloud={"base_url": "https://api.tinybird.co"},
            local={"running": False},
            branch={"git_branch": "main"},
            project={"resources": {"datasources": 1, "pipes": 2}},
            branches=[],
        ),
    )
    assert cli_index.main(["info"]) == 0
    out = capsys.readouterr().out
    assert "api.tinybird.co" in out


def test_cli_entrypoint_info_json(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from tinybird_sdk.cli.commands.info import InfoCommandResult

    _deny_delegation(monkeypatch, "info")
    monkeypatch.setattr(
        cli_index,
        "run_info",
        lambda *_args, **_kwargs: InfoCommandResult(
            success=True, cloud=None, local=None, branch=None, project=None, branches=None
        ),
    )
    assert cli_index.main(["info", "--json"]) == 0
    out = capsys.readouterr().out
    assert '"success": true' in out


def test_cli_entrypoint_info_failure_returns_error(monkeypatch: pytest.MonkeyPatch) -> None:
    from tinybird_sdk.cli.commands.info import InfoCommandResult

    _mute_output(monkeypatch)
    _deny_delegation(monkeypatch, "info")
    monkeypatch.setattr(
        cli_index,
        "run_info",
        lambda *_args, **_kwargs: InfoCommandResult(
            success=False,
            cloud=None,
            local=None,
            branch=None,
            project=None,
            branches=None,
            error="boom",
        ),
    )
    assert cli_index.main(["info"]) == 1


def test_cli_entrypoint_preview(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _deny_delegation(monkeypatch, "preview")
    calls: list[dict] = []

    def fake_run_preview(options: dict) -> SimpleNamespace:
        calls.append(options)
        return SimpleNamespace(
            success=True,
            duration_ms=10,
            error=None,
            branch={"name": "tmp_ci_feature_x", "url": "https://api.tinybird.co"},
            build=None,
            deploy=None,
        )

    monkeypatch.setattr(cli_index, "run_preview", fake_run_preview)

    assert cli_index.main(["preview", "--check", "--name", "custom", "--local"]) == 0
    assert calls == [
        {"dry_run": False, "check": True, "name": "custom", "dev_mode_override": "local"}
    ]
    out = capsys.readouterr().out
    assert "Preview completed" in out
    assert "tmp_ci_feature_x" in out


def test_cli_entrypoint_preview_failure_returns_error(monkeypatch: pytest.MonkeyPatch) -> None:
    _mute_output(monkeypatch)
    _deny_delegation(monkeypatch, "preview")
    monkeypatch.setattr(
        cli_index,
        "run_preview",
        lambda *_args, **_kwargs: SimpleNamespace(
            success=False, duration_ms=1, error="boom", branch=None, build=None, deploy=None
        ),
    )
    assert cli_index.main(["preview"]) == 1


def test_cli_entrypoint_preview_rejects_local_and_branch_together() -> None:
    with pytest.raises(SystemExit):
        cli_index.main(["preview", "--local", "--branch"])


def test_cli_entrypoint_branch_requires_subcommand() -> None:
    with pytest.raises(SystemExit):
        cli_index.main(["branch"])
