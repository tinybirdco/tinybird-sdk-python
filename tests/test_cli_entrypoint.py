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
        lambda argv: 7 if argv == ["pull", "--force"] else 1,
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
    assert cli_index.main(["pull", "--force"]) == 7


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


def _deny_delegation(monkeypatch: pytest.MonkeyPatch, command: str) -> None:
    monkeypatch.setattr(
        cli_index,
        "_run_installed_tinybird_cli",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError(f"should not delegate {command}")
        ),
    )


def test_cli_entrypoint_runs_build_locally(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _deny_delegation(monkeypatch, "build")
    calls: list[dict] = []

    def fake_run_build(options: dict) -> SimpleNamespace:
        calls.append(options)
        return SimpleNamespace(
            success=True,
            error=None,
            duration_ms=42,
            build=None,
            deploy=None,
            branch_info=SimpleNamespace(
                git_branch="feature-x",
                tinybird_branch="feature_x",
                was_created=True,
                dashboard_url="https://app.tinybird.co/feature_x",
                is_local=False,
            ),
        )

    monkeypatch.setattr(cli_index, "run_build", fake_run_build)

    assert cli_index.main(["build", "--branch"]) == 0
    assert calls == [{"dry_run": False, "dev_mode_override": "branch"}]
    out = capsys.readouterr().out
    assert "Build completed" in out
    assert "Tinybird branch: feature_x" in out
    assert "Dashboard: https://app.tinybird.co/feature_x" in out


def test_cli_entrypoint_build_dry_run_passes_flags(monkeypatch: pytest.MonkeyPatch) -> None:
    _deny_delegation(monkeypatch, "build")
    calls: list[dict] = []

    def fake_run_build(options: dict) -> SimpleNamespace:
        calls.append(options)
        return SimpleNamespace(
            success=True, error=None, duration_ms=1, build=None, deploy=None, branch_info=None
        )

    monkeypatch.setattr(cli_index, "run_build", fake_run_build)

    assert cli_index.main(["build", "--dry-run", "--local"]) == 0
    assert calls == [{"dry_run": True, "dev_mode_override": "local"}]


def test_cli_entrypoint_build_failure_returns_error(monkeypatch: pytest.MonkeyPatch) -> None:
    _mute_output(monkeypatch)
    _deny_delegation(monkeypatch, "build")
    monkeypatch.setattr(
        cli_index,
        "run_build",
        lambda *_args, **_kwargs: SimpleNamespace(
            success=False, error="boom", duration_ms=1, build=None, deploy=None, branch_info=None
        ),
    )
    assert cli_index.main(["build"]) == 1


def test_cli_entrypoint_runs_deploy_locally(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _deny_delegation(monkeypatch, "deploy")
    calls: list[dict] = []

    def fake_run_deploy(options: dict) -> SimpleNamespace:
        calls.append(options)
        return SimpleNamespace(success=True, error=None, duration_ms=99, build=None, deploy=None)

    monkeypatch.setattr(cli_index, "run_deploy", fake_run_deploy)

    assert cli_index.main(["deploy", "--check", "--allow-destructive-operations"]) == 0
    assert calls == [{"check": True, "allow_destructive_operations": True}]
    out = capsys.readouterr().out
    assert "Deploy completed" in out


def test_cli_entrypoint_deploy_failure_returns_error(monkeypatch: pytest.MonkeyPatch) -> None:
    _mute_output(monkeypatch)
    _deny_delegation(monkeypatch, "deploy")
    monkeypatch.setattr(
        cli_index,
        "run_deploy",
        lambda *_args, **_kwargs: SimpleNamespace(
            success=False, error="deploy boom", duration_ms=1, build=None, deploy=None
        ),
    )
    assert cli_index.main(["deploy"]) == 1


def test_cli_entrypoint_runs_dev_locally(monkeypatch: pytest.MonkeyPatch) -> None:
    _deny_delegation(monkeypatch, "dev")
    calls: list[dict] = []

    def fake_run_dev(options: dict) -> dict:
        calls.append(options)
        return {"success": True}

    monkeypatch.setattr(cli_index, "run_dev", fake_run_dev)

    assert cli_index.main(["dev", "--local"]) == 0
    assert calls == [{"dev_mode_override": "local"}]


def test_cli_entrypoint_dev_failure_returns_error(monkeypatch: pytest.MonkeyPatch) -> None:
    _mute_output(monkeypatch)
    _deny_delegation(monkeypatch, "dev")
    monkeypatch.setattr(
        cli_index, "run_dev", lambda *_args, **_kwargs: {"success": False, "error": "dev boom"}
    )
    assert cli_index.main(["dev"]) == 1


def test_cli_entrypoint_build_rejects_local_and_branch_together() -> None:
    with pytest.raises(SystemExit):
        cli_index.main(["build", "--local", "--branch"])
