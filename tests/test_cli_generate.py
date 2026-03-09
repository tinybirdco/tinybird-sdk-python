from __future__ import annotations

from types import SimpleNamespace

from tinybird_sdk.cli.commands.generate import runGenerate, run_generate


def test_run_generate_returns_artifacts_with_stable_relative_paths(monkeypatch) -> None:
    monkeypatch.setattr(
        "tinybird_sdk.cli.commands.generate.load_config_async",
        lambda *_args, **_kwargs: {
            "include": ["lib/tinybird.py"],
            "cwd": "/tmp/project",
            "config_path": "/tmp/project/tinybird.config.json",
        },
    )
    monkeypatch.setattr(
        "tinybird_sdk.cli.commands.generate.build_from_include",
        lambda *_args, **_kwargs: SimpleNamespace(
            resources=SimpleNamespace(
                datasources=[SimpleNamespace(name="events", content="SCHEMA >")],
                pipes=[SimpleNamespace(name="events_endpoint", content="TYPE endpoint")],
                connections=[SimpleNamespace(name="kafka_main", content="TYPE kafka")],
            ),
            stats={"datasource_count": 1, "pipe_count": 1, "connection_count": 1},
        ),
    )

    result = run_generate()

    assert result.success is True
    assert result.artifacts is not None
    assert [artifact.relative_path for artifact in result.artifacts] == [
        "datasources/events.datasource",
        "pipes/events_endpoint.pipe",
        "connections/kafka_main.connection",
    ]
    assert result.stats is not None
    assert result.stats["total_count"] == 3


def test_run_generate_camel_case_alias_points_to_same_function() -> None:
    assert runGenerate is run_generate


def test_run_generate_returns_error_when_loading_config_fails(monkeypatch) -> None:
    monkeypatch.setattr(
        "tinybird_sdk.cli.commands.generate.load_config_async",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(ValueError("No tinybird config")),
    )

    result = run_generate()

    assert result.success is False
    assert result.error is not None
    assert "No tinybird config" in result.error


def test_run_generate_writes_artifacts_to_output_dir(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(
        "tinybird_sdk.cli.commands.generate.load_config_async",
        lambda *_args, **_kwargs: {
            "include": ["lib/tinybird.py"],
            "cwd": str(tmp_path),
            "config_path": str(tmp_path / "tinybird.config.json"),
        },
    )
    monkeypatch.setattr(
        "tinybird_sdk.cli.commands.generate.build_from_include",
        lambda *_args, **_kwargs: SimpleNamespace(
            resources=SimpleNamespace(
                datasources=[SimpleNamespace(name="events", content="SCHEMA >")],
                pipes=[SimpleNamespace(name="events_endpoint", content="TYPE endpoint")],
                connections=[SimpleNamespace(name="kafka_main", content="TYPE kafka")],
            ),
            stats={"datasource_count": 1, "pipe_count": 1, "connection_count": 1},
        ),
    )

    output_dir = tmp_path / "generated"
    result = run_generate({"cwd": str(tmp_path), "output_dir": str(output_dir)})

    assert result.success is True
    assert (output_dir / "datasources" / "events.datasource").read_text(encoding="utf-8") == "SCHEMA >"
    assert (output_dir / "pipes" / "events_endpoint.pipe").read_text(encoding="utf-8") == "TYPE endpoint"
    assert (output_dir / "connections" / "kafka_main.connection").read_text(encoding="utf-8") == "TYPE kafka"


def test_run_generate_returns_error_when_build_fails(monkeypatch) -> None:
    monkeypatch.setattr(
        "tinybird_sdk.cli.commands.generate.load_config_async",
        lambda *_args, **_kwargs: {
            "include": ["lib/tinybird.py"],
            "cwd": "/tmp/project",
            "config_path": "/tmp/project/tinybird.config.json",
        },
    )
    monkeypatch.setattr(
        "tinybird_sdk.cli.commands.generate.build_from_include",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("generator failed")),
    )

    result = run_generate()

    assert result.success is False
    assert result.error is not None
    assert "generator failed" in result.error
