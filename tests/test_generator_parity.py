from __future__ import annotations

from pathlib import Path

from tinybird_sdk.generator.client import generate_client_file
from tinybird_sdk.generator.include_paths import get_include_watch_directories, resolve_include_files
from tinybird_sdk.generator.index import build_from_include
from tinybird_sdk.generator.loader import load_entities


def _write_schema_files(base: Path) -> tuple[Path, Path]:
    datasources = base / "datasources.py"
    datasources.write_text(
        """from tinybird_sdk import define_datasource, t, engine

events = define_datasource("events", {
    "schema": {"id": t.int32()},
    "engine": engine.merge_tree({"sorting_key": ["id"]}),
})
""",
        encoding="utf-8",
    )

    pipes = base / "pipes.py"
    pipes.write_text(
        """from tinybird_sdk import define_endpoint, node, t

top_events = define_endpoint("top_events", {
    "nodes": [node({"name": "n", "sql": "SELECT id FROM events"})],
    "output": {"id": t.int32()},
})
""",
        encoding="utf-8",
    )
    return datasources, pipes


def test_resolve_include_globs_and_watch_dirs(tmp_path: Path) -> None:
    (tmp_path / "a").mkdir()
    (tmp_path / "a" / "one.py").write_text("x=1\n", encoding="utf-8")
    (tmp_path / "a" / "two.py").write_text("x=2\n", encoding="utf-8")

    resolved = resolve_include_files(["a/*.py"], str(tmp_path))
    assert [item.source_path for item in resolved] == ["a/one.py", "a/two.py"]

    watch_dirs = get_include_watch_directories(["a/*.py"], str(tmp_path))
    assert watch_dirs == [str((tmp_path / "a").resolve())]


def test_load_entities_and_build_from_include_with_raw_datafiles(tmp_path: Path) -> None:
    datasources, pipes = _write_schema_files(tmp_path)
    raw_datasource = tmp_path / "legacy.datasource"
    raw_pipe = tmp_path / "legacy.pipe"
    raw_datasource.write_text("SCHEMA >\n    id Int32\n_engine \"MergeTree\"\n", encoding="utf-8")
    raw_pipe.write_text("NODE endpoint\n_sql >\n    %\n    SELECT 1 as id\n", encoding="utf-8")

    entities = load_entities(
        {
            "cwd": str(tmp_path),
            "include_paths": [
                datasources.name,
                pipes.name,
                raw_datasource.name,
                raw_pipe.name,
            ],
        }
    )
    assert set(entities.datasources.keys()) == {"events"}
    assert set(entities.pipes.keys()) == {"top_events"}
    assert [item.name for item in entities.raw_datasources] == ["legacy"]
    assert [item.name for item in entities.raw_pipes] == ["legacy"]

    build = build_from_include(
        {
            "cwd": str(tmp_path),
            "include_paths": [datasources.name, pipes.name, raw_datasource.name, raw_pipe.name],
        }
    )
    assert build.stats["datasource_count"] == 2
    assert build.stats["pipe_count"] == 2


def test_generate_client_file_uses_relative_imports_from_source_maps(tmp_path: Path) -> None:
    output_path = tmp_path / "lib" / "client.py"
    output_path.parent.mkdir(parents=True)

    generated = generate_client_file(
        {
            "datasources": {"events": object()},
            "pipes": {"top_events": object()},
            "output_path": str(output_path),
            "cwd": str(tmp_path),
            "datasource_source_files": {"events": "lib/entities/datasources.py"},
            "pipe_source_files": {"top_events": "lib/entities/pipes.py"},
        }
    )

    assert "from .entities.datasources import events" in generated.content
    assert "from .entities.pipes import top_events" in generated.content
