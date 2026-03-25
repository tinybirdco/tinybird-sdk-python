from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
from typing import Any

from ..config import get_client_path, get_config_path, get_datasources_path, get_pipes_path
from ..utils.package_manager import detect_package_manager_run_cmd
from .login import run_login


DATASOURCES_TEMPLATE = '''from tinybird_sdk import define_datasource, t, engine


page_views = define_datasource("page_views", {
    "description": "Page view tracking data",
    "schema": {
        "timestamp": t.date_time(),
        "pathname": t.string(),
        "session_id": t.string(),
        "country": t.string().low_cardinality().nullable(),
    },
    "engine": engine.merge_tree({
        "sorting_key": ["pathname", "timestamp"],
    }),
})
'''

PIPES_TEMPLATE = '''from tinybird_sdk import define_endpoint, node, t, p


top_pages = define_endpoint("top_pages", {
    "description": "Get the most visited pages",
    "params": {
        "start_date": p.date_time(),
        "end_date": p.date_time(),
        "limit": p.int32().optional(10),
    },
    "nodes": [
        node({
            "name": "aggregated",
            "sql": """
                SELECT pathname, count() AS views
                FROM page_views
                WHERE timestamp >= {{DateTime(start_date)}}
                  AND timestamp <= {{DateTime(end_date)}}
                GROUP BY pathname
                ORDER BY views DESC
                LIMIT {{Int32(limit, 10)}}
            """,
        }),
    ],
    "output": {
        "pathname": t.string(),
        "views": t.uint64(),
    },
})
'''

CLIENT_TEMPLATE = '''from tinybird_sdk import Tinybird
from .datasources import page_views
from .pipes import top_pages


tinybird = Tinybird({
    "datasources": {"page_views": page_views},
    "pipes": {"top_pages": top_pages},
})
'''


@dataclass(frozen=True, slots=True)
class InitOptions:
    cwd: str | None = None
    folder: str | None = None
    force: bool = False
    skip_login: bool = False
    dev_mode: str | None = None
    client_path: str | None = None


@dataclass(frozen=True, slots=True)
class InitResult:
    success: bool
    client_path: str | None = None
    logged_in: bool | None = None
    workspace_name: str | None = None
    user_email: str | None = None
    existing_datafiles: list[str] | None = None
    error: str | None = None


def find_existing_datafiles(cwd: str) -> list[str]:
    base = Path(cwd)
    found: list[str] = []
    for ext in ("*.datasource", "*.pipe", "*.connection"):
        found.extend(str(path.relative_to(base)) for path in base.rglob(ext))
    return sorted(found)


def _write_file(path: Path, content: str, force: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and not force:
        return
    path.write_text(content, encoding="utf-8")


def run_init(options: InitOptions | dict[str, Any] | None = None) -> InitResult:
    normalized = options if isinstance(options, InitOptions) else InitOptions(**(options or {}))
    cwd = Path(normalized.cwd or os.getcwd()).resolve()

    try:
        config_path = Path(get_config_path(str(cwd)))

        if normalized.folder:
            folder = Path(normalized.folder)
            if not folder.is_absolute():
                folder = cwd / folder
            datasources_path = folder / "datasources.py"
            pipes_path = folder / "pipes.py"
            client_path = folder / "client.py"
        else:
            datasources_path = Path(get_datasources_path(str(cwd)))
            pipes_path = Path(get_pipes_path(str(cwd)))
            client_path = Path(normalized.client_path) if normalized.client_path else Path(get_client_path(str(cwd)))
        if not client_path.is_absolute():
            client_path = cwd / client_path

        existing_datafiles = find_existing_datafiles(str(cwd))

        include = [str(datasources_path.relative_to(cwd)), str(pipes_path.relative_to(cwd))]
        include.extend(existing_datafiles)

        config_payload = {
            "include": include,
            "token": "${TINYBIRD_TOKEN}",
            "base_url": "${TINYBIRD_URL}",
            "dev_mode": normalized.dev_mode or "branch",
        }

        _write_file(config_path, json.dumps(config_payload, indent=2) + "\n", normalized.force)
        _write_file(datasources_path, DATASOURCES_TEMPLATE, normalized.force)
        _write_file(pipes_path, PIPES_TEMPLATE, normalized.force)
        _write_file(client_path, CLIENT_TEMPLATE, normalized.force)

        login_result = None
        if not normalized.skip_login:
            login_result = run_login({"cwd": str(cwd)})

        return InitResult(
            success=True,
            client_path=str(client_path.relative_to(cwd)),
            logged_in=login_result.success if login_result else False,
            workspace_name=login_result.workspace_name if login_result else None,
            user_email=login_result.user_email if login_result else None,
            existing_datafiles=existing_datafiles,
        )
    except Exception as error:
        return InitResult(success=False, error=str(error))


__all__ = ["InitOptions", "InitResult", "find_existing_datafiles", "run_init"]
