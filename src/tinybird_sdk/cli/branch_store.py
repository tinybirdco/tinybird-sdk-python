from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass(frozen=True, slots=True)
class BranchInfo:
    id: str
    token: str
    created_at: str


def get_branch_store_path() -> str:
    return str(Path.home() / ".tinybird" / "branches.json")


def _ensure_tinybird_dir() -> None:
    tinybird_dir = Path.home() / ".tinybird"
    tinybird_dir.mkdir(parents=True, exist_ok=True)


def load_branch_store() -> dict[str, Any]:
    store_path = Path(get_branch_store_path())
    if not store_path.exists():
        return {"workspaces": {}}

    try:
        return json.loads(store_path.read_text(encoding="utf-8"))
    except Exception:
        return {"workspaces": {}}


def save_branch_store(store: dict[str, Any]) -> None:
    _ensure_tinybird_dir()
    store_path = Path(get_branch_store_path())
    store_path.write_text(json.dumps(store, indent=2), encoding="utf-8")


def get_branch_token(workspace_id: str, branch_name: str) -> BranchInfo | None:
    store = load_branch_store()
    info = (((store.get("workspaces") or {}).get(workspace_id) or {}).get("branches") or {}).get(branch_name)
    if not info:
        return None
    return BranchInfo(id=info["id"], token=info["token"], created_at=info["created_at"])


def set_branch_token(workspace_id: str, branch_name: str, info: BranchInfo | dict[str, str]) -> None:
    payload = info if isinstance(info, dict) else {"id": info.id, "token": info.token, "created_at": info.created_at}

    store = load_branch_store()
    workspaces = store.setdefault("workspaces", {})
    workspace_entry = workspaces.setdefault(workspace_id, {"branches": {}})
    branches = workspace_entry.setdefault("branches", {})
    branches[branch_name] = payload
    save_branch_store(store)


def remove_branch(workspace_id: str, branch_name: str) -> None:
    store = load_branch_store()
    workspaces = store.get("workspaces") or {}
    branches = ((workspaces.get(workspace_id) or {}).get("branches") or {})
    if branch_name in branches:
        del branches[branch_name]
        save_branch_store(store)


def list_cached_branches(workspace_id: str) -> dict[str, BranchInfo]:
    store = load_branch_store()
    branches = (((store.get("workspaces") or {}).get(workspace_id) or {}).get("branches") or {})
    result: dict[str, BranchInfo] = {}
    for name, info in branches.items():
        result[name] = BranchInfo(id=info["id"], token=info["token"], created_at=info["created_at"])
    return result


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


__all__ = [
    "BranchInfo",
    "get_branch_store_path",
    "load_branch_store",
    "save_branch_store",
    "get_branch_token",
    "set_branch_token",
    "remove_branch",
    "list_cached_branches",
    "now_iso",
]
