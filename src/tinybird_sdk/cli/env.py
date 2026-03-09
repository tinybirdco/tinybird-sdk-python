from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class SaveTokenResult:
    created: bool


def _upsert_env_var(path: Path, key: str, value: str) -> bool:
    line = f"{key}={value}\n"

    if path.exists():
        content = path.read_text(encoding="utf-8")
        if f"{key}=" in content:
            updated = []
            replaced = False
            for current in content.splitlines(True):
                if current.startswith(f"{key}="):
                    updated.append(line)
                    replaced = True
                else:
                    updated.append(current)
            if not replaced:
                updated.append(line)
            path.write_text("".join(updated), encoding="utf-8")
        else:
            needs_newline = len(content) > 0 and not content.endswith("\n")
            path.write_text(content + ("\n" if needs_newline else "") + line, encoding="utf-8")
        return False

    path.write_text(line, encoding="utf-8")
    return True


def save_tinybird_token(directory: str, token: str) -> SaveTokenResult:
    env_local = Path(directory) / ".env.local"
    created = _upsert_env_var(env_local, "TINYBIRD_TOKEN", token)
    return SaveTokenResult(created=created)


def save_tinybird_base_url(directory: str, base_url: str) -> None:
    env_local = Path(directory) / ".env.local"
    _upsert_env_var(env_local, "TINYBIRD_URL", base_url)


__all__ = ["SaveTokenResult", "save_tinybird_token", "save_tinybird_base_url"]
