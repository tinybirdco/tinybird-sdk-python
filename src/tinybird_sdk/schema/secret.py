from __future__ import annotations


def secret(name: str, default_value: str | None = None) -> str:
    if not name or not name.strip():
        raise ValueError("Secret name must be a non-empty string.")

    if default_value is None:
        return f'{{{{ tb_secret("{name}") }}}}'

    return f'{{{{ tb_secret("{name}", "{default_value}") }}}}'
