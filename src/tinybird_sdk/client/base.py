from __future__ import annotations

from dataclasses import asdict
from typing import Any

from ..api.api import TinybirdApi, TinybirdApiError
from ..api.branches import get_or_create_branch
from ..cli.config import load_config_async
from .preview import get_preview_branch_name, is_preview_environment
from .tokens import TokensNamespace
from .types import ClientContext, TinybirdError


class _DatasourcesNamespace:
    def __init__(self, client: "TinybirdClient"):
        self._client = client

    def ingest(self, datasource_name: str, event: dict[str, Any], options: dict[str, Any] | None = None) -> dict[str, Any]:
        return self._client._ingest_datasource(datasource_name, event, options or {})

    def append(self, datasource_name: str, options: dict[str, Any]) -> dict[str, Any]:
        return self._client._append_datasource(datasource_name, options)

    def replace(self, datasource_name: str, options: dict[str, Any]) -> dict[str, Any]:
        return self._client._replace_datasource(datasource_name, options)

    def delete(self, datasource_name: str, options: dict[str, Any]) -> dict[str, Any]:
        return self._client._delete_datasource(datasource_name, options)

    def truncate(self, datasource_name: str, options: dict[str, Any] | None = None) -> dict[str, Any]:
        return self._client._truncate_datasource(datasource_name, options or {})


class TinybirdClient:
    def __init__(self, config: dict[str, Any]):
        if not config.get("base_url"):
            raise ValueError("base_url is required")
        if not config.get("token"):
            raise ValueError("token is required")

        self._config = {**config, "base_url": str(config["base_url"]).rstrip("/")}
        self._apis_by_token: dict[str, TinybirdApi] = {}
        self._resolved_context: ClientContext | None = None

        self.datasources = _DatasourcesNamespace(self)
        self.tokens = TokensNamespace(
            self._get_token,
            self._config["base_url"],
            timeout=self._config.get("timeout"),
        )

    def _append_datasource(self, datasource_name: str, options: dict[str, Any]) -> dict[str, Any]:
        token = self._get_token()
        try:
            return self._get_api(token).append_datasource(datasource_name, options)
        except Exception as error:
            self._rethrow_api_error(error)

    def _replace_datasource(self, datasource_name: str, options: dict[str, Any]) -> dict[str, Any]:
        token = self._get_token()
        try:
            return self._get_api(token).append_datasource(datasource_name, options, {"mode": "replace"})
        except Exception as error:
            self._rethrow_api_error(error)

    def _delete_datasource(self, datasource_name: str, options: dict[str, Any]) -> dict[str, Any]:
        token = self._get_token()
        try:
            return self._get_api(token).delete_datasource(datasource_name, options)
        except Exception as error:
            self._rethrow_api_error(error)

    def _truncate_datasource(self, datasource_name: str, options: dict[str, Any]) -> dict[str, Any]:
        token = self._get_token()
        try:
            return self._get_api(token).truncate_datasource(datasource_name, options)
        except Exception as error:
            self._rethrow_api_error(error)

    def _ingest_datasource(self, datasource_name: str, event: dict[str, Any], options: dict[str, Any]) -> dict[str, Any]:
        token = self._get_token()
        try:
            return self._get_api(token).ingest_batch(datasource_name, [event], options)
        except Exception as error:
            self._rethrow_api_error(error)

    def _get_token(self) -> str:
        return self._resolve_context().token

    def _resolve_context(self) -> ClientContext:
        if self._resolved_context:
            return self._resolved_context

        if not self._config.get("dev_mode"):
            self._resolved_context = self._build_context(
                {
                    "token": self._config["token"],
                    "is_branch_token": False,
                }
            )
            return self._resolved_context

        self._resolved_context = self._resolve_branch_context()
        return self._resolved_context

    def _build_context(self, token_info: dict[str, Any]) -> ClientContext:
        return ClientContext(
            token=token_info["token"],
            base_url=self._config["base_url"],
            dev_mode=bool(self._config.get("dev_mode", False)),
            is_branch_token=bool(token_info.get("is_branch_token", False)),
            branch_name=token_info.get("branch_name"),
            git_branch=token_info.get("git_branch"),
        )

    def _resolve_branch_context(self) -> ClientContext:
        try:
            if is_preview_environment():
                git_branch_name = get_preview_branch_name()
                sanitized = None
                if git_branch_name:
                    import re

                    sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", git_branch_name)
                    sanitized = re.sub(r"_+", "_", sanitized).strip("_")
                tinybird_branch_name = f"tmp_ci_{sanitized}" if sanitized else None
                return self._build_context(
                    {
                        "token": self._config["token"],
                        "is_branch_token": bool(tinybird_branch_name),
                        "branch_name": tinybird_branch_name,
                        "git_branch": git_branch_name,
                    }
                )

            config = load_config_async(self._config.get("config_dir"))
            git_branch = config.get("git_branch")

            if config.get("is_main_branch") or not config.get("tinybird_branch"):
                return self._build_context(
                    {
                        "token": self._config["token"],
                        "is_branch_token": False,
                        "git_branch": git_branch,
                    }
                )

            branch_name = config["tinybird_branch"]
            branch = get_or_create_branch(
                {
                    "base_url": self._config["base_url"],
                    "token": self._config["token"],
                },
                branch_name,
            )

            if not branch.get("token"):
                return self._build_context(
                    {
                        "token": self._config["token"],
                        "is_branch_token": False,
                        "git_branch": git_branch,
                    }
                )

            return self._build_context(
                {
                    "token": branch["token"],
                    "is_branch_token": True,
                    "branch_name": branch_name,
                    "git_branch": git_branch,
                }
            )
        except Exception as error:
            raise TinybirdError(f"Failed to resolve branch context: {error}", 500) from error

    def query(self, pipe_name: str, params: dict[str, Any] | None = None, options: dict[str, Any] | None = None) -> dict[str, Any]:
        token = self._get_token()
        try:
            return self._get_api(token).query(pipe_name, params or {}, options or {})
        except Exception as error:
            self._rethrow_api_error(error)

    def ingest(self, datasource_name: str, event: dict[str, Any], options: dict[str, Any] | None = None) -> dict[str, Any]:
        return self.datasources.ingest(datasource_name, event, options or {})

    def ingest_batch(
        self,
        datasource_name: str,
        events: list[dict[str, Any]],
        options: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        token = self._get_token()
        try:
            return self._get_api(token).ingest_batch(datasource_name, events, options or {})
        except Exception as error:
            self._rethrow_api_error(error)

    def sql(self, sql: str, options: dict[str, Any] | None = None) -> dict[str, Any]:
        token = self._get_token()
        try:
            return self._get_api(token).sql(sql, options or {})
        except Exception as error:
            self._rethrow_api_error(error)

    def get_context(self) -> dict[str, Any]:
        return asdict(self._resolve_context())

    def _get_api(self, token: str) -> TinybirdApi:
        if token in self._apis_by_token:
            return self._apis_by_token[token]

        api = TinybirdApi(
            {
                "base_url": self._config["base_url"],
                "token": token,
                "timeout": self._config.get("timeout"),
            }
        )
        self._apis_by_token[token] = api
        return api

    def _rethrow_api_error(self, error: Exception) -> None:
        if isinstance(error, TinybirdApiError):
            raise TinybirdError(str(error), error.status_code, error.response) from error
        raise error


def create_client(config: dict[str, Any]) -> TinybirdClient:
    return TinybirdClient(config)
