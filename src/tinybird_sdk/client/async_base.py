from __future__ import annotations

import asyncio
from dataclasses import asdict
from typing import Any, cast

from ..api.async_api import AsyncTinybirdApi
from ..api.api import TinybirdApiError
from ..api.branches import CreateBranchOptions, get_or_create_branch
from ..cli.config import load_config_async
from .async_tokens import AsyncTokensNamespace
from .preview import get_preview_branch_name, is_preview_environment
from .types import ClientContext, TinybirdError, TinybirdErrorResponse


class _AsyncDatasourcesNamespace:
    def __init__(self, client: "AsyncTinybirdClient"):
        self._client = client

    async def ingest(
        self, datasource_name: str, event: dict[str, Any], options: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        return await self._client._ingest_datasource(datasource_name, event, options or {})

    async def append(self, datasource_name: str, options: dict[str, Any]) -> dict[str, Any]:
        return await self._client._append_datasource(datasource_name, options)

    async def replace(self, datasource_name: str, options: dict[str, Any]) -> dict[str, Any]:
        return await self._client._replace_datasource(datasource_name, options)

    async def delete(self, datasource_name: str, options: dict[str, Any]) -> dict[str, Any]:
        return await self._client._delete_datasource(datasource_name, options)

    async def truncate(
        self, datasource_name: str, options: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        return await self._client._truncate_datasource(datasource_name, options or {})


class AsyncTinybirdClient:
    """Async counterpart to `TinybirdClient`. Same config shape, same method
    surface and semantics (`query`, `ingest`, `ingest_batch`, `sql`, the
    `datasources`/`tokens` namespaces), backed by `AsyncTinybirdApi`.

    Branch-token resolution (`dev_mode=True`) still goes through the existing
    synchronous branch-management API (`get_or_create_branch`), since creating
    or polling for a Tinybird branch is an infrequent, inherently slow
    (up to ~2 minutes) one-time setup operation, not a per-request hot path -
    rewriting that whole module as async would be a large expansion of scope
    for no real benefit. It's offloaded via `asyncio.to_thread` so it still
    doesn't block the event loop while it runs.
    """

    def __init__(self, config: dict[str, Any]):
        if not config.get("base_url"):
            raise ValueError("base_url is required")
        if not config.get("token"):
            raise ValueError("token is required")

        self._config = {**config, "base_url": str(config["base_url"]).rstrip("/")}
        self._apis_by_token: dict[str, AsyncTinybirdApi] = {}
        self._resolved_context: ClientContext | None = None
        self._context_lock = asyncio.Lock()

        self.datasources = _AsyncDatasourcesNamespace(self)
        self.tokens = AsyncTokensNamespace(
            self._get_token,
            self._config["base_url"],
            timeout=self._config.get("timeout"),
        )

    async def aclose(self) -> None:
        for api in self._apis_by_token.values():
            await api.aclose()
        self._apis_by_token.clear()

    async def __aenter__(self) -> "AsyncTinybirdClient":
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.aclose()

    async def _append_datasource(
        self, datasource_name: str, options: dict[str, Any]
    ) -> dict[str, Any]:
        token = await self._get_token()
        try:
            return await (await self._get_api(token)).append_datasource(datasource_name, options)
        except Exception as error:
            self._rethrow_api_error(error)
            raise AssertionError("unreachable")

    async def _replace_datasource(
        self, datasource_name: str, options: dict[str, Any]
    ) -> dict[str, Any]:
        token = await self._get_token()
        try:
            api = await self._get_api(token)
            return await api.append_datasource(datasource_name, options, {"mode": "replace"})
        except Exception as error:
            self._rethrow_api_error(error)
            raise AssertionError("unreachable")

    async def _delete_datasource(
        self, datasource_name: str, options: dict[str, Any]
    ) -> dict[str, Any]:
        token = await self._get_token()
        try:
            return await (await self._get_api(token)).delete_datasource(datasource_name, options)
        except Exception as error:
            self._rethrow_api_error(error)
            raise AssertionError("unreachable")

    async def _truncate_datasource(
        self, datasource_name: str, options: dict[str, Any]
    ) -> dict[str, Any]:
        token = await self._get_token()
        try:
            return await (await self._get_api(token)).truncate_datasource(datasource_name, options)
        except Exception as error:
            self._rethrow_api_error(error)
            raise AssertionError("unreachable")

    async def _ingest_datasource(
        self, datasource_name: str, event: dict[str, Any], options: dict[str, Any]
    ) -> dict[str, Any]:
        token = await self._get_token()
        try:
            api = await self._get_api(token)
            return await api.ingest_batch(datasource_name, [event], options)
        except Exception as error:
            self._rethrow_api_error(error)
            raise AssertionError("unreachable")

    async def _get_token(self) -> str:
        return (await self._resolve_context()).token

    async def _resolve_context(self) -> ClientContext:
        if self._resolved_context:
            return self._resolved_context

        async with self._context_lock:
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

            self._resolved_context = await self._resolve_branch_context()
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

    async def _resolve_branch_context(self) -> ClientContext:
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
            branch_options = None
            branch_value = config.get("branch_data_mode")
            if branch_value and config.get("dev_mode") != "local":
                branch_options = CreateBranchOptions(branch_data_mode=branch_value)

            # get_or_create_branch is sync (it polls a job to completion, up to
            # ~2 minutes) - run it off the event loop thread rather than blocking it.
            branch = await asyncio.to_thread(
                get_or_create_branch,
                {
                    "base_url": self._config["base_url"],
                    "token": self._config["token"],
                },
                branch_name,
                options=branch_options,
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

    async def query(
        self,
        pipe_name: str,
        params: dict[str, Any] | None = None,
        options: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        token = await self._get_token()
        try:
            api = await self._get_api(token)
            return await api.query(pipe_name, params or {}, options or {})
        except Exception as error:
            self._rethrow_api_error(error)
            raise AssertionError("unreachable")

    async def ingest(
        self,
        datasource_name: str,
        event: dict[str, Any],
        options: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return await self.datasources.ingest(datasource_name, event, options or {})

    async def ingest_batch(
        self,
        datasource_name: str,
        events: list[dict[str, Any]],
        options: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        token = await self._get_token()
        try:
            api = await self._get_api(token)
            return await api.ingest_batch(datasource_name, events, options or {})
        except Exception as error:
            self._rethrow_api_error(error)
            raise AssertionError("unreachable")

    async def sql(self, sql: str, options: dict[str, Any] | None = None) -> dict[str, Any]:
        token = await self._get_token()
        try:
            api = await self._get_api(token)
            return await api.sql(sql, options or {})
        except Exception as error:
            self._rethrow_api_error(error)
            raise AssertionError("unreachable")

    async def get_context(self) -> dict[str, Any]:
        return asdict(await self._resolve_context())

    async def _get_api(self, token: str) -> AsyncTinybirdApi:
        if token in self._apis_by_token:
            return self._apis_by_token[token]

        api = AsyncTinybirdApi(
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
            response = cast(TinybirdErrorResponse | None, error.response)
            raise TinybirdError(str(error), error.status_code, response) from error
        raise error


def create_async_client(config: dict[str, Any]) -> AsyncTinybirdClient:
    return AsyncTinybirdClient(config)
