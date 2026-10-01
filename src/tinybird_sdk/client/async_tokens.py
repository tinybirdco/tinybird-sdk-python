from __future__ import annotations

from typing import Any, Awaitable, Callable

from ..api.tokens import TokenApiError, create_jwt_async
from .types import TinybirdError


class AsyncTokensNamespace:
    def __init__(
        self,
        get_token: Callable[[], Awaitable[str]],
        base_url: str,
        timeout: int | None = None,
    ):
        self._get_token = get_token
        self._base_url = base_url
        self._timeout = timeout

    async def create_jwt(self, options: dict[str, Any]) -> dict[str, str]:
        token = await self._get_token()

        try:
            return await create_jwt_async(
                {
                    "base_url": self._base_url,
                    "token": token,
                    "timeout": self._timeout,
                },
                options,
            )
        except TokenApiError as error:
            raise TinybirdError(
                str(error),
                error.status,
                {
                    "error": str(error),
                    "status": error.status,
                },
            ) from error
