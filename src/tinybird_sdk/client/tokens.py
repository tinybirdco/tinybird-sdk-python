from __future__ import annotations

from typing import Any, Callable

from ..api.tokens import TokenApiError, create_jwt
from .types import TinybirdError


class TokensNamespace:
    def __init__(
        self,
        get_token: Callable[[], str],
        base_url: str,
        timeout: int | None = None,
    ):
        self._get_token = get_token
        self._base_url = base_url
        self._timeout = timeout

    def create_jwt(self, options: dict[str, Any]) -> dict[str, str]:
        token = self._get_token()

        try:
            return create_jwt(
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
