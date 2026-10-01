from __future__ import annotations

from typing import Any, Callable, cast
from typing import List as _List

from ..api.api import TinybirdApi, TinybirdApiError
from ..api.tokens import TokenApiError, create_jwt
from .types import TinybirdError, TinybirdErrorResponse


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

    def list(self, options: dict[str, Any] | None = None) -> _List[dict[str, Any]]:
        """List tokens in the workspace, matching ``tb token ls``."""
        result = self._request(lambda api, opts: api.list_tokens(opts), options)
        return list(result.get("tokens", []))

    def get(self, name: str, options: dict[str, Any] | None = None) -> dict[str, Any]:
        """Get a single token's details, including its scopes and current value."""
        return self._request(lambda api, opts: api.get_token(name, opts), options)

    def scopes(self, name: str, options: dict[str, Any] | None = None) -> _List[dict[str, Any]]:
        """List a token's scopes, matching ``tb token scopes``."""
        return list(self.get(name, options).get("scopes", []))

    def refresh(self, name: str, options: dict[str, Any] | None = None) -> dict[str, Any]:
        """Rotate a token's value, matching ``tb token refresh``."""
        return self._request(lambda api, opts: api.refresh_token(name, opts), options)

    def revoke(self, name: str, options: dict[str, Any] | None = None) -> dict[str, Any]:
        """Revoke (delete) a token, matching ``tb token rm``."""
        return self._request(lambda api, opts: api.revoke_token(name, opts), options)

    def copy(self, name: str, options: dict[str, Any] | None = None) -> str:
        """Return a token's current value.

        ``tb token copy`` copies the token value to the system clipboard, which has
        no equivalent in a library context; this returns the same underlying value
        (via ``get``) for the caller to use or store as needed.
        """
        return str(self.get(name, options).get("token", ""))

    def _request(
        self,
        call: "Callable[[TinybirdApi, dict[str, Any]], dict[str, Any]]",
        options: dict[str, Any] | None,
    ) -> dict[str, Any]:
        opts = dict(options or {})
        opts.setdefault("timeout", self._timeout)
        api = TinybirdApi(
            {
                "base_url": self._base_url,
                "token": self._get_token(),
                "timeout": self._timeout,
            }
        )
        try:
            return call(api, opts)
        except TinybirdApiError as error:
            response = cast(TinybirdErrorResponse | None, error.response)
            raise TinybirdError(str(error), error.status_code, response) from error
