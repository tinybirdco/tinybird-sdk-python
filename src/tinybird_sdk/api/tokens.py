from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from .api import TinybirdApiError, create_tinybird_api


@dataclass(frozen=True, slots=True)
class TokenApiConfig:
    base_url: str
    token: str
    timeout: int | None = None


class TokenApiError(Exception):
    def __init__(self, message: str, status: int, body: Any = None):
        super().__init__(message)
        self.status = status
        self.body = body


def _to_unix_timestamp(expires_at: datetime | int | str) -> int:
    if isinstance(expires_at, int):
        return expires_at
    if isinstance(expires_at, datetime):
        return int(expires_at.timestamp())
    return int(datetime.fromisoformat(expires_at.replace("Z", "+00:00")).timestamp())


def create_jwt(config: TokenApiConfig | dict[str, Any], options: dict[str, Any]) -> dict[str, str]:
    normalized = config if isinstance(config, TokenApiConfig) else TokenApiConfig(**config)
    expiration_time = _to_unix_timestamp(options["expires_at"])

    body = {
        "name": options["name"],
        "scopes": options.get("scopes", []),
    }
    if options.get("limits") is not None:
        body["limits"] = options["limits"]

    api = create_tinybird_api(
        {
            "base_url": normalized.base_url,
            "token": normalized.token,
            "timeout": normalized.timeout,
        }
    )

    try:
        result = api.create_token(body, {"expiration_time": expiration_time})
        return {"token": result["token"]}
    except TinybirdApiError as error:
        response_body = error.response_body or str(error)
        if error.status_code == 403:
            message = (
                "Permission denied creating JWT token. "
                "Make sure the token has TOKENS or ADMIN scope. "
                f"API response: {response_body}"
            )
        elif error.status_code == 400:
            message = f"Invalid JWT token request: {response_body}"
        else:
            message = f"Failed to create JWT token: {error.status_code}. API response: {response_body}"

        raise TokenApiError(message, error.status_code, response_body) from error
