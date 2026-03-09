from __future__ import annotations

from dataclasses import dataclass
import json
from http.server import BaseHTTPRequestHandler, HTTPServer
import os
from threading import Event, Thread
import webbrowser
from urllib.parse import urlencode, urlparse, parse_qs

from ..api.fetcher import tinybird_fetch


AUTH_SERVER_PORT = 49160
DEFAULT_AUTH_HOST = "https://cloud.tinybird.co"
DEFAULT_API_HOST = "https://api.tinybird.co"
SERVER_MAX_WAIT_TIME = 180


def get_auth_host() -> str:
    return os.getenv("TINYBIRD_AUTH_HOST", DEFAULT_AUTH_HOST)


@dataclass(frozen=True, slots=True)
class AuthResult:
    success: bool
    token: str | None = None
    base_url: str | None = None
    workspace_name: str | None = None
    user_email: str | None = None
    error: str | None = None


def open_browser(url: str) -> bool:
    try:
        return webbrowser.open(url)
    except Exception:
        return False


def exchange_code_for_tokens(code: str, auth_host: str) -> dict[str, str]:
    url = f"{auth_host.rstrip('/')}/api/cli-login?{urlencode({'code': code})}"
    response = tinybird_fetch(url)
    if not response.ok:
        raise ValueError(f"Token exchange failed: {response.status_code} {response.text}")
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError("Invalid token exchange response")
    return payload


def browser_login(options: dict[str, str] | None = None) -> AuthResult:
    options = options or {}
    auth_host = options.get("auth_host") or get_auth_host()
    api_host = options.get("api_host") or DEFAULT_API_HOST

    # CI/non-interactive fallback.
    env_code = os.getenv("TINYBIRD_AUTH_CODE")
    if env_code:
        try:
            tokens = exchange_code_for_tokens(env_code, auth_host)
            return AuthResult(
                success=True,
                token=tokens.get("workspace_token"),
                base_url=tokens.get("api_host"),
                workspace_name=tokens.get("workspace_name"),
                user_email=tokens.get("user_email"),
            )
        except Exception as error:
            return AuthResult(success=False, error=str(error))

    code_holder: dict[str, str] = {}
    done = Event()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # type: ignore[override]
            parsed = urlparse(self.path)
            query = parse_qs(parsed.query)
            code = (query.get("code") or [None])[0]
            if code:
                code_holder["code"] = code
                done.set()
                self.send_response(200)
                self.send_header("Content-Type", "text/html")
                self.end_headers()
                self.wfile.write(b"Authentication successful. You can close this tab.")
                return

            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(b"Waiting for authentication callback...")

        def log_message(self, format, *args):  # noqa: A003
            return

    server = HTTPServer(("127.0.0.1", AUTH_SERVER_PORT), Handler)

    def serve() -> None:
        while not done.is_set():
            server.handle_request()

    thread = Thread(target=serve, daemon=True)
    thread.start()

    auth_url = f"{auth_host.rstrip('/')}/api/cli-login?{urlencode({'api_host': api_host, 'origin': 'ts-sdk'})}"
    open_browser(auth_url)

    if not done.wait(timeout=SERVER_MAX_WAIT_TIME):
        server.server_close()
        return AuthResult(success=False, error=f"Authentication timed out after {SERVER_MAX_WAIT_TIME} seconds")

    server.server_close()
    code = code_holder.get("code")
    if not code:
        return AuthResult(success=False, error="Missing authentication code")

    try:
        tokens = exchange_code_for_tokens(code, auth_host)
    except Exception as error:
        return AuthResult(success=False, error=str(error))

    return AuthResult(
        success=True,
        token=tokens.get("workspace_token"),
        base_url=tokens.get("api_host"),
        workspace_name=tokens.get("workspace_name"),
        user_email=tokens.get("user_email"),
    )


__all__ = [
    "AUTH_SERVER_PORT",
    "DEFAULT_AUTH_HOST",
    "DEFAULT_API_HOST",
    "SERVER_MAX_WAIT_TIME",
    "get_auth_host",
    "AuthResult",
    "open_browser",
    "exchange_code_for_tokens",
    "browser_login",
]
