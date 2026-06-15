"""Hybrid auth middleware for MCP HTTP transports.

Accepts EITHER form on incoming requests:

  1. **Google id-token** — verified via google.oauth2.id_token. The token's
     `aud` claim must equal MCP_ID_TOKEN_AUDIENCE (the MCP's own URL) and
     the `email` claim must appear in MCP_ALLOWED_SA_EMAILS (comma-
     separated). This is the path the DeepInsights backend uses — it
     mints short-lived id-tokens per request from its Cloud Run SA.

  2. **Static bearer token** — must equal MCP_BEARER_TOKEN exactly. This
     is the path human-facing Claude clients use (claude.ai Connectors,
     Claude Desktop's `mcp-remote --header`). Easier to paste, but
     shared-secret risk if leaked.

ROLLOUT MODES — set via MCP_AUTH_REQUIRED (default `false`):

  • `false` → log-only. Failed auth is logged but the request continues.
    Use during rollout: deploy code, get callers updated, then flip.
  • `true`  → enforce. Failed auth returns 401.

PATHS BYPASSED unconditionally: `/health`, `/healthz`, `/` (Cloud Run's
startup probes hit `/`).

To wire into a FastMCP server:

    from .auth import install_auth_middleware
    app = mcp.streamable_http_app()
    install_auth_middleware(app, service_url="https://...")
    uvicorn.run(app, ...)
"""
from __future__ import annotations

import logging
import os
from typing import Iterable

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.types import ASGIApp


log = logging.getLogger("mcp.auth")

_BYPASS_PATHS = {"/health", "/healthz", "/", "/livez", "/readyz"}


def _split_csv(s: str | None) -> list[str]:
    if not s:
        return []
    return [x.strip() for x in s.split(",") if x.strip()]


def _is_enforcing() -> bool:
    return os.environ.get("MCP_AUTH_REQUIRED", "false").strip().lower() in (
        "1", "true", "yes", "on",
    )


class HybridAuthMiddleware(BaseHTTPMiddleware):
    """Verifies Authorization header on every request that isn't a
    health-check. Accepts a Google id-token (verified against an audience
    + SA-email allowlist) or a static bearer token."""

    def __init__(
        self,
        app: ASGIApp,
        *,
        audience: str,
        bearer_token: str | None,
        allowed_emails: Iterable[str],
    ) -> None:
        super().__init__(app)
        self._audience = audience
        self._bearer = (bearer_token or "").strip() or None
        self._allowed_emails = {e.lower() for e in allowed_emails}
        # The Google auth library is heavy; import lazily so the module
        # remains importable in environments that don't need id-token
        # verification (e.g. unit tests).
        self._gauth = None  # type: ignore[assignment]

    def _verify_id_token(self, token: str) -> tuple[bool, str | None]:
        """Returns (ok, email_or_reason)."""
        try:
            if self._gauth is None:
                # google.auth.transport.requests needs `requests` installed.
                # We list it in every MCP's pyproject so this import
                # succeeds; the fallback message helps surface a missing
                # dep loudly instead of "id_token verify failed: …".
                try:
                    from google.auth.transport import requests as grequests
                except ImportError as e:  # noqa: BLE001
                    return False, f"id_token transport missing — add 'requests' to deps: {e}"
                from google.oauth2 import id_token as gid_token
                self._gauth = (gid_token, grequests.Request())
            gid_token, request = self._gauth
            claims = gid_token.verify_oauth2_token(
                token, request, audience=self._audience,
            )
        except Exception as e:  # noqa: BLE001
            return False, f"id_token verify failed: {e}"
        email = (claims.get("email") or "").lower()
        if not email:
            return False, "id_token missing email"
        if self._allowed_emails and email not in self._allowed_emails:
            return False, f"id_token email {email!r} not in allowlist"
        return True, email

    async def dispatch(self, request: Request, call_next):  # type: ignore[override]
        path = request.url.path
        if path in _BYPASS_PATHS:
            return await call_next(request)

        auth_header = request.headers.get("authorization", "")
        ok = False
        method = "none"
        reason: str | None = None

        if auth_header.startswith("Bearer "):
            token = auth_header[len("Bearer "):].strip()
            if self._bearer and token == self._bearer:
                ok = True
                method = "bearer"
            else:
                # Try id-token only when bearer didn't match — avoids
                # spending a Google JWKS round-trip on every static-token
                # call.
                ok, info = self._verify_id_token(token)
                if ok:
                    method = "id_token"
                else:
                    reason = info
        else:
            reason = "missing Authorization: Bearer header"

        if ok:
            log.info("auth ok method=%s path=%s", method, path)
            return await call_next(request)

        # Log + decide whether to enforce.
        enforce = _is_enforcing()
        log.warning(
            "auth FAIL enforce=%s path=%s reason=%s ua=%r",
            enforce, path, reason,
            (request.headers.get("user-agent") or "")[:80],
        )
        if enforce:
            return JSONResponse(
                {"error": "unauthorized", "reason": reason},
                status_code=401,
            )
        return await call_next(request)


def install_auth_middleware(app, *, service_url: str) -> None:
    """Mount HybridAuthMiddleware on a Starlette app using env vars:

      MCP_BEARER_TOKEN      — static bearer accepted from Claude clients.
      MCP_ALLOWED_SA_EMAILS — CSV of SA emails permitted to mint id-tokens
                              for this service. Backend's compute SA goes
                              here.
      MCP_AUTH_REQUIRED     — `true` to enforce, anything else to log only.

    Call after `mcp.streamable_http_app()` and before `uvicorn.run(app)`.
    """
    app.add_middleware(
        HybridAuthMiddleware,
        audience=service_url,
        bearer_token=os.environ.get("MCP_BEARER_TOKEN"),
        allowed_emails=_split_csv(os.environ.get("MCP_ALLOWED_SA_EMAILS")),
    )
    log.info(
        "auth middleware installed (enforce=%s, audience=%s, "
        "bearer_set=%s, allowed_emails=%d)",
        _is_enforcing(), service_url,
        bool(os.environ.get("MCP_BEARER_TOKEN")),
        len(_split_csv(os.environ.get("MCP_ALLOWED_SA_EMAILS"))),
    )
