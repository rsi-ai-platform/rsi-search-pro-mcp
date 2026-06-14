"""Entry point — mirrors the surface of our other MCP servers."""
from __future__ import annotations

import argparse
import logging
import os
import sys


def main() -> None:
    parser = argparse.ArgumentParser(prog="rsi-search-pro")
    parser.add_argument(
        "--transport",
        default=os.environ.get("MCP_TRANSPORT", "stdio"),
        choices=["stdio", "sse", "streamable-http"],
    )
    parser.add_argument("--host", default=os.environ.get("MCP_HOST", "0.0.0.0"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("PORT", "7863")))
    parser.add_argument("--log-level", default=os.environ.get("LOG_LEVEL", "INFO"))
    args = parser.parse_args()

    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    from .server import mcp

    if args.transport == "stdio":
        mcp.run("stdio")
    elif args.transport == "sse":
        mcp.settings.host = args.host
        mcp.settings.port = args.port
        mcp.run("sse")
    elif args.transport == "streamable-http":
        # Match the rest of the fleet: stateless HTTP, JSON response,
        # transport_security relaxed so Cloud Run's Host header passes.
        try:
            from mcp.server.transport_security import TransportSecuritySettings
            allowed = os.environ.get("ALLOWED_HOSTS")
            if allowed:
                hosts = [h.strip() for h in allowed.split(",") if h.strip()]
                mcp.settings.transport_security = TransportSecuritySettings(
                    enable_dns_rebinding_protection=True,
                    allowed_hosts=hosts,
                    allowed_origins=[f"https://{h}" for h in hosts],
                )
            else:
                mcp.settings.transport_security = TransportSecuritySettings(
                    enable_dns_rebinding_protection=False,
                )
        except Exception as e:  # noqa: BLE001
            logging.getLogger(__name__).warning(
                "Could not adjust transport_security: %s", e)

        mcp.settings.host = args.host
        mcp.settings.port = args.port
        # Stateless sessions stay on (each request creates its own MCP
        # session — no server-side state across requests). But
        # json_response is now OFF so we ship the response as SSE rather
        # than a single JSON body. SSE is what allows `research` to emit
        # notifications/progress events live to subscribed clients
        # (Layer 1 of the streaming work). Non-streaming callers parse
        # the single `data:` event identically to a plain JSON body —
        # backend MCPClient's _parse_sse_single handles both.
        mcp.settings.stateless_http = True
        mcp.settings.json_response = False
        mcp.run("streamable-http")
    else:
        print(f"unknown transport: {args.transport}", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
