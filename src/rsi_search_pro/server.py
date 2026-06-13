"""FastMCP server — exposes the merged upstream tool catalog as one MCP.

We use FastMCP's underlying low-level Server (via `_mcp_server`) to override
list_tools / call_tool so the routing is fully dynamic. The high-level
`@mcp.tool()` decorator API only knows about statically declared functions —
useless when we want to mirror the upstream catalogs at runtime.
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from mcp.server.fastmcp import FastMCP
from mcp.types import TextContent, Tool

from . import tools as proxy

log = logging.getLogger("rsi_search_pro.server")


_INSTRUCTIONS = (
    "RSI Search Pro — one MCP, all the web-research firepower. This server "
    "transparently aggregates the tool catalogs of two upstream services:\n"
    "\n"
    "  • authority-web-search-mcp — Tavily-backed authoritative web search, "
    "    PDF fetch+structured extraction, AJAX-form POST, sitemap walk, "
    "    Indian-context default routing, query playbook hints.\n"
    "  • browser-research-mcp — real Chromium (patched Playwright) with "
    "    Sonnet vision. Use ONLY when the cheap rungs fail.\n"
    "\n"
    "FETCH LADDER — follow it strictly:\n"
    "  1. web_search_authoritative / pick_authority_domains  → discover sources\n"
    "  2. web_fetch_structured                              → simple HTML pages\n"
    "  3. pdf_discover → pdf_fetch / pdf_fetch_structured   → text PDFs (incl. octet-stream)\n"
    "  4. http_post_form                                     → AJAX dropdowns (PPAC-style)\n"
    "  5. visit / extract / act (browser-research)          → last resort: JS-rendered\n"
    "       SPAs, login walls, chart-only values drawn via canvas/SVG, dynamic\n"
    "       dropdowns whose data isn't in the HTML. Slower (5-15× the PDF rung)\n"
    "       but universal.\n"
    "\n"
    "Use `act(url, steps, focus)` for browser flows that need interaction — "
    "selecting a Year/Month dropdown, clicking a tab, scrolling. Use "
    "`extract(url, focus)` when the data is already on the rendered page. Use "
    "`visit(url)` for a cheap snapshot when you just want to see the page.\n"
    "\n"
    "INDIAN FISCAL YEAR: tables labelled '2025-2026' / 'FY26' span April 2025 "
    "to March 2026 — the April…December columns are the FIRST year, January-"
    "March the SECOND. Never read 'April' in an FY-labelled table as the "
    "current calendar year by default. This rule is baked into the upstream "
    "extraction prompts as well."
)


# Build the FastMCP wrapper purely for its run/transport machinery; we override
# the two MCP methods that matter (list_tools, call_tool) on the underlying
# low-level Server it exposes via `_mcp_server`.
mcp = FastMCP("rsi-search-pro", instructions=_INSTRUCTIONS)


@mcp._mcp_server.list_tools()  # type: ignore[misc]
async def _list_tools() -> list[Tool]:
    """Return the merged catalog. Re-discovered after the 5-min TTL expires;
    cache hits return in <1 ms."""
    catalog, _ = await proxy.discover_tools()
    return [
        Tool(
            name=t["name"],
            description=t.get("description", ""),
            inputSchema=t.get("inputSchema") or {"type": "object", "properties": {}},
        )
        for t in catalog
    ]


@mcp._mcp_server.call_tool()  # type: ignore[misc]
async def _call_tool(name: str, arguments: dict[str, Any] | None) -> list[TextContent]:
    """Route a tool call to whichever upstream owns the name. We unwrap the
    upstream's response back to a typed JSON object, then re-wrap as a single
    TextContent block so the calling agent sees the same shape it would from
    calling the upstream directly."""
    result = await proxy.call_upstream(name, arguments or {})
    # Match upstream's serialisation: pretty JSON in a single text block.
    return [TextContent(type="text", text=json.dumps(result, default=str))]


def get_status() -> dict[str, Any]:
    """Used by the readiness/health hooks if we add one later."""
    return proxy.snapshot_status()
