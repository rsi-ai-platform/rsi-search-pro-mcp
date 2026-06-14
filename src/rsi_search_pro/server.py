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
    "aggregates the tool catalogs of SIX upstream MCPs into one place and "
    "exposes a high-level agentic tool that orchestrates them for you:\n"
    "\n"
    "  • authority-web-search-mcp — Tavily authoritative search, PDF / "
    "    Excel / CSV fetch + structured extraction, AJAX-form POST, sitemap.\n"
    "  • browser-research-mcp — real Chromium (patched Playwright) +\n"
    "    Sonnet vision. Last-resort for JS-rendered pages.\n"
    "  • rbi-dbie — Reserve Bank of India Database on Indian Economy.\n"
    "  • mospi-esankhyiki — MoSPI macro (CPI/IIP/GDP/ASI).\n"
    "  • data-gov-in — 100k+ Indian govt open datasets.\n"
    "  • cga — Controller General of Accounts (union/state public finance).\n"
    "\n"
    "TWO WAYS TO USE THIS MCP:\n"
    "\n"
    "  1. HIGH-LEVEL  →  `research(query)`  ← RECOMMENDED for most queries.\n"
    "     Runs its own planner → executor → observer → synthesizer loop\n"
    "     inside RSI Search Pro using Sonnet + Haiku, calling the right\n"
    "     low-level tools on the right upstreams, observing each result,\n"
    "     re-routing on empty/wrong/error, synthesizing a cited final\n"
    "     answer. Returns a rich trace so the caller can render the\n"
    "     reasoning + tool timeline. Always keeps the user's objective\n"
    "     in focus; never gives up silently — on failure it returns an\n"
    "     honest 'I could not answer' plus a data_gap dict the platform\n"
    "     can queue for follow-up ingest.\n"
    "\n"
    "  2. LOW-LEVEL   →  call any of the 50+ aggregated tools directly.\n"
    "     The catalog is the union of every upstream's tools/list. Pick\n"
    "     these when you already know exactly which tool you want and\n"
    "     don't need the orchestrator's overhead (~$0.05 + ~30s).\n"
    "\n"
    "FETCH LADDER (the orchestrator follows this; you should too if going\n"
    "low-level):\n"
    "  1. pick_authority_domains            → routing via the query playbook\n"
    "  2. web_fetch_structured              → simple HTML pages\n"
    "  3. pdf_fetch_structured              → text PDFs\n"
    "  4. excel_fetch_structured            → spreadsheets (GST, CGA, MoSPI)\n"
    "  5. http_post_form                    → AJAX-dropdown gov dashboards\n"
    "  6. Source-specific tools:\n"
    "       RBI DBIE get_data               → repo rate, FX, money supply\n"
    "       eSankhyiki get_data             → CPI, IIP, GDP\n"
    "       data.gov.in search_datasets     → long-tail Indian datasets\n"
    "       CGA get_monthly_account         → union accounts, NSDP\n"
    "  7. browser-research visit/act/extract → last resort (5-45s, Chromium)\n"
    "\n"
    "INDIAN FISCAL YEAR: tables labelled '2025-2026' / 'FY26' span April 2025 "
    "to March 2026 — April…December are the FIRST year, Jan–March the SECOND. "
    "Never read 'April' in an FY-labelled table as the current calendar year "
    "by default."
)


# Build the FastMCP wrapper purely for its run/transport machinery; we override
# the two MCP methods that matter (list_tools, call_tool) on the underlying
# low-level Server it exposes via `_mcp_server`.
mcp = FastMCP("rsi-search-pro", instructions=_INSTRUCTIONS)


# Static descriptor for the agentic high-level tool — first entry in the
# catalog so calling agents see it before the 50+ proxied tools.
_RESEARCH_TOOL = Tool(
    name="research",
    description=(
        "Agentic research over all 6 upstream MCPs. Give it a question; it "
        "plans, executes, observes each result, re-routes on empty/wrong, "
        "and returns a cited answer plus a step-by-step trace. Use this "
        "when you want 'just answer the question'. For fine-grained tool "
        "control, call the lower-level tools directly. Latency ~5-45s; "
        "internal model spend ~$0.02-$0.15 per call."
    ),
    inputSchema={
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": (
                    "The user's question. Be specific. Examples: 'GST monthly "
                    "collections for the last 4 months', 'India CPI YoY May "
                    "2026', 'RBI repo rate as of today', 'Domestic LPG "
                    "consumption FY2024-25 month by month'."
                ),
            },
            "max_steps": {"type": "integer", "default": 12,
                            "description": "Hard cap on tool calls (1-24)."},
            "max_seconds": {"type": "number", "default": 90,
                              "description": "Wall-time cap in seconds."},
            "answer_style": {
                "type": "string", "enum": ["concise", "detailed"],
                "default": "concise",
                "description": "How long the synthesised answer should be.",
            },
        },
        "required": ["query"],
    },
)


@mcp._mcp_server.list_tools()  # type: ignore[misc]
async def _list_tools() -> list[Tool]:
    """Return `research` + the merged proxy catalog. Catalog hits the
    in-process TTL cache after the first request."""
    catalog, _ = await proxy.discover_tools()
    proxied = [
        Tool(
            name=t["name"],
            description=t.get("description", ""),
            inputSchema=t.get("inputSchema") or {"type": "object", "properties": {}},
        )
        for t in catalog
    ]
    return [_RESEARCH_TOOL, *proxied]


def _build_progress_emitter():
    """Return an async `emit(kind, data)` that pushes MCP progress
    notifications to the calling client, or None when the client didn't
    supply a progressToken.

    Per MCP spec, the client opts in by attaching `_meta.progressToken`
    to its tools/call params. The server then sends one or more
    `notifications/progress` events tagged with that token until the
    tool returns its final result. The high-level FastMCP Context class
    wraps this; we replicate the wiring here because our `research`
    tool is registered through the low-level call_tool handler (which
    doesn't auto-inject Context).
    """
    try:
        from mcp.server.lowlevel.server import request_ctx  # type: ignore
        from mcp import types  # type: ignore
    except Exception as e:  # noqa: BLE001
        log.debug("progress notifications unavailable: %s", e)
        return None
    try:
        rc = request_ctx.get()
    except LookupError:
        return None
    if rc is None:
        return None

    # The lowlevel MCP SDK parks the request metadata directly on
    # RequestContext.meta (NOT inside rc.request.params). Field shape:
    # mcp.types.RequestParams.Meta with .progressToken.
    meta = getattr(rc, "meta", None)
    token = getattr(meta, "progressToken", None) if meta is not None else None
    # Verbose one-time debug — what's actually in the context. Remove
    # once the streaming path is stable.
    try:
        rc_keys = sorted(k for k in dir(rc) if not k.startswith("_"))[:12]
        req_obj = getattr(rc, "request", None)
        req_attrs = sorted(k for k in dir(req_obj) if not k.startswith("_"))[:12] if req_obj else None
        log.info("progress emitter probe: meta=%r token=%r rc_keys=%r req=%r req_attrs=%r",
                  meta, token, rc_keys, type(req_obj).__name__ if req_obj else None, req_attrs)
    except Exception as e:  # noqa: BLE001
        log.warning("probe failed: %s", e)
    if token is None:
        # Client didn't ask for progress — short-circuit to avoid pointless
        # JSON serialisation on the hot path inside research().
        return None
    session = rc.session
    counter = {"n": 0}

    async def emit(kind: str, data: dict[str, Any]) -> None:
        counter["n"] += 1
        try:
            payload = json.dumps({"kind": kind, "data": data}, default=str)
        except Exception:  # noqa: BLE001
            payload = json.dumps({"kind": kind, "data": {"error": "unserializable"}})
        try:
            await session.send_notification(
                types.ServerNotification(
                    types.ProgressNotification(
                        method="notifications/progress",
                        params=types.ProgressNotificationParams(
                            progressToken=token,
                            progress=float(counter["n"]),
                            total=None,
                            message=payload,
                        ),
                    )
                )
            )
        except Exception as e:  # noqa: BLE001
            log.warning("send_notification failed: %s", e)

    return emit


@mcp._mcp_server.call_tool()  # type: ignore[misc]
async def _call_tool(name: str, arguments: dict[str, Any] | None) -> list[TextContent]:
    """Route a tool call. Two paths:
      - `research` → run the in-house agentic loop (planner + executor +
        observer + synthesizer). Streams progress to the calling client
        when it included `_meta.progressToken` on the request.
      - any other name → transparent proxy to the upstream that owns it."""
    args = arguments or {}
    if name == "research":
        emit = _build_progress_emitter()
        result = await proxy.research(
            args.get("query") or "",
            max_steps=int(args.get("max_steps", 12) or 12),
            max_seconds=float(args.get("max_seconds", 90) or 90),
            answer_style=str(args.get("answer_style", "concise") or "concise"),
            emit=emit,
        )
    else:
        result = await proxy.call_upstream(name, args)
    return [TextContent(type="text", text=json.dumps(result, default=str))]


def get_status() -> dict[str, Any]:
    """Used by the readiness/health hooks if we add one later."""
    return proxy.snapshot_status()
