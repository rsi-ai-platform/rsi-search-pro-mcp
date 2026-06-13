"""Upstream proxy — talks JSON-RPC over streamable-HTTP to the two real MCP
services (authority-web-search-mcp and browser-research-mcp) and surfaces
their tools as a single catalog.

The aggregation lives entirely at request time: we never bundle the upstream
code. So when authority's playbook is updated or browser-research adds a new
step verb, this server picks it up automatically inside one TTL window
(~5 min). Only changes to the routing logic itself force a redeploy here.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any

import httpx
from cachetools import TTLCache

log = logging.getLogger("rsi_search_pro")


# ============================================================================
# Upstream endpoints. Env-overridable for staging / testing.
# Production defaults point at the live Cloud Run services.
# ============================================================================

UPSTREAMS: dict[str, str] = {
    "authority-web-search": os.environ.get(
        "AUTHORITY_WEB_SEARCH_URL",
        "https://authority-web-search-mcp-pef65a33ta-el.a.run.app/mcp",
    ),
    "browser-research": os.environ.get(
        "BROWSER_RESEARCH_URL",
        "https://browser-research-mcp-pef65a33ta-el.a.run.app/mcp",
    ),
}


# ============================================================================
# Shared httpx client. The upstream Cloud Run services are stateless-HTTP
# so we never need to track Mcp-Session-Id; every call is independent.
# ============================================================================

_http_client: httpx.AsyncClient | None = None
_http_lock = asyncio.Lock()


async def _http() -> httpx.AsyncClient:
    global _http_client
    if _http_client is not None and not _http_client.is_closed:
        return _http_client
    async with _http_lock:
        if _http_client is not None and not _http_client.is_closed:
            return _http_client
        # Read timeout is generous because browser-research.extract can take
        # 30-45s (Chromium launch + page render + Sonnet vision). We let the
        # upstream's own timeout drive the failure boundary.
        _http_client = httpx.AsyncClient(
            timeout=httpx.Timeout(connect=10.0, read=180.0, write=10.0, pool=10.0),
            limits=httpx.Limits(max_connections=80, max_keepalive_connections=30),
            follow_redirects=True,
            headers={
                "User-Agent": "rsi-search-pro/0.1 (+aggregator)",
                "Content-Type": "application/json",
                # streamable-http requires this Accept header — even when the
                # upstream responds with plain JSON (it varies on body shape).
                "Accept": "application/json, text/event-stream",
            },
        )
        return _http_client


# ============================================================================
# Catalog + routing. The catalog is the merged tools/list across upstreams;
# routing is name → upstream. Both live behind a 5-min TTL cache so we don't
# re-discover on every list_tools call but still pick up upstream changes
# inside a normal user-session lifetime.
# ============================================================================

_catalog_cache: TTLCache = TTLCache(maxsize=1, ttl=300)
_routing_cache: TTLCache = TTLCache(maxsize=1, ttl=300)
_discover_lock = asyncio.Lock()


async def _rpc(url: str, method: str, params: dict[str, Any] | None = None,
                req_id: str = "1", timeout: float | None = None) -> dict[str, Any]:
    """Send a single JSON-RPC request to an upstream MCP. Returns the parsed
    JSON-RPC response body (with `result` or `error`)."""
    payload = {"jsonrpc": "2.0", "id": req_id, "method": method,
                "params": params or {}}
    client = await _http()
    kwargs: dict[str, Any] = {"json": payload}
    if timeout is not None:
        kwargs["timeout"] = timeout
    r = await client.post(url, **kwargs)
    r.raise_for_status()
    # Upstream replies with application/json in stateless mode. SSE only
    # happens for streaming endpoints we don't use here.
    return r.json()


async def discover_tools(*, force: bool = False) -> tuple[list[dict[str, Any]], dict[str, str]]:
    """Query both upstreams' tools/list, merge into a flat catalog, build a
    routing table mapping each tool name back to its upstream.

    Cached for 5 min. On cache miss (or `force=True`), the discovery runs
    against both upstreams in parallel. Failures on one upstream don't fail
    the whole call — the live one's tools are still surfaced.
    """
    if not force:
        cached = _catalog_cache.get("c")
        cached_r = _routing_cache.get("r")
        if cached is not None and cached_r is not None:
            return cached, cached_r

    async with _discover_lock:
        # Re-check under the lock — somebody else might have populated.
        if not force:
            cached = _catalog_cache.get("c")
            cached_r = _routing_cache.get("r")
            if cached is not None and cached_r is not None:
                return cached, cached_r

        async def _one(name: str, url: str) -> tuple[str, list[dict[str, Any]]]:
            try:
                resp = await _rpc(url, "tools/list", req_id=f"list-{name}",
                                    timeout=20.0)
                if "error" in resp:
                    log.warning("upstream %s tools/list error: %s",
                                 name, resp["error"])
                    return name, []
                return name, list(resp.get("result", {}).get("tools") or [])
            except Exception as e:  # noqa: BLE001
                log.warning("upstream %s unreachable: %s", name, e)
                return name, []

        results = await asyncio.gather(
            *(_one(name, url) for name, url in UPSTREAMS.items())
        )

        catalog: list[dict[str, Any]] = []
        routing: dict[str, str] = {}
        for upstream_name, tools_list in results:
            for t in tools_list:
                tname = t.get("name")
                if not tname:
                    continue
                if tname in routing:
                    # First upstream wins on collision. We log so collisions
                    # don't go unnoticed.
                    log.warning("tool name collision: %s already mapped to %s, "
                                "ignoring duplicate from %s",
                                tname, routing[tname], upstream_name)
                    continue
                routing[tname] = upstream_name
                catalog.append(t)

        _catalog_cache["c"] = catalog
        _routing_cache["r"] = routing
        log.info("catalog refreshed: %d tools across %d upstreams",
                  len(catalog), len(UPSTREAMS))
        return catalog, routing


async def call_upstream(tool_name: str, arguments: dict[str, Any]) -> Any:
    """Proxy a tools/call to whichever upstream owns `tool_name`.

    Returns the upstream's response unwrapped to its native shape (the tool's
    own returned dict — not the MCP `content` envelope). When the upstream's
    content is JSON-shaped (our case for all of authority-web-search and
    browser-research tools), we parse it; otherwise we wrap as `{text: …}`.
    """
    _, routing = await discover_tools()
    upstream_name = routing.get(tool_name)
    if upstream_name is None:
        # Re-discover once — upstream may have just rolled a new tool.
        _, routing = await discover_tools(force=True)
        upstream_name = routing.get(tool_name)
    if upstream_name is None:
        return {
            "error": (
                f"tool {tool_name!r} not found in any upstream. Known: "
                f"{sorted(routing.keys())[:20]}…"
            ),
            "tool": tool_name,
        }

    url = UPSTREAMS[upstream_name]
    t0 = time.perf_counter()
    try:
        resp = await _rpc(
            url, "tools/call",
            params={"name": tool_name, "arguments": arguments or {}},
            req_id=f"call-{tool_name}",
        )
    except httpx.HTTPError as e:
        log.warning("upstream call failed: %s on %s: %s",
                     tool_name, upstream_name, e)
        return {"error": f"upstream call failed: {e}", "tool": tool_name,
                "upstream": upstream_name}

    dt_ms = round((time.perf_counter() - t0) * 1000)
    if "error" in resp:
        err = resp["error"]
        log.warning("upstream JSON-RPC error on %s/%s: %s",
                     upstream_name, tool_name, err)
        return {"error": err.get("message", str(err)),
                "tool": tool_name, "upstream": upstream_name,
                "code": err.get("code")}

    result = resp.get("result") or {}
    content = result.get("content") or []
    out: Any
    if isinstance(content, list) and content and content[0].get("type") == "text":
        text = content[0].get("text") or ""
        try:
            out = json.loads(text)
        except json.JSONDecodeError:
            out = {"text": text}
    else:
        out = result

    log.info(json.dumps({
        "evt": "proxy", "tool": tool_name, "upstream": upstream_name,
        "duration_ms": dt_ms,
        "is_error": bool(result.get("isError")),
    }))
    return out


def upstream_for(tool_name: str) -> str | None:
    """Synchronous routing lookup against the latest cached routing table.
    Used for telemetry / debug; falls back to None if not yet discovered."""
    routing = _routing_cache.get("r")
    if routing is None:
        return None
    return routing.get(tool_name)


def snapshot_status() -> dict[str, Any]:
    """Read-only summary of the current proxy state. Useful for /health and
    debug — exposes upstream URLs + last-known tool counts."""
    catalog = _catalog_cache.get("c") or []
    routing = _routing_cache.get("r") or {}
    per_upstream: dict[str, int] = {name: 0 for name in UPSTREAMS}
    for tname, uname in routing.items():
        per_upstream[uname] = per_upstream.get(uname, 0) + 1
    return {
        "upstreams": UPSTREAMS,
        "total_tools": len(catalog),
        "tools_per_upstream": per_upstream,
        "current_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
    }
