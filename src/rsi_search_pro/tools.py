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
import re
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

# Six-upstream pool. Each entry carries the URL, whether the upstream is a
# stateful MCP (needs Mcp-Session-Id threaded), an optional bearer token
# (CGA), and a short capability hint the research() planner uses to route.
# Stateless upstreams skip the initialize round-trip entirely.
UPSTREAMS: dict[str, dict[str, Any]] = {
    "authority-web-search": {
        "url": os.environ.get(
            "AUTHORITY_WEB_SEARCH_URL",
            "https://authority-web-search-mcp-pef65a33ta-el.a.run.app/mcp",
        ),
        "stateful": False,
        "token": None,
        "capability": (
            "Authoritative web search + structured fetch. Tavily 3-pass "
            "ladder against 87 curated authority domains, PDF / Excel / "
            "CSV download + parse, AJAX form POST for dropdown-driven gov "
            "dashboards, sitemap walk, Indian-context default routing, "
            "query playbook hints (e.g. 'GST → Excel on gst.gov.in')."
        ),
    },
    "browser-research": {
        "url": os.environ.get(
            "BROWSER_RESEARCH_URL",
            "https://browser-research-mcp-pef65a33ta-el.a.run.app/mcp",
        ),
        "stateful": False,
        "token": None,
        "capability": (
            "Real Chromium via patched Playwright + Sonnet vision. "
            "Last-resort: JS-rendered SPAs, login walls, chart-only "
            "values drawn via canvas/SVG, dynamic dropdowns whose data "
            "isn't in the HTML. Slow (5-45s) — only when cheap rungs fail."
        ),
    },
    "rbi-dbie": {
        "url": os.environ.get(
            "RBI_DBIE_URL",
            "https://rbi-dbie-mcp-pef65a33ta-el.a.run.app/mcp",
        ),
        "stateful": False,
        "token": None,
        "capability": (
            "Reserve Bank of India — Database on Indian Economy. 474 "
            "SDMX-style datasets across Financial Market, External "
            "Sector, Financial Sector, Public Finance, Real Sector. "
            "Canonical for: policy repo rate, reference FX rates, M0/M1/"
            "M3 money supply, BoP, govt securities yields."
        ),
    },
    "mospi-esankhyiki": {
        "url": os.environ.get(
            "MOSPI_ESANKHYIKI_URL",
            "https://mcp.mospi.gov.in/",
        ),
        "stateful": False,
        "token": None,
        "capability": (
            "Ministry of Statistics & Programme Implementation (MoSPI) "
            "eSankhyiki MCP. CPI (headline + groups), IIP, GDP / GVA, "
            "ASI manufacturing, services sector, employment. Canonical "
            "for Indian macro indicators."
        ),
    },
    "data-gov": {
        "url": os.environ.get(
            "DATA_GOV_URL",
            "https://data-gov-mcp-800435094335.asia-south1.run.app/mcp",
        ),
        "stateful": False,
        "token": None,
        "capability": (
            "India's open-data portal — 100k+ government datasets across "
            "every ministry and state. Tools: search_datasets (keyword "
            "search), get_dataset_info (metadata), get_data (rows). The "
            "long-tail ladder for any 'I need official Indian X' query."
        ),
    },
    "cga": {
        "url": os.environ.get(
            "CGA_URL",
            "https://cga-mcp-800435094335.us-central1.run.app/mcp",
        ),
        "stateful": True,
        "token": os.environ.get("CGA_AUTH_TOKEN"),
        "capability": (
            "Controller General of Accounts (Ministry of Finance). "
            "Monthly union government accounts, NSDP (state-level fiscal "
            "indicators), finance accounts, circulars, dashboard data. "
            "Canonical for Indian central + state public finance series."
        ),
    },
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


# Short upstream ids used as tool-name prefixes ONLY when the same tool name
# exists in multiple upstreams (e.g. SDMX-style `get_data` on RBI DBIE +
# MoSPI + Data.gov). Unique names stay bare for ergonomics. Picking
# explicit short ids keeps the prefixed names short and human-readable.
_UPSTREAM_SHORT_ID: dict[str, str] = {
    "authority-web-search": "aws",
    "browser-research": "br",
    "rbi-dbie": "rbi",
    "mospi-esankhyiki": "mospi",
    "data-gov": "dgov",
    "cga": "cga",
}


# Per-upstream session state for stateful MCPs (e.g. CGA). Initialized on
# first call to the upstream and reused across the process lifetime.
_session_ids: dict[str, str | None] = {name: None for name in UPSTREAMS}
_session_locks: dict[str, asyncio.Lock] = {name: asyncio.Lock() for name in UPSTREAMS}


async def _rpc_to(upstream_name: str, method: str,
                    params: dict[str, Any] | None = None,
                    *, req_id: str = "1", timeout: float | None = None,
                    is_notification: bool = False) -> dict[str, Any]:
    """Send a single JSON-RPC request to an upstream MCP. Threads the right
    bearer token + session id based on the upstream's config. Returns the
    parsed JSON-RPC response body (with `result` or `error`).

    For stateful upstreams (currently CGA), lazily initializes the session
    on the first call and reuses it thereafter. Stateless upstreams
    short-circuit the initialize round-trip.
    """
    spec = UPSTREAMS[upstream_name]
    url = spec["url"]
    body: dict[str, Any] = {"jsonrpc": "2.0", "method": method,
                              "params": params or {}}
    if not is_notification:
        body["id"] = req_id

    headers: dict[str, str] = {}
    if spec.get("token"):
        headers["Authorization"] = f"Bearer {spec['token']}"
    sid = _session_ids.get(upstream_name)
    if sid:
        headers["Mcp-Session-Id"] = sid

    # Ensure the session is initialized for stateful upstreams before any
    # non-initialize call. Re-entrant: initialize() goes through this same
    # path with method == "initialize", short-circuiting the guard.
    if spec.get("stateful") and method not in ("initialize", "notifications/initialized") and sid is None:
        await _ensure_session(upstream_name)
        sid = _session_ids.get(upstream_name)
        if sid:
            headers["Mcp-Session-Id"] = sid

    client = await _http()
    kwargs: dict[str, Any] = {"json": body, "headers": headers}
    if timeout is not None:
        kwargs["timeout"] = timeout
    r = await client.post(url, **kwargs)
    # Capture the session id from the initialize response BEFORE
    # raise_for_status — even error responses tag the header on
    # stateful servers.
    if method == "initialize":
        new_sid = r.headers.get("mcp-session-id") or r.headers.get("Mcp-Session-Id")
        if new_sid:
            _session_ids[upstream_name] = new_sid.strip()
    r.raise_for_status()
    if is_notification:
        return {}
    # Some MCP servers (RBI DBIE, MoSPI eSankhyiki, the SDK's default
    # stateful mode) reply with text/event-stream rather than plain JSON.
    # Detect by content-type and pull the single JSON-encoded data event.
    ctype = (r.headers.get("content-type") or "").lower()
    if ctype.startswith("text/event-stream"):
        return _parse_sse_single(r.text)
    return r.json()


def _parse_sse_single(body: str) -> dict[str, Any]:
    """Read one JSON-encoded `data:` event from an SSE response body."""
    for raw in body.splitlines():
        line = raw.strip()
        if not line or line.startswith(":"):
            continue
        if line.startswith("data:"):
            try:
                return json.loads(line[5:].strip())
            except Exception:
                continue
    raise RuntimeError("no parseable SSE event in response")


async def _ensure_session(upstream_name: str) -> None:
    """Initialize a stateful upstream's session and send the required
    notifications/initialized. No-op for stateless upstreams or if a
    session id already exists. Concurrent calls are serialised."""
    spec = UPSTREAMS[upstream_name]
    if not spec.get("stateful"):
        return
    if _session_ids.get(upstream_name):
        return
    async with _session_locks[upstream_name]:
        if _session_ids.get(upstream_name):
            return
        try:
            await _rpc_to(upstream_name, "initialize", {
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {}},
                "clientInfo": {"name": "rsi-search-pro", "version": "0.2"},
            }, req_id="init", timeout=20.0)
            if _session_ids.get(upstream_name):
                # Per MCP spec, must send notifications/initialized
                # before any other request once the session is open.
                try:
                    await _rpc_to(upstream_name, "notifications/initialized",
                                    {}, timeout=10.0, is_notification=True)
                except Exception as e:  # noqa: BLE001
                    log.warning("notifications/initialized %s: %s",
                                upstream_name, e)
            else:
                log.warning("%s initialize returned no session id", upstream_name)
        except Exception as e:  # noqa: BLE001
            log.warning("%s initialize failed: %s", upstream_name, e)


async def _rpc(url: str, method: str, params: dict[str, Any] | None = None,
                req_id: str = "1", timeout: float | None = None) -> dict[str, Any]:
    """LEGACY shim retained for backwards-compat with the discover_tools
    code path. Resolves the upstream by URL and delegates to _rpc_to."""
    upstream_name = next(
        (name for name, spec in UPSTREAMS.items() if spec["url"] == url),
        None,
    )
    if upstream_name is None:
        # Fallback to old behaviour — unknown URL, plain JSON-RPC POST.
        payload = {"jsonrpc": "2.0", "id": req_id, "method": method,
                    "params": params or {}}
        client = await _http()
        kwargs: dict[str, Any] = {"json": payload}
        if timeout is not None:
            kwargs["timeout"] = timeout
        r = await client.post(url, **kwargs)
        r.raise_for_status()
        return r.json()
    return await _rpc_to(upstream_name, method, params,
                           req_id=req_id, timeout=timeout)


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

        async def _one(name: str) -> tuple[str, list[dict[str, Any]]]:
            try:
                resp = await _rpc_to(name, "tools/list",
                                       req_id=f"list-{name}", timeout=20.0)
                if "error" in resp:
                    log.warning("upstream %s tools/list error: %s",
                                 name, resp["error"])
                    return name, []
                return name, list(resp.get("result", {}).get("tools") or [])
            except Exception as e:  # noqa: BLE001
                log.warning("upstream %s unreachable: %s", name, e)
                return name, []

        results = await asyncio.gather(
            *(_one(name) for name in UPSTREAMS)
        )

        # First pass: count how many upstreams export each tool name.
        from collections import Counter
        name_counts: Counter = Counter()
        for upstream_name, tools_list in results:
            for t in tools_list:
                if t.get("name"):
                    name_counts[t["name"]] += 1

        # Second pass: emit catalog with namespacing on collisions.
        # routing[name] = (upstream, original_name) so call_upstream can
        # un-prefix before dispatching to the upstream.
        catalog: list[dict[str, Any]] = []
        routing: dict[str, tuple[str, str]] = {}
        for upstream_name, tools_list in results:
            short = _UPSTREAM_SHORT_ID.get(upstream_name, upstream_name)
            for t in tools_list:
                original = t.get("name")
                if not original:
                    continue
                if name_counts[original] > 1:
                    # Collision: prefix EVERY copy so no caller silently
                    # gets the wrong upstream's tool.
                    exposed = f"{short}__{original}"
                    desc = f"[{upstream_name}] " + (t.get("description") or "")
                else:
                    exposed = original
                    desc = t.get("description") or ""
                routing[exposed] = (upstream_name, original)
                catalog.append({
                    "name": exposed,
                    "description": desc,
                    "inputSchema": t.get("inputSchema"),
                })

        _catalog_cache["c"] = catalog
        _routing_cache["r"] = routing
        log.info("catalog refreshed: %d tools across %d upstreams "
                  "(collisions: %d)", len(catalog), len(UPSTREAMS),
                  sum(1 for n, c in name_counts.items() if c > 1))
        return catalog, routing


async def call_upstream(tool_name: str, arguments: dict[str, Any]) -> Any:
    """Proxy a tools/call to whichever upstream owns `tool_name`.

    Returns the upstream's response unwrapped to its native shape (the tool's
    own returned dict — not the MCP `content` envelope). When the upstream's
    content is JSON-shaped (our case for all of authority-web-search and
    browser-research tools), we parse it; otherwise we wrap as `{text: …}`.
    """
    _, routing = await discover_tools()
    entry = routing.get(tool_name)
    if entry is None:
        # Re-discover once — upstream may have just rolled a new tool.
        _, routing = await discover_tools(force=True)
        entry = routing.get(tool_name)
    if entry is None:
        return {
            "error": (
                f"tool {tool_name!r} not found in any upstream. Known: "
                f"{sorted(routing.keys())[:20]}…"
            ),
            "tool": tool_name,
        }
    upstream_name, original_name = entry

    t0 = time.perf_counter()
    try:
        resp = await _rpc_to(
            upstream_name, "tools/call",
            params={"name": original_name, "arguments": arguments or {}},
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
    Returns just the upstream name (the routing table stores
    (upstream, original_name) tuples to handle name collisions; callers of
    upstream_for only care about which MCP it lives in)."""
    routing = _routing_cache.get("r")
    if routing is None:
        return None
    entry = routing.get(tool_name)
    if entry is None:
        return None
    return entry[0]


def snapshot_status() -> dict[str, Any]:
    """Read-only summary of the current proxy state. Useful for /health and
    debug — exposes upstream URLs + last-known tool counts."""
    catalog = _catalog_cache.get("c") or []
    routing = _routing_cache.get("r") or {}
    per_upstream: dict[str, int] = {name: 0 for name in UPSTREAMS}
    for tname, entry in routing.items():
        uname = entry[0] if isinstance(entry, tuple) else entry
        per_upstream[uname] = per_upstream.get(uname, 0) + 1
    return {
        "upstreams": {name: {"url": spec["url"], "stateful": spec.get("stateful")}
                       for name, spec in UPSTREAMS.items()},
        "total_tools": len(catalog),
        "tools_per_upstream": per_upstream,
        "current_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
    }


# ============================================================================
# Agentic research loop. Planner (Sonnet) → execute upstream tool →
# Observer (Haiku) judges good/reroute/done → repeat → Synthesizer (Sonnet)
# produces the final answer. Hard caps on steps, wall time, and tokens.
# Returns the full trace so the Playground UI can render the loop post-hoc
# with timestamps — same UX as live streaming once the call completes.
# ============================================================================

_anthropic_client: Any | None = None
_anthropic_lock = asyncio.Lock()


def _anthropic_key() -> str | None:
    return os.environ.get("ANTHROPIC_API_KEY") or None


async def _anthropic():
    global _anthropic_client
    if not _anthropic_key():
        return None
    if _anthropic_client is None:
        async with _anthropic_lock:
            if _anthropic_client is None:
                from anthropic import AsyncAnthropic
                _anthropic_client = AsyncAnthropic(api_key=_anthropic_key())
    return _anthropic_client


_PLANNER_SYSTEM = (
    "You are the PLANNER for RSI Search Pro — an agentic research MCP for "
    "Indian financial / regulatory / industrial questions.\n\n"
    "JURISDICTION DEFAULT: India. Indian fiscal year runs April → March; "
    "'FY26' = 2025-04-01 → 2026-03-31. Currency defaults to INR.\n\n"
    "You receive (1) the user's query and (2) the catalog of every tool "
    "across all upstream MCPs, with their domain capabilities. Pick the "
    "MOST EFFICIENT ladder of steps that will answer the query.\n\n"
    "FETCH LADDER — cheap rungs first:\n"
    "  1. pick_authority_domains (authority-web-search) — uses the query\n"
    "     playbook to surface domain-specific hints (e.g. GST → Excel).\n"
    "  2. web_fetch_structured / pdf_fetch_structured / "
    "     excel_fetch_structured for direct sources.\n"
    "  3. http_post_form for AJAX-dropdown gov dashboards (PPAC etc).\n"
    "  4. Source-specific upstreams: RBI DBIE (repo rate, FX), eSankhyiki\n"
    "     MoSPI (CPI, IIP, GDP), Data.gov.in (long-tail datasets), CGA\n"
    "     (union/state public finance).\n"
    "  5. browser-research visit / act / extract — LAST RESORT (5-45s,\n"
    "     real Chromium). Only when 1-4 won't reach the data.\n\n"
    "Plan 2-5 steps. Each step MUST be CONCRETE — give exact URLs / "
    "indicator codes / dataset ids when you know them. The planner output "
    "becomes the executor's input verbatim.\n\n"
    "OUTPUT JSON ONLY (no prose, no fences):\n"
    "{\n"
    '  "objective": "<one-sentence restatement of what we are trying to answer>",\n'
    '  "success_criterion": "<how we will know we have the answer>",\n'
    '  "steps": [\n'
    '    {"tool": "<tool_name>", "args": {...}, "rationale": "<why this step>"}\n'
    "  ]\n"
    "}"
)


_OBSERVER_SYSTEM = (
    "You are the OBSERVER for RSI Search Pro's research loop. You judge "
    "ONE tool result and decide whether to keep going, re-route, or stop.\n\n"
    "INPUT: the loop's objective, the last step we ran, and a summary of\n"
    "the tool result.\n\n"
    "JUDGE EXACTLY ONE OF:\n"
    "  'good'    — the result contributes useful data; CONTINUE with the next\n"
    "              planned step.\n"
    "  'reroute' — empty / wrong / error / unrelated. PROPOSE a DIFFERENT\n"
    "              tool or upstream to try next. NEVER suggest re-running the\n"
    "              same tool with the same args.\n"
    "  'done'    — we already have enough data to answer the objective; skip\n"
    "              any remaining planned steps.\n\n"
    "Be HONEST. An empty result is empty — say so, don't pretend.\n\n"
    "OUTPUT JSON ONLY:\n"
    "{\n"
    '  "judgement": "good"|"reroute"|"done",\n'
    '  "notes": "<one or two sentences explaining the judgement>",\n'
    '  "next_step": {"tool": "...", "args": {...}, "rationale": "..."}'
    "  // include next_step ONLY when judgement is 'reroute'\n"
    "}"
)


_SYNTHESIZER_SYSTEM = (
    "You are the SYNTHESIZER for RSI Search Pro. Combine the findings from "
    "a research loop into a clear, cited answer for the user.\n\n"
    "RULES:\n"
    "  - Cite every load-bearing number with its source URL + period\n"
    "    (e.g. '₹1.74 lakh crore (gst.gov.in, May 2026)').\n"
    "  - Indian fiscal-year semantics: 'Apr 2025', not 'April'.\n"
    "  - If findings are incomplete, say so — don't fabricate.\n"
    "  - Markdown body; clean prose.\n\n"
    "CONFIDENCE LEVELS (pick ONE):\n"
    "  'high'   — answered with cited numbers from authoritative sources.\n"
    "  'medium' — partial / some inference / cross-source disagreement.\n"
    "  'low'    — only loose inference; explicitly flag the gaps.\n"
    "  'none'   — could not answer; tools returned no usable data.\n\n"
    "OUTPUT JSON ONLY:\n"
    "{\n"
    '  "answer": "<markdown answer or \\"I could not find this\\" with reasons>",\n'
    '  "confidence": "high"|"medium"|"low"|"none",\n'
    '  "citations": [{"source": "...", "url": "...", "period": "..."}]\n'
    "}"
)


def _parse_relaxed_json(text: str) -> dict[str, Any]:
    """Pull JSON object out of an LLM response, tolerating fences + truncation."""
    s = (text or "").strip()
    s = re.sub(r"^```(?:json)?\s*", "", s)
    s = re.sub(r"\s*```$", "", s)
    if not s:
        return {}
    start = s.find("{")
    if start < 0:
        return {}
    s = s[start:]
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        pass
    # Salvage truncated JSON by closing dangling brackets.
    for end in range(len(s), 0, -1):
        cand = s[:end]
        if cand.count('"') % 2 == 1:
            cand += '"'
        for ch, m in (("[", "]"), ("{", "}")):
            opens = cand.count(ch) - cand.count(m)
            if opens > 0:
                cand += m * opens
        try:
            return json.loads(cand)
        except json.JSONDecodeError:
            continue
    return {}


def _build_catalog_brief(catalog: list[dict[str, Any]],
                          routing: dict[str, str]) -> str:
    """Short tools/list summary the planner sees. Groups tools by upstream
    and prepends the upstream's capability blurb so the planner knows what
    each MCP is good at."""
    by_upstream: dict[str, list[dict[str, Any]]] = {n: [] for n in UPSTREAMS}
    for t in catalog:
        entry = routing.get(t["name"])
        u = entry[0] if isinstance(entry, tuple) else entry
        if u and u in by_upstream:
            by_upstream[u].append(t)
    parts: list[str] = ["CATALOG (by upstream):"]
    for u, tools in by_upstream.items():
        if not tools:
            continue
        cap = UPSTREAMS[u].get("capability", "")
        parts.append(f"\n## {u}\n{cap}\nTools:")
        for t in tools:
            desc = (t.get("description") or "").replace("\n", " ").strip()
            parts.append(f"  - {t['name']}: {desc[:140]}")
    return "\n".join(parts)


def _summarize_result(result: Any, max_chars: int = 3000) -> str:
    """Compact a tool result for the observer. We strip big payloads
    (screenshot_b64, raw_content) and JSON-dump the rest, truncated."""
    if not isinstance(result, dict):
        return str(result)[:max_chars]
    clean = {k: v for k, v in result.items()
              if k not in ("screenshot_b64", "raw_content", "content")}
    if "content" in result and isinstance(result["content"], str):
        clean["content"] = result["content"][:1200]
    try:
        s = json.dumps(clean, default=str, ensure_ascii=False, indent=2)
    except Exception:
        s = str(clean)
    return s[:max_chars]


def _extract_error(result: Any) -> str | None:
    if not isinstance(result, dict):
        return None
    err = result.get("error") or result.get("error_extract")
    if err:
        return str(err)[:200]
    return None


async def _llm_call(model: str, system: str, user: str,
                     *, max_tokens: int = 1500) -> dict[str, Any]:
    """Single Anthropic call. Returns {text, usage} or {error}."""
    client = await _anthropic()
    if client is None:
        return {"error": "ANTHROPIC_API_KEY not set", "text": "", "usage": {}}
    try:
        resp = await client.messages.create(
            model=model,
            max_tokens=max_tokens,
            system=[{"type": "text", "text": system,
                      "cache_control": {"type": "ephemeral"}}],
            messages=[{"role": "user", "content": user}],
        )
        text = "".join(b.text for b in resp.content if b.type == "text")
        u = getattr(resp, "usage", None)
        usage = {
            "input_tokens": getattr(u, "input_tokens", 0) if u else 0,
            "output_tokens": getattr(u, "output_tokens", 0) if u else 0,
            "cache_read_input_tokens": getattr(u, "cache_read_input_tokens", 0) if u else 0,
        }
        return {"text": text, "usage": usage}
    except Exception as e:  # noqa: BLE001
        return {"error": str(e), "text": "", "usage": {}}


def _planner_model() -> str:
    return os.environ.get("RSP_PLANNER_MODEL", "claude-sonnet-4-6")


def _observer_model() -> str:
    return os.environ.get("RSP_OBSERVER_MODEL", "claude-haiku-4-5-20251001")


def _synth_model() -> str:
    return os.environ.get("RSP_SYNTH_MODEL", "claude-sonnet-4-6")


async def research(
    query: str,
    *,
    max_steps: int = 12,
    max_seconds: float = 90.0,
    max_input_tokens: int = 25_000,
    answer_style: str = "concise",
) -> dict[str, Any]:
    """Agentic research loop. Planner → Executor → Observer → Synthesizer.

    Returns: {query, answer, confidence, citations, findings_count, trace,
              data_gap (if no answer), stats}.

    The trace is rich and timestamped so a UI can render a live-feeling
    timeline after the call returns.
    """
    t_overall = time.perf_counter()
    trace: list[dict[str, Any]] = []
    findings: list[dict[str, Any]] = []
    observations: list[dict[str, Any]] = []
    tokens_in_used = 0
    tokens_out_used = 0

    def _now_ms() -> int:
        return round((time.perf_counter() - t_overall) * 1000)

    catalog, routing = await discover_tools()
    catalog_brief = _build_catalog_brief(catalog, routing)

    # ── PHASE 1: PLAN ────────────────────────────────────────────────────
    plan_t0 = time.perf_counter()
    plan_resp = await _llm_call(
        _planner_model(),
        _PLANNER_SYSTEM,
        f"USER QUERY: {query}\n\n{catalog_brief}\n\n"
        f"Plan ≤{max_steps} steps. Return JSON only.",
        max_tokens=2200,
    )
    plan = _parse_relaxed_json(plan_resp.get("text", ""))
    tokens_in_used += plan_resp.get("usage", {}).get("input_tokens", 0)
    tokens_out_used += plan_resp.get("usage", {}).get("output_tokens", 0)
    trace.append({
        "kind": "plan", "t_ms": _now_ms(),
        "duration_ms": round((time.perf_counter() - plan_t0) * 1000),
        "objective": plan.get("objective", "(planner returned no objective)"),
        "success_criterion": plan.get("success_criterion", ""),
        "steps_planned": len(plan.get("steps") or []),
        "plan": plan,
        "usage": plan_resp.get("usage", {}),
    })
    if "error" in plan_resp:
        trace.append({"kind": "planner_failed", "t_ms": _now_ms(),
                       "error": plan_resp["error"]})

    objective = plan.get("objective") or query
    pending_steps: list[dict[str, Any]] = list(plan.get("steps") or [])

    # ── PHASE 2: EXECUTE + OBSERVE loop ──────────────────────────────────
    steps_executed = 0
    stopped_reason = "plan_completed"
    for step_idx in range(1, max_steps + 1):
        # Budget gates
        if (time.perf_counter() - t_overall) > max_seconds:
            stopped_reason = "time_cap"
            trace.append({"kind": "budget", "t_ms": _now_ms(),
                           "reason": "time_cap_hit",
                           "elapsed_s": round(time.perf_counter() - t_overall, 1)})
            break
        if tokens_in_used > max_input_tokens:
            stopped_reason = "token_cap"
            trace.append({"kind": "budget", "t_ms": _now_ms(),
                           "reason": "token_cap_hit",
                           "tokens_in_used": tokens_in_used})
            break
        if not pending_steps:
            stopped_reason = "plan_completed"
            break

        step = pending_steps.pop(0)
        tool_name = step.get("tool") or ""
        args = step.get("args") or {}
        rationale = step.get("rationale", "")
        entry = routing.get(tool_name)
        upstream_name = entry[0] if isinstance(entry, tuple) else None

        step_t0 = time.perf_counter()
        try:
            result = await call_upstream(tool_name, args)
        except Exception as e:  # noqa: BLE001
            result = {"error": str(e), "exception": True}
        step_dt = round((time.perf_counter() - step_t0) * 1000)
        steps_executed += 1

        err = _extract_error(result)
        trace.append({
            "kind": "step", "n": step_idx, "t_ms": _now_ms(),
            "duration_ms": step_dt,
            "tool": tool_name, "args": args, "rationale": rationale,
            "upstream": upstream_name or "unknown",
            "is_error": bool(err),
            "error": err,
            "result_summary": _summarize_result(result, max_chars=1500),
        })

        # ── OBSERVE ──
        obs_t0 = time.perf_counter()
        budget_left = max(0.0, max_seconds - (time.perf_counter() - t_overall))
        obs_user = (
            f"OBJECTIVE: {objective}\n\n"
            f"LAST STEP (n={step_idx}):\n"
            f"  tool: {tool_name}\n"
            f"  args: {json.dumps(args, default=str)[:400]}\n"
            f"  rationale: {rationale}\n\n"
            f"RESULT (compact):\n{_summarize_result(result, max_chars=2500)}\n\n"
            f"REMAINING PLANNED STEPS: {len(pending_steps)}\n"
            f"TIME BUDGET LEFT: {budget_left:.0f}s\n\n"
            f"Catalog snippet for reroute (top 30 tool names):\n"
            f"{', '.join(routing.keys())[:1200]}\n\n"
            "Return JSON only."
        )
        obs_resp = await _llm_call(_observer_model(), _OBSERVER_SYSTEM,
                                      obs_user, max_tokens=700)
        obs_dt = round((time.perf_counter() - obs_t0) * 1000)
        obs = _parse_relaxed_json(obs_resp.get("text", ""))
        tokens_in_used += obs_resp.get("usage", {}).get("input_tokens", 0)
        tokens_out_used += obs_resp.get("usage", {}).get("output_tokens", 0)
        judgement = (obs.get("judgement") or "good").lower()
        observations.append({"n": step_idx, "judgement": judgement,
                              "notes": obs.get("notes", "")})
        trace.append({
            "kind": "observe", "n": step_idx, "t_ms": _now_ms(),
            "duration_ms": obs_dt,
            "judgement": judgement,
            "notes": obs.get("notes", ""),
            "next_step": obs.get("next_step"),
            "usage": obs_resp.get("usage", {}),
        })

        if judgement == "good":
            findings.append({"step": step, "result": result,
                              "notes": obs.get("notes", "")})
        elif judgement == "reroute":
            next_step = obs.get("next_step")
            if isinstance(next_step, dict) and next_step.get("tool"):
                # Insert observer's reroute at the front of the queue.
                pending_steps.insert(0, next_step)
            # Don't add to findings — the result was empty/wrong.
        elif judgement == "done":
            findings.append({"step": step, "result": result,
                              "notes": obs.get("notes", "")})
            stopped_reason = "observer_done"
            break
        else:
            # Unknown judgement — treat as good to make progress.
            findings.append({"step": step, "result": result})

    # ── PHASE 3: SYNTHESIZE ──────────────────────────────────────────────
    synth_t0 = time.perf_counter()
    findings_brief = "\n\n".join([
        f"Finding {i+1}:\n  step: {json.dumps(f['step'], default=str)[:300]}\n"
        f"  result: {_summarize_result(f['result'], max_chars=2500)}\n"
        f"  notes: {f.get('notes','')}"
        for i, f in enumerate(findings)
    ]) or "(no findings)"
    synth_user = (
        f"USER QUERY: {query}\n"
        f"OBJECTIVE: {objective}\n"
        f"STOPPED REASON: {stopped_reason}\n\n"
        f"FINDINGS:\n{findings_brief}\n\nReturn JSON only."
    )
    synth_resp = await _llm_call(_synth_model(), _SYNTHESIZER_SYSTEM,
                                    synth_user, max_tokens=2500)
    synth = _parse_relaxed_json(synth_resp.get("text", ""))
    synth_dt = round((time.perf_counter() - synth_t0) * 1000)
    tokens_in_used += synth_resp.get("usage", {}).get("input_tokens", 0)
    tokens_out_used += synth_resp.get("usage", {}).get("output_tokens", 0)
    trace.append({
        "kind": "synth", "t_ms": _now_ms(),
        "duration_ms": synth_dt,
        "confidence": synth.get("confidence", "low"),
        "usage": synth_resp.get("usage", {}),
    })

    answer = synth.get("answer", "") or "I could not produce a final answer."
    confidence = (synth.get("confidence") or "low").lower()
    citations = synth.get("citations") or []

    # ── PHASE 4: DATA GAP FLAG ──────────────────────────────────────────
    # Fire whenever the synthesizer says 'none' — the answer pointed at
    # next-step sources but the actual value was not retrieved. Also fire
    # on 'low' with no citations at all. Both cases are honest "I couldn't"
    # signals worth queueing for follow-up ingest.
    data_gap = None
    if confidence == "none" or (confidence == "low" and not citations):
        rec_sources: list[str] = []
        # Best-effort: surface the upstreams the planner intended to use.
        for s in (plan.get("steps") or []):
            entry = routing.get(s.get("tool") or "")
            uname = entry[0] if isinstance(entry, tuple) else None
            if uname and uname not in rec_sources:
                rec_sources.append(uname)
        data_gap = {
            "title": f"RSI Search Pro could not answer: {query[:180]}",
            "description": (
                f"research() exhausted its plan (reason: {stopped_reason}) "
                f"without finding cited data. Steps executed: {steps_executed}. "
                f"Worth reviewing the trace to either add a new source MCP, "
                f"extend a playbook entry, or queue a manual ingest."
            ),
            "requesting_query": query,
            "recommended_sources": rec_sources[:6],
            "indicators": [],
            "period": None,
            "priority": "normal",
            "requested_by": "rsi-search-pro.research",
        }
        trace.append({"kind": "data_gap", "t_ms": _now_ms(),
                       "data_gap": data_gap})

    total_dt = round((time.perf_counter() - t_overall) * 1000)
    return {
        "query": query,
        "answer": answer,
        "confidence": confidence,
        "citations": citations,
        "findings_count": len(findings),
        "trace": trace,
        "data_gap": data_gap,
        "stopped_reason": stopped_reason,
        "stats": {
            "duration_ms": total_dt,
            "steps_executed": steps_executed,
            "tokens_in": tokens_in_used,
            "tokens_out": tokens_out_used,
            "upstreams_used": sorted({
                t.get("upstream") for t in trace
                if t.get("kind") == "step" and t.get("upstream") != "unknown"
            }),
        },
    }
