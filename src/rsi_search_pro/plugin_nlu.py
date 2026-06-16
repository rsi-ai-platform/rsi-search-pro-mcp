"""Plugin-attached NLU layer for the authority-web-search MCP.

When the MCP is configured with `ATTACHED_PLUGINS=indian_finance` (and
`PLUGIN_BUNDLE_BASE_URL` pointing at the backend), it pulls the plugin's
indicator catalog + authority rules and uses them to interpret incoming
queries BEFORE running any tool.

The understanding step maps:

    "India energy consumption statistics"
      → indicators: ["electricity_generation", "energy_consumption"]
      → source_ids: ["CEA", "POWER", "MOSPI"]
      → jurisdiction: "IN"

so that `pick_authority_domains` can pass those source-ids into its
authority-domains lookup INSTEAD of falling back to the generic
topic_hint list.

Behaviour summary:
  - Bundle is fetched at first use and cached for `PLUGIN_BUNDLE_TTL_SECONDS`
    (default 5 minutes). On fetch failure, a stale-but-cached bundle is
    used; if no cached bundle exists, NLU returns an empty result and the
    caller falls through to its non-NLU code path.
  - Haiku is the planner (cheap, fast). The 93-indicator catalog goes into
    a cache_control:ephemeral system block so the prompt cost amortises to
    near-zero after warm-up.
  - The result is a structured plan, not free text — callers can act on
    `indicators` / `source_ids` directly.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from typing import Any

import httpx

log = logging.getLogger("rsi_search_pro.plugin_nlu")

_BUNDLE_BASE_URL = os.environ.get(
    "PLUGIN_BUNDLE_BASE_URL",
    "https://agentic-rag-backend-800435094335.asia-south1.run.app/api/plugins",
).rstrip("/")
_ATTACHED = [
    p.strip()
    for p in os.environ.get("ATTACHED_PLUGINS", "").split(",")
    if p.strip()
]
_TTL = float(os.environ.get("PLUGIN_BUNDLE_TTL_SECONDS", "300"))
_NLU_MODEL = os.environ.get("PLUGIN_NLU_MODEL", "claude-haiku-4-5-20251001")
_NLU_MAX_INDICATORS = int(os.environ.get("PLUGIN_NLU_MAX_INDICATORS", "4"))

_bundles: dict[str, dict[str, Any]] = {}
_bundle_ts: dict[str, float] = {}
_fetch_lock = asyncio.Lock()


def enabled() -> bool:
    return bool(_ATTACHED)


def attached_plugin_ids() -> list[str]:
    return list(_ATTACHED)


_FETCH_TIMEOUT = float(os.environ.get("PLUGIN_BUNDLE_FETCH_TIMEOUT", "30"))
_FETCH_RETRIES = int(os.environ.get("PLUGIN_BUNDLE_FETCH_RETRIES", "3"))


async def _fetch_bundle(plugin_id: str) -> dict[str, Any] | None:
    """Fetch the bundle with retries + exponential backoff. Empty-string
    httpx errors (the original 15s timeout symptom) are now logged with
    their exception type so future incidents are diagnosable."""
    url = f"{_BUNDLE_BASE_URL}/{plugin_id}/bundle"
    last_err: Exception | None = None
    for attempt in range(1, _FETCH_RETRIES + 1):
        try:
            async with httpx.AsyncClient(timeout=_FETCH_TIMEOUT) as client:
                r = await client.get(url)
                r.raise_for_status()
                data = r.json()
                log.info(
                    "Fetched plugin bundle %s on attempt %d (%d indicators, %d authority rules)",
                    plugin_id, attempt,
                    data.get("counts", {}).get("indicators", 0),
                    data.get("counts", {}).get("authority_rules", 0),
                )
                return data
        except Exception as e:  # noqa: BLE001
            last_err = e
            log.warning(
                "Bundle fetch attempt %d/%d for %s failed (%s: %r)",
                attempt, _FETCH_RETRIES, plugin_id, type(e).__name__, str(e),
            )
            if attempt < _FETCH_RETRIES:
                await asyncio.sleep(min(2 ** attempt, 8))
    log.error(
        "Bundle fetch giving up for %s after %d attempts; last error: %s: %r",
        plugin_id, _FETCH_RETRIES, type(last_err).__name__,
        str(last_err) if last_err else "<no exception captured>",
    )
    return None


async def get_bundles(force: bool = False) -> list[dict[str, Any]]:
    """Return the attached plugin bundles, refreshing any older than TTL."""
    if not enabled():
        return []
    now = time.time()
    out: list[dict[str, Any]] = []
    async with _fetch_lock:
        for pid in _ATTACHED:
            if (
                not force
                and pid in _bundles
                and (now - _bundle_ts.get(pid, 0)) < _TTL
            ):
                out.append(_bundles[pid])
                continue
            b = await _fetch_bundle(pid)
            if b:
                _bundles[pid] = b
                _bundle_ts[pid] = now
                out.append(b)
            elif pid in _bundles:
                log.info("Using stale bundle %s after fetch failure", pid)
                out.append(_bundles[pid])
    return out


def _candidate_block(bundles: list[dict[str, Any]], max_lines: int = 250) -> str:
    """Compact indicator listing for the Haiku system prompt."""
    lines: list[str] = []
    for b in bundles:
        for ind in b.get("indicators") or []:
            tags = ", ".join((ind.get("tags") or [])[:5]) or "—"
            disp = (ind.get("display_name") or "")[:40]
            desc = (ind.get("description") or "")[:80]
            lines.append(
                f"- {ind['key']:30s}  {disp:40s}  tags: {tags:55s}  {desc}"
            )
            if len(lines) >= max_lines:
                return "\n".join(lines)
    return "\n".join(lines)


def _rules_lookup(
    bundles: list[dict[str, Any]],
) -> dict[tuple[str, str], list[str]]:
    """{(indicator_key, jurisdiction): [source_ids]}. Jurisdiction "" means
    catch-all."""
    out: dict[tuple[str, str], list[str]] = {}
    for b in bundles:
        for r in b.get("authority_rules") or []:
            key = (r.get("indicator_key") or "", (r.get("jurisdiction") or "").upper())
            if key[0]:
                out[key] = list(r.get("sources") or [])
    return out


def _sources_for_indicator(
    rules: dict[tuple[str, str], list[str]],
    indicator: str,
    jurisdiction: str,
) -> list[str]:
    j = (jurisdiction or "").upper()
    for try_key in (
        (indicator, j),
        (indicator, "IN"),
        (indicator, ""),
    ):
        if try_key in rules:
            return rules[try_key]
    return []


def _strip_code_fence(text: str) -> str:
    t = text.strip()
    if t.startswith("```"):
        t = t.strip("`")
        if t.lower().startswith("json"):
            t = t[4:]
    return t.strip()


_NLU_SYSTEM_INSTRUCTIONS = (
    "You are a routing planner for an Indian financial / regulatory / "
    "industrial data agent. Given a user query, identify which indicators "
    "from the attached plugin catalog apply.\n\n"
    "Rules:\n"
    "  • Use ONLY indicator keys present in the catalog below — never invent.\n"
    f"  • Pick at most {_NLU_MAX_INDICATORS} indicators that BEST match. "
    "Return [] if the query is generic or off-domain.\n"
    "  • Set confidence in [0,1]: 0.9+ when the query names an indicator "
    "explicitly; 0.6-0.8 when implied by tags or display names; below 0.5 "
    "when matches are weak.\n"
    "  • Default jurisdiction is IN unless the query clearly names another "
    "country (Fed/SEC/BoE/ECB/PBOC/NHTSA → US/UK/EU/CN).\n\n"
    'Output JSON ONLY — no prose, no markdown fence: '
    '{"indicators": [str], "jurisdiction": "IN", '
    '"confidence": float, "rationale": str}'
)


async def understand(
    query: str,
    *,
    jurisdiction_hint: str | None = None,
) -> dict[str, Any]:
    """Map a free-text query to {indicators, source_ids, jurisdiction,
    confidence, rationale}. Returns an empty / unconfident plan when:
      - no plugins are attached
      - the bundle fetch failed and no cache is warm
      - ANTHROPIC_API_KEY is unset
      - Haiku returns non-JSON
    Callers must fall through gracefully in those cases."""
    bundles = await get_bundles()
    juris = (jurisdiction_hint or "").upper().strip()
    if not bundles:
        # Distinguish the 3 reasons we'd get here so the trace is diagnosable:
        #   1. ATTACHED_PLUGINS env not set            → enabled() is False
        #   2. env set, but fetch failed (no cache)    → "bundle_fetch_failed"
        #   3. env set, fetch returned non-dict        → handled inside _fetch_bundle
        if not enabled():
            rationale = (
                "ATTACHED_PLUGINS env not set on this MCP — no plugin attached. "
                "Set ATTACHED_PLUGINS=<id> + PLUGIN_BUNDLE_BASE_URL=<backend>."
            )
        else:
            rationale = (
                f"Plugin {_ATTACHED!r} attached but bundle fetch failed. "
                f"Check that {_BUNDLE_BASE_URL}/<id>/bundle is reachable from "
                "this MCP (cold-start backend? network?). Will retry on next call."
            )
        return {
            "indicators": [],
            "source_ids": [],
            "jurisdiction": juris or "IN",
            "confidence": 0.0,
            "rationale": rationale,
            "plugins": [],
        }

    if not juris:
        juris = bundles[0].get("default_jurisdiction") or "IN"

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return {
            "indicators": [],
            "source_ids": [],
            "jurisdiction": juris,
            "confidence": 0.0,
            "rationale": "ANTHROPIC_API_KEY not set on the MCP",
            "plugins": [b.get("plugin_id") for b in bundles],
        }

    catalog = _candidate_block(bundles)
    system_blocks = [
        {
            "type": "text",
            "text": (
                _NLU_SYSTEM_INSTRUCTIONS
                + "\n\nCATALOG (indicator_key  display_name  tags  description):\n"
                + catalog
            ),
            "cache_control": {"type": "ephemeral"},
        }
    ]
    user_msg = (
        f"Query: {query!r}\n"
        f"Jurisdiction hint: {juris}\n\n"
        "Return the JSON plan."
    )

    try:
        from anthropic import AsyncAnthropic
        client = AsyncAnthropic(api_key=api_key)
        resp = await client.messages.create(
            model=_NLU_MODEL,
            max_tokens=400,
            system=system_blocks,
            messages=[{"role": "user", "content": user_msg}],
        )
        text = "".join(b.text for b in resp.content if b.type == "text")
    except Exception as e:  # noqa: BLE001
        log.warning("NLU Haiku call failed: %s", e)
        return {
            "indicators": [],
            "source_ids": [],
            "jurisdiction": juris,
            "confidence": 0.0,
            "rationale": f"nlu_haiku_failed: {e!s}",
            "plugins": [b.get("plugin_id") for b in bundles],
        }

    try:
        parsed = json.loads(_strip_code_fence(text))
    except Exception:
        return {
            "indicators": [],
            "source_ids": [],
            "jurisdiction": juris,
            "confidence": 0.0,
            "rationale": f"nlu_parse_error: {text[:160]}",
            "plugins": [b.get("plugin_id") for b in bundles],
        }

    indicators = [
        i for i in (parsed.get("indicators") or []) if isinstance(i, str)
    ][:_NLU_MAX_INDICATORS]
    juris_out = (parsed.get("jurisdiction") or juris or "IN").upper()

    rules = _rules_lookup(bundles)
    source_ids: list[str] = []
    for ind in indicators:
        source_ids.extend(_sources_for_indicator(rules, ind, juris_out))
    # Dedup while preserving order.
    source_ids = list(dict.fromkeys(source_ids))

    return {
        "indicators": indicators,
        "source_ids": source_ids,
        "jurisdiction": juris_out,
        "confidence": float(parsed.get("confidence") or 0.0),
        "rationale": parsed.get("rationale") or "",
        "plugins": [b.get("plugin_id") for b in bundles],
    }


async def warm_up() -> None:
    """Best-effort pre-fetch of every attached bundle. Call at MCP startup
    so the first user request doesn't pay the fetch latency."""
    if not enabled():
        log.info("plugin_nlu warm_up: no plugins attached")
        return
    try:
        bundles = await get_bundles(force=True)
        log.info(
            "plugin_nlu warm_up: fetched %d bundle(s) for plugins=%s",
            len(bundles), [b.get("plugin_id") for b in bundles],
        )
    except Exception as e:  # noqa: BLE001
        log.warning("plugin_nlu warm_up failed (will retry lazily): %s: %r",
                    type(e).__name__, e)


def _schedule_warmup() -> None:
    """Spawn a daemon thread that runs warm_up() in its own event loop. Fires at
    module import time so the bundle is usually cached before the first user
    request arrives. Safe to call in any sync context."""
    if not enabled():
        return
    if os.environ.get("PLUGIN_NLU_DISABLE_WARMUP"):
        log.info("plugin_nlu warmup disabled via env")
        return
    import threading

    def _runner() -> None:
        try:
            asyncio.run(warm_up())
        except Exception as e:  # noqa: BLE001
            log.warning("warmup thread failed: %s: %r", type(e).__name__, e)

    threading.Thread(
        target=_runner,
        daemon=True,
        name="plugin-nlu-warmup",
    ).start()


# Fire warm-up on import. Daemon thread → never blocks MCP startup.
_schedule_warmup()


def status() -> dict[str, Any]:
    return {
        "enabled": enabled(),
        "attached_plugin_ids": _ATTACHED,
        "bundle_base_url": _BUNDLE_BASE_URL,
        "ttl_seconds": _TTL,
        "model": _NLU_MODEL,
        "cached_bundles": list(_bundles.keys()),
    }
