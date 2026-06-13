# rsi-search-pro-mcp

**One MCP, all the web-research firepower.** A transparent meta-MCP that
proxies two upstream Cloud Run services:

| Upstream | Surface |
|---|---|
| [authority-web-search-mcp](https://github.com/rsi-ai-platform/authority-web-search-mcp) | 12 tools — Tavily authoritative search, structured fetch, PDF discovery + fetch, AJAX form POST, sitemap walk, Indian-context default routing |
| [browser-research-mcp](https://github.com/rsi-ai-platform/browser-research-mcp) | 3 tools — `visit` / `extract` / `act` via real Chromium + Sonnet vision |

The aggregator does **no work at build time** — it discovers each upstream's
`tools/list` at request time and routes `tools/call` by name. Updates to
either upstream propagate within one TTL window (~5 min); this service only
needs to redeploy when the routing logic itself changes.

## Fetch ladder

The aggregator's instructions tell the agent to follow a strict cost ladder:

```
web_search_authoritative → web_fetch_structured → pdf_fetch_structured →
http_post_form → (last resort) visit / extract / act
```

The browser tools are 5-15× slower than the PDF/AJAX path; using them when
the cheap rungs would have worked just burns Chromium-CPU on Cloud Run.

## Run locally

```bash
uv tool install rsi-search-pro-mcp --python 3.12

# stdio (Claude Desktop, Cursor, …)
uvx rsi-search-pro

# streamable-http (your own backend)
uvx rsi-search-pro --transport streamable-http --port 7863
```

## Environment

| Var | Default | Purpose |
|---|---|---|
| `AUTHORITY_WEB_SEARCH_URL` | the prod Cloud Run URL | Override per environment |
| `BROWSER_RESEARCH_URL` | the prod Cloud Run URL | Override per environment |
| `MCP_TRANSPORT` | `stdio` | `stdio` / `sse` / `streamable-http` |
| `MCP_HOST` / `PORT` | `0.0.0.0` / `7863` | Bind for the HTTP transports |
| `ALLOWED_HOSTS` | (unset → DNS-rebinding disabled) | Comma-separated allowlist |
| `FORWARDED_ALLOW_IPS` | `*` (in Dockerfile) | Trust proxy headers |

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                  Agent (your backend)                    │
│                          │                               │
│                          │  POST /mcp                    │
│                          ▼                               │
│      ┌──────────────────────────────────────────┐        │
│      │   rsi-search-pro-mcp (Cloud Run)          │        │
│      │                                            │        │
│      │   tools/list  →  merge from upstreams      │        │
│      │   tools/call  →  route to upstream by name │        │
│      │                                            │        │
│      │   5-min catalog TTL                        │        │
│      └────────────┬────────────────────┬──────────┘        │
│                   │                    │                   │
│        ┌──────────▼──────┐   ┌────────▼────────────┐       │
│        │ authority-web-  │   │ browser-research-   │       │
│        │ search-mcp      │   │ mcp                 │       │
│        │ (Cloud Run)     │   │ (Cloud Run)         │       │
│        └─────────────────┘   └─────────────────────┘       │
└─────────────────────────────────────────────────────────┘
```

## License

Apache-2.0.
