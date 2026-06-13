# Multi-stage build. Pure Python proxy — no browser, no extra system deps.
# Final image is ~140 MB.

FROM python:3.12-slim AS builder
WORKDIR /app
RUN pip install --no-cache-dir uv
COPY pyproject.toml README.md ./
COPY src ./src
RUN uv pip install --system --no-cache .

FROM python:3.12-slim AS runtime
WORKDIR /app
RUN useradd -r -u 1000 -ms /bin/bash app
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    LOG_LEVEL=INFO \
    MCP_TRANSPORT=streamable-http \
    MCP_HOST=0.0.0.0 \
    PORT=7863 \
    # Trust X-Forwarded-* from any IP so uvicorn honours Cloud Run's TLS
    # termination. Same fix as our other Cloud Run MCPs.
    FORWARDED_ALLOW_IPS="*"
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin/rsi-search-pro /usr/local/bin/rsi-search-pro
USER app
EXPOSE 7863
CMD ["rsi-search-pro"]
