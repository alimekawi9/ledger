"""Structured JSON logging, request ids, and Prometheus metrics."""

import contextvars
import json
import logging
import sys
import time
import uuid
from datetime import datetime, timezone

from prometheus_client import CollectorRegistry, Counter, Histogram
from prometheus_client.core import GaugeMetricFamily
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

request_id_var: contextvars.ContextVar[str | None] = contextvars.ContextVar("request_id", default=None)

# ------------------------------------------------------------------ logging


class JsonFormatter(logging.Formatter):
    """One JSON object per line: greppable, and ingestible by any log pipeline."""

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.fromtimestamp(record.created, timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            "request_id": request_id_var.get(),
        }
        payload.update(getattr(record, "fields", {}))
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def configure_logging(level: str = "INFO") -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level)


def log_event(logger: logging.Logger, msg: str, level: int = logging.INFO, **fields) -> None:
    logger.log(level, msg, extra={"fields": fields})


# ------------------------------------------------------------------ metrics

REGISTRY = CollectorRegistry()

TRANSFERS = Counter(
    "ledger_transfers_total",
    "Transfer requests by kind and outcome "
    "(completed, rejected, replayed, key_reused, invalid, error).",
    ["kind", "outcome"],
    registry=REGISTRY,
)
TRANSFER_LATENCY = Histogram(
    "ledger_transfer_duration_seconds",
    "Server-side time to process a transfer request.",
    ["kind", "outcome"],
    buckets=(0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
    registry=REGISTRY,
)
HTTP_REQUESTS = Counter(
    "ledger_http_requests_total",
    "HTTP requests by route template and status code.",
    ["method", "route", "status"],
    registry=REGISTRY,
)
HTTP_LATENCY = Histogram(
    "ledger_http_request_duration_seconds",
    "HTTP request latency by route template.",
    ["method", "route"],
    buckets=TRANSFER_LATENCY._upper_bounds[:-1],
    registry=REGISTRY,
)


class PoolCollector:
    """Exposes connection-pool state at scrape time.

    `requests_waiting` > 0 means requests are queued for a database connection:
    the direct signal for "the pool is the bottleneck".
    """

    def __init__(self, pool):
        self.pool = pool

    def collect(self):
        stats = self.pool.get_stats()
        for key, help_text in (
            ("pool_size", "Connections currently open."),
            ("pool_available", "Idle connections."),
            ("requests_waiting", "Requests queued waiting for a connection."),
        ):
            g = GaugeMetricFamily(f"ledger_db_{key}", help_text)
            g.add_metric([], stats.get(key, 0))
            yield g
        g = GaugeMetricFamily("ledger_db_pool_max_size", "Configured maximum pool size.")
        g.add_metric([], self.pool.max_size)
        yield g
        g = GaugeMetricFamily(
            "ledger_db_requests_wait_seconds_total",
            "Cumulative time requests spent waiting for a connection.",
        )
        g.add_metric([], stats.get("requests_wait_ms", 0) / 1000)
        yield g


class RequestContextMiddleware(BaseHTTPMiddleware):
    """Assigns/propagates X-Request-ID and records HTTP metrics."""

    async def dispatch(self, request: Request, call_next):
        rid = request.headers.get("x-request-id") or str(uuid.uuid4())
        token = request_id_var.set(rid)
        start = time.perf_counter()
        status = 500
        try:
            response = await call_next(request)
            status = response.status_code
            response.headers["X-Request-ID"] = rid
            return response
        finally:
            # Label by route *template* (/accounts/{account_id}), never the raw
            # path, or every account id becomes its own time series.
            route = request.scope.get("route")
            route_label = getattr(route, "path", "unmatched")
            HTTP_REQUESTS.labels(request.method, route_label, str(status)).inc()
            HTTP_LATENCY.labels(request.method, route_label).observe(time.perf_counter() - start)
            request_id_var.reset(token)
