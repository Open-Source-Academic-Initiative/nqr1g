import logging

from prometheus_client import CONTENT_TYPE_LATEST, REGISTRY, Counter, Histogram, generate_latest


logger = logging.getLogger("OpenSAI")

_LATENCY_BUCKETS = (0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 20.0, 40.0, 80.0, 120.0)


def configure_logging() -> None:
    root = logging.getLogger()
    if root.handlers:
        return
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
    root.addHandler(handler)
    root.setLevel(logging.INFO)


def _counter(name: str, doc: str, labels: list[str]) -> Counter:
    try:
        return Counter(name, doc, labels)
    except ValueError:
        return REGISTRY._names_to_collectors[name]  # type: ignore[attr-defined,return-value]


def _histogram(name: str, doc: str, labels: list[str], buckets: tuple[float, ...] | None = None) -> Histogram:
    try:
        if buckets is None:
            return Histogram(name, doc, labels)
        return Histogram(name, doc, labels, buckets=buckets)
    except ValueError:
        return REGISTRY._names_to_collectors[name]  # type: ignore[attr-defined,return-value]


HTTP_REQUESTS_TOTAL = _counter(
    "http_requests_total",
    "Total HTTP requests served by the application",
    ["method", "path", "status_code"],
)
HTTP_REQUEST_DURATION_SECONDS = _histogram(
    "http_request_duration_seconds",
    "HTTP request duration in seconds",
    ["method", "path"],
    _LATENCY_BUCKETS,
)
SOCRATA_REQUESTS_TOTAL = _counter(
    "socrata_requests_total",
    "Total Socrata requests by source and status code",
    ["source", "status_code"],
)
SOCRATA_REQUEST_DURATION_SECONDS = _histogram(
    "socrata_request_duration_seconds",
    "Socrata request duration in seconds",
    ["source"],
    _LATENCY_BUCKETS,
)
SOCRATA_ERRORS_TOTAL = _counter(
    "socrata_errors_total",
    "Socrata request errors by source and error type",
    ["source", "error_type"],
)
SOCRATA_BREAKER_EVENTS_TOTAL = _counter(
    "socrata_breaker_events_total",
    "Circuit breaker state transitions by source",
    ["source", "event"],
)


def record_breaker_event(source: str, event: str) -> None:
    SOCRATA_BREAKER_EVENTS_TOTAL.labels(source, event).inc()


def record_http_request(method: str, path: str, status_code: int, elapsed_seconds: float) -> None:
    HTTP_REQUESTS_TOTAL.labels(method, path, str(status_code)).inc()
    HTTP_REQUEST_DURATION_SECONDS.labels(method, path).observe(elapsed_seconds)


def record_socrata_request(source: str, status_code: int, elapsed_seconds: float) -> None:
    SOCRATA_REQUESTS_TOTAL.labels(source, str(status_code)).inc()
    SOCRATA_REQUEST_DURATION_SECONDS.labels(source).observe(elapsed_seconds)


def record_socrata_error(source: str, error_type: str) -> None:
    SOCRATA_ERRORS_TOTAL.labels(source, error_type).inc()


def render_metrics() -> tuple[bytes, str]:
    return generate_latest(), CONTENT_TYPE_LATEST
