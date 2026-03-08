import logging

from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest


logger = logging.getLogger("OpenSAI")


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

HTTP_REQUESTS_TOTAL = Counter(
    "http_requests_total",
    "Total HTTP requests served by the application",
    ["method", "path", "status_code"],
)
HTTP_REQUEST_DURATION_SECONDS = Histogram(
    "http_request_duration_seconds",
    "HTTP request duration in seconds",
    ["method", "path"],
)
SOCRATA_REQUESTS_TOTAL = Counter(
    "socrata_requests_total",
    "Total Socrata requests by source and status code",
    ["source", "status_code"],
)
SOCRATA_REQUEST_DURATION_SECONDS = Histogram(
    "socrata_request_duration_seconds",
    "Socrata request duration in seconds",
    ["source"],
)
SOCRATA_ERRORS_TOTAL = Counter(
    "socrata_errors_total",
    "Socrata request errors by source and error type",
    ["source", "error_type"],
)


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
