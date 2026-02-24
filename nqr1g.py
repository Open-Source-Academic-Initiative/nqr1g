"""
OpenSAI - SECOP Unified Query Microservice (I and II)
Migrated to FastAPI - Version 3.0.0
Date: February 2026
"""

import asyncio
import html
import logging
import os
import random
import re
import secrets
import time
import unicodedata
import uuid
from collections import deque
from datetime import datetime
from typing import Any, cast
from urllib.parse import urlsplit

import httpx
import pandas as pd
from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse
from fastapi.templating import Jinja2Templates
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from starlette.middleware.base import BaseHTTPMiddleware

# --- CONFIGURATION ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
logger = logging.getLogger("OpenSAI")

app = FastAPI(title="OpenSAI - SECOP API", version="3.0.0")
UPSTREAM_FAILURE_MESSAGE = (
    "El servicio de https://www.datos.gov.co/ ha fallado o no responde en este momento. "
    "Intente nuevamente más tarde."
)

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

APP_ENV = os.getenv("APP_ENV", "development").strip().lower()
SECURITY_STRICT_MODE_RAW = os.getenv("SECURITY_STRICT_MODE", "auto").strip().lower()


def _is_truthy(value: str) -> bool:
    return value in {"1", "true", "yes", "on"}


def _is_falsy(value: str) -> bool:
    return value in {"0", "false", "no", "off"}


if _is_truthy(SECURITY_STRICT_MODE_RAW):
    STRICT_SECURITY_MODE = True
elif _is_falsy(SECURITY_STRICT_MODE_RAW):
    STRICT_SECURITY_MODE = False
else:
    STRICT_SECURITY_MODE = APP_ENV in {"production", "prod"}


def parse_cors_origins(raw: str) -> list[str]:
    return [origin.strip() for origin in raw.split(",") if origin.strip()]


def build_csp_header(nonce: str) -> str:
    if STRICT_SECURITY_MODE:
        return (
            "default-src 'self'; "
            f"script-src 'self' 'nonce-{nonce}' https://cdn.jsdelivr.net https://www.googletagmanager.com; "
            f"style-src 'self' 'nonce-{nonce}' https://cdn.jsdelivr.net https://cdnjs.cloudflare.com; "
            "font-src 'self' https://cdnjs.cloudflare.com; "
            "img-src 'self' data: https:; "
            "connect-src 'self' https://www.datos.gov.co https://www.google-analytics.com https://region1.google-analytics.com; "
            "base-uri 'self'; object-src 'none'; frame-ancestors 'none'; form-action 'self';"
        )
    return (
        "default-src 'self'; "
        "script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net https://www.googletagmanager.com; "
        "style-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net https://cdnjs.cloudflare.com; "
        "font-src https://cdnjs.cloudflare.com; "
        "connect-src 'self' https://www.datos.gov.co;"
    )

# Security Headers Middleware
class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        request.state.csp_nonce = secrets.token_urlsafe(16)
        response = await call_next(request)
        response.headers['X-Content-Type-Options'] = 'nosniff'
        response.headers['X-Frame-Options'] = 'DENY'
        response.headers['Content-Security-Policy'] = build_csp_header(request.state.csp_nonce)
        if STRICT_SECURITY_MODE:
            response.headers['Strict-Transport-Security'] = 'max-age=31536000; includeSubDomains; preload'
            response.headers['Referrer-Policy'] = 'strict-origin-when-cross-origin'
            response.headers['Permissions-Policy'] = 'geolocation=(), microphone=(), camera=()'
        return response


class RequestContextMiddleware(BaseHTTPMiddleware):
    """Inject request id and latency logging for traceability."""

    async def dispatch(self, request: Request, call_next):
        request_id = request.headers.get("X-Request-ID", str(uuid.uuid4()))
        started_at = time.perf_counter()
        response = await call_next(request)
        elapsed_seconds = time.perf_counter() - started_at
        elapsed_ms = elapsed_seconds * 1000
        response.headers["X-Request-ID"] = request_id
        HTTP_REQUESTS_TOTAL.labels(request.method, request.url.path, str(response.status_code)).inc()
        HTTP_REQUEST_DURATION_SECONDS.labels(request.method, request.url.path).observe(elapsed_seconds)
        logger.info(
            "request_id=%s method=%s path=%s status=%s latency_ms=%.2f",
            request_id,
            request.method,
            request.url.path,
            response.status_code,
            elapsed_ms,
        )
        return response


app.add_middleware(SecurityHeadersMiddleware)
app.add_middleware(RequestContextMiddleware)

CORS_ALLOW_ORIGINS_RAW = os.getenv("CORS_ALLOW_ORIGINS", "*")
if STRICT_SECURITY_MODE:
    strict_cors_raw = os.getenv("CORS_ALLOW_ORIGINS_STRICT", CORS_ALLOW_ORIGINS_RAW)
    CORS_ALLOW_ORIGINS = [origin for origin in parse_cors_origins(strict_cors_raw) if origin != "*"]
    if not CORS_ALLOW_ORIGINS:
        logger.warning("Strict security mode enabled but no explicit CORS origins configured.")
else:
    CORS_ALLOW_ORIGINS = parse_cors_origins(CORS_ALLOW_ORIGINS_RAW)

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ALLOW_ORIGINS if STRICT_SECURITY_MODE else (CORS_ALLOW_ORIGINS or ["*"]),
    allow_methods=["GET"],
    allow_headers=["*"],
    allow_credentials=STRICT_SECURITY_MODE,
)

templates = Jinja2Templates(directory="templates")

# Constants
SOCRATA_APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN", None)
PER_PAGE = max(1, int(os.getenv("PER_PAGE", "50")))
MAX_QUERY_WINDOW = max(PER_PAGE, int(os.getenv("MAX_QUERY_WINDOW", "5000")))
MAX_CONCURRENT_REQUESTS = max(1, int(os.getenv("MAX_CONCURRENT_REQUESTS", "5")))
MAX_RETRIES = max(0, int(os.getenv("SOCRATA_MAX_RETRIES", "0")))
RETRY_BASE_SECONDS = max(0.1, float(os.getenv("SOCRATA_RETRY_BASE_SECONDS", "0.4")))
MAX_RETRY_DELAY_SECONDS = max(0.2, float(os.getenv("SOCRATA_MAX_RETRY_DELAY_SECONDS", "1.2")))
RETRYABLE_STATUS_CODES = {202, 429, 500, 502, 503, 504}
SOCRATA_SEARCH_MODE = os.getenv("SOCRATA_SEARCH_MODE", "exact_or_composed").strip().lower()
USE_UNACCENT = os.getenv("SOCRATA_USE_UNACCENT", "0").strip().lower() in {"1", "true", "yes"}
TIMEOUT_CAP_SECONDS = 120.0
REQUEST_BUDGET_SECONDS = min(TIMEOUT_CAP_SECONDS, max(10.0, float(os.getenv("REQUEST_BUDGET_SECONDS", "120.0"))))
SOCRATA_REQUEST_MAX_WAIT_SECONDS = min(
    TIMEOUT_CAP_SECONDS,
    max(1.0, float(os.getenv("SOCRATA_REQUEST_MAX_WAIT_SECONDS", "120.0"))),
)
SOCRATA_CONNECT_TIMEOUT_SECONDS = min(
    TIMEOUT_CAP_SECONDS,
    max(0.2, float(os.getenv("SOCRATA_CONNECT_TIMEOUT_SECONDS", "5.0"))),
)
# httpx read timeout is inactivity-based; if upstream keeps sending chunks, the timer resets.
SOCRATA_READ_TIMEOUT_SECONDS = min(
    TIMEOUT_CAP_SECONDS,
    max(0.2, float(os.getenv("SOCRATA_READ_TIMEOUT_SECONDS", "120.0"))),
)
SOCRATA_WRITE_TIMEOUT_SECONDS = min(
    TIMEOUT_CAP_SECONDS,
    max(0.2, float(os.getenv("SOCRATA_WRITE_TIMEOUT_SECONDS", "10.0"))),
)
SOCRATA_POOL_TIMEOUT_SECONDS = min(
    TIMEOUT_CAP_SECONDS,
    max(0.2, float(os.getenv("SOCRATA_POOL_TIMEOUT_SECONDS", "5.0"))),
)
SOCRATA_HEALTH_TIMEOUT_SECONDS = min(
    TIMEOUT_CAP_SECONDS,
    max(0.2, float(os.getenv("SOCRATA_HEALTH_TIMEOUT_SECONDS", "5.0"))),
)
SOCRATA_HEALTH_CACHE_SECONDS = max(1.0, float(os.getenv("SOCRATA_HEALTH_CACHE_SECONDS", "30.0")))
THROTTLE_WINDOW_SECONDS = max(1, int(os.getenv("THROTTLE_WINDOW_SECONDS", "60")))
THROTTLE_GLOBAL_REQUESTS = max(1, int(os.getenv("THROTTLE_GLOBAL_REQUESTS", "240")))
THROTTLE_PER_IP_REQUESTS = max(1, int(os.getenv("THROTTLE_PER_IP_REQUESTS", "60")))
MAX_TRACKED_IP_BUCKETS = max(100, int(os.getenv("MAX_TRACKED_IP_BUCKETS", "5000")))
THROTTLE_ERROR_MESSAGE = (
    "Se alcanzó temporalmente el límite de consultas. "
    "Por favor intente nuevamente en unos segundos."
)
PUBLIC_BASE_URL_RAW = os.getenv("PUBLIC_BASE_URL", "").strip()

if SOCRATA_SEARCH_MODE not in {"contains", "starts_with", "exact_or_composed"}:
    logger.warning("Invalid SOCRATA_SEARCH_MODE=%s, using exact_or_composed", SOCRATA_SEARCH_MODE)
    SOCRATA_SEARCH_MODE = "exact_or_composed"

SOURCES = {
    "SECOP_I": {
        "id": "rpmr-utcd",
        "cols": {
            "id_contrato": "numero_del_contrato",
            "entidad": "nombre_de_la_entidad",
            "objeto": "objeto_a_contratar",
            "valor": "valor_contrato",
            "contratista": "nom_raz_social_contratista",
            "fecha": "fecha_de_firma_del_contrato",
            "url": "url_contrato"
        }
    },
    "SECOP_II": {
        "id": "jbjy-vk9h",
        "cols": {
            "id_contrato": "referencia_del_contrato",
            "entidad": "nombre_entidad",
            "objeto": "objeto_del_contrato",
            "valor": "valor_del_contrato",
            "contratista": "proveedor_adjudicado", 
            "fecha": "fecha_de_firma",
            "url": "urlproceso"
        }
    }
}

# --- ASYNC CLIENT ---
# Use a global async client to reuse connections (lower latency + fewer TLS handshakes).
REQUEST_SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
async_client = httpx.AsyncClient(
    timeout=httpx.Timeout(
        connect=SOCRATA_CONNECT_TIMEOUT_SECONDS,
        read=SOCRATA_READ_TIMEOUT_SECONDS,
        write=SOCRATA_WRITE_TIMEOUT_SECONDS,
        pool=SOCRATA_POOL_TIMEOUT_SECONDS,
    ),
    limits=httpx.Limits(max_keepalive_connections=10, max_connections=20),
    headers={
        "User-Agent": "OpenSAI-Bot/3.0 (FastAPI)",
        "Accept": "application/json",
        "Accept-Encoding": "gzip, deflate",
        **({"X-App-Token": SOCRATA_APP_TOKEN} if SOCRATA_APP_TOKEN else {})
    }
)
SOCRATA_HEALTH_CACHE: dict[str, Any] = {"checked_at": 0.0, "ok": True, "reason": "not_checked"}
SOCRATA_HEALTH_LOCK = asyncio.Lock()


class SlidingWindowLimiter:
    """Simple async-safe sliding window limiter."""

    def __init__(self, limit: int, window_seconds: int):
        self.limit = limit
        self.window_seconds = window_seconds
        self._events: deque[float] = deque()
        self._lock = asyncio.Lock()

    async def allow(self, now: float | None = None) -> bool:
        current = now if now is not None else time.monotonic()
        threshold = current - self.window_seconds
        async with self._lock:
            while self._events and self._events[0] <= threshold:
                self._events.popleft()
            if len(self._events) >= self.limit:
                return False
            self._events.append(current)
            return True


class PerIPSlidingWindowLimiter:
    """Per-IP sliding window limiter with stale bucket cleanup."""

    def __init__(self, limit: int, window_seconds: int, max_buckets: int):
        self.limit = limit
        self.window_seconds = window_seconds
        self.max_buckets = max_buckets
        self._events_by_ip: dict[str, deque[float]] = {}
        self._lock = asyncio.Lock()

    async def allow(self, ip: str, now: float | None = None) -> bool:
        current = now if now is not None else time.monotonic()
        threshold = current - self.window_seconds
        async with self._lock:
            events = self._events_by_ip.setdefault(ip, deque())
            while events and events[0] <= threshold:
                events.popleft()
            if len(events) >= self.limit:
                return False
            events.append(current)

            if len(self._events_by_ip) > self.max_buckets:
                stale_ips = [
                    candidate_ip
                    for candidate_ip, candidate_events in self._events_by_ip.items()
                    if not candidate_events or candidate_events[-1] <= threshold
                ]
                for candidate_ip in stale_ips:
                    self._events_by_ip.pop(candidate_ip, None)
            return True


GLOBAL_REQUEST_LIMITER = SlidingWindowLimiter(
    limit=THROTTLE_GLOBAL_REQUESTS,
    window_seconds=THROTTLE_WINDOW_SECONDS,
)
PER_IP_REQUEST_LIMITER = PerIPSlidingWindowLimiter(
    limit=THROTTLE_PER_IP_REQUESTS,
    window_seconds=THROTTLE_WINDOW_SECONDS,
    max_buckets=MAX_TRACKED_IP_BUCKETS,
)

@app.on_event("shutdown")
async def shutdown_event():
    await async_client.aclose()

# --- HELPERS ---
def clean_input(text: str) -> str:
    """Sanitize input to prevent injection and filter special characters."""
    if not text:
        return ""
    # Allow alphanumeric, spaces, dots and common Spanish characters
    return re.sub(r'[^a-zA-Z0-9\sñÑáéíóúÁÉÍÓÚ\.]', '', text).strip()

def format_soql_string(text: str) -> str:
    """Escape single quotes for SoQL queries."""
    return text.replace("'", "''")

def remove_accents(text: str) -> str:
    """Normalize accent marks for optional accent-insensitive lookups."""
    normalized = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def build_where_clause(col_map: dict[str, str], contractor: str, year: int) -> str:
    """Create a SARGable filter for year + contractor query."""
    start_date = f"{year}-01-01T00:00:00"
    end_date = f"{year}-12-31T23:59:59"
    if USE_UNACCENT:
        contractor_term = remove_accents(contractor).upper()
        field_expr = f"upper(unaccent({col_map['contratista']}))"
    else:
        contractor_term = contractor.upper()
        field_expr = f"upper({col_map['contratista']})"
    safe_contractor = format_soql_string(contractor_term)
    if SOCRATA_SEARCH_MODE == "starts_with":
        search_expr = f"starts_with({field_expr}, '{safe_contractor}')"
    elif SOCRATA_SEARCH_MODE == "contains":
        search_expr = f"contains({field_expr}, '{safe_contractor}')"
    else:
        # Exact term OR composed names that include the term with common word boundaries.
        search_expr = (
            f"{field_expr} = '{safe_contractor}' OR "
            f"starts_with({field_expr}, '{safe_contractor} ') OR "
            f"contains({field_expr}, ' {safe_contractor} ') OR "
            f"contains({field_expr}, ' {safe_contractor}') OR "
            f"contains({field_expr}, '{safe_contractor} ') OR "
            f"contains({field_expr}, '{safe_contractor}-') OR "
            f"contains({field_expr}, '-{safe_contractor}') OR "
            f"contains({field_expr}, '{safe_contractor}.') OR "
            f"contains({field_expr}, '.{safe_contractor}') OR "
            f"contains({field_expr}, '{safe_contractor},') OR "
            f"contains({field_expr}, ',{safe_contractor}') OR "
            f"contains({field_expr}, '({safe_contractor}') OR "
            f"contains({field_expr}, '{safe_contractor})') OR "
            f"contains({field_expr}, '{safe_contractor}/') OR "
            f"contains({field_expr}, '/{safe_contractor}')"
        )
    return (
        f"{col_map['fecha']} BETWEEN '{start_date}' AND '{end_date}' "
        f"AND ({search_expr})"
    )


def build_rows_params(
    col_map: dict[str, str],
    where_clause: str,
    limit: int,
    use_nested_url: bool = True,
) -> dict[str, Any]:
    url_expr = f"{col_map['url']}.url" if use_nested_url else col_map["url"]
    select_fields = [
        f"{col_map['id_contrato']} as id_contrato",
        f"{col_map['entidad']} as entidad",
        f"{col_map['objeto']} as objeto",
        f"{col_map['valor']} as valor",
        f"{col_map['contratista']} as contratista",
        f"{col_map['fecha']} as fecha",
        f"{url_expr} as url",
        ":id as row_id",
    ]
    return {
        "$select": ",".join(select_fields),
        "$where": where_clause,
        "$limit": max(1, limit),
        "$order": f"{col_map['fecha']} DESC, :id DESC",
    }


def build_count_params(where_clause: str) -> dict[str, Any]:
    return {
        "$select": "count(*) as total",
        "$where": where_clause,
        "$limit": 1,
    }


def compute_retry_delay(response: httpx.Response | None, attempt: int) -> float:
    """Compute retry delay honoring Retry-After when available."""
    if response is not None:
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                return min(float(retry_after), MAX_RETRY_DELAY_SECONDS)
            except ValueError:
                pass
    backoff = RETRY_BASE_SECONDS * (2 ** attempt)
    jitter = random.uniform(0, 0.3)
    return min(backoff + jitter, MAX_RETRY_DELAY_SECONDS)


class RequestBudgetExceeded(Exception):
    """Raised when query budget is exhausted before Socrata can reply."""


def remaining_budget_seconds(deadline: float | None) -> float:
    if deadline is None:
        return REQUEST_BUDGET_SECONDS
    return deadline - time.monotonic()


def build_request_timeout(remaining_seconds: float) -> httpx.Timeout:
    bounded = max(0.2, remaining_seconds)
    return httpx.Timeout(
        connect=min(SOCRATA_CONNECT_TIMEOUT_SECONDS, bounded),
        read=min(SOCRATA_READ_TIMEOUT_SECONDS, bounded),
        write=min(SOCRATA_WRITE_TIMEOUT_SECONDS, bounded),
        pool=min(SOCRATA_POOL_TIMEOUT_SECONDS, bounded),
    )


def get_client_ip(request: Request) -> str:
    forwarded_for = request.headers.get("X-Forwarded-For", "")
    if forwarded_for:
        first_ip = forwarded_for.split(",")[0].strip()
        if first_ip:
            return first_ip
    return request.client.host if request.client else "unknown"


def sanitize_public_base_url(url: str) -> str | None:
    if not url:
        return None
    parsed = urlsplit(url)
    if parsed.scheme not in {"http", "https"}:
        return None
    if not parsed.netloc:
        return None
    return f"{parsed.scheme}://{parsed.netloc}"


def resolve_public_base_url(request: Request) -> str:
    configured = sanitize_public_base_url(PUBLIC_BASE_URL_RAW)
    if configured:
        return configured
    if PUBLIC_BASE_URL_RAW:
        logger.warning("Invalid PUBLIC_BASE_URL=%s, using request base URL fallback", PUBLIC_BASE_URL_RAW)
    request_base = f"{request.url.scheme}://{request.url.netloc}"
    return sanitize_public_base_url(request_base) or "http://localhost:5000"


async def allow_search_request(request: Request) -> tuple[bool, str]:
    now = time.monotonic()
    if not await GLOBAL_REQUEST_LIMITER.allow(now):
        return False, "global"
    client_ip = get_client_ip(request)
    if not await PER_IP_REQUEST_LIMITER.allow(client_ip, now):
        return False, "ip"
    return True, "ok"


async def check_socrata_health(deadline: float | None = None) -> tuple[bool, str]:
    now = time.monotonic()
    if now - float(SOCRATA_HEALTH_CACHE["checked_at"]) <= SOCRATA_HEALTH_CACHE_SECONDS:
        return bool(SOCRATA_HEALTH_CACHE["ok"]), str(SOCRATA_HEALTH_CACHE["reason"])

    async with SOCRATA_HEALTH_LOCK:
        now = time.monotonic()
        if now - float(SOCRATA_HEALTH_CACHE["checked_at"]) <= SOCRATA_HEALTH_CACHE_SECONDS:
            return bool(SOCRATA_HEALTH_CACHE["ok"]), str(SOCRATA_HEALTH_CACHE["reason"])

        remaining = remaining_budget_seconds(deadline)
        if remaining <= 0:
            return False, "request_budget_exhausted"

        probe_timeout = max(0.2, min(SOCRATA_HEALTH_TIMEOUT_SECONDS, remaining))
        probe = httpx.Timeout(
            connect=probe_timeout,
            read=probe_timeout,
            write=probe_timeout,
            pool=probe_timeout,
        )
        try:
            response = await async_client.get(
                "https://www.datos.gov.co/resource/rpmr-utcd.json",
                params={"$select": ":id", "$limit": 1},
                timeout=probe,
            )
            ok = 200 <= response.status_code < 300
            reason = f"http_{response.status_code}"
        except (httpx.TimeoutException, httpx.RequestError) as exc:
            ok = False
            reason = exc.__class__.__name__

        SOCRATA_HEALTH_CACHE.update({"checked_at": time.monotonic(), "ok": ok, "reason": reason})
        return ok, reason


async def soda_get(
    endpoint: str,
    params: dict[str, Any],
    source_name: str,
    deadline: float | None = None,
) -> httpx.Response:
    """Execute Socrata request with retry policy for transient failures."""
    call_deadline = time.monotonic() + SOCRATA_REQUEST_MAX_WAIT_SECONDS
    if deadline is not None:
        call_deadline = min(call_deadline, deadline)

    for attempt in range(MAX_RETRIES + 1):
        try:
            remaining = remaining_budget_seconds(call_deadline)
            if remaining <= 0:
                raise RequestBudgetExceeded(f"Budget exhausted before source {source_name}")
            started_at = time.perf_counter()
            async with REQUEST_SEMAPHORE:
                response = await async_client.get(
                    endpoint,
                    params=params,
                    timeout=build_request_timeout(remaining),
                )
            request_id = response.headers.get("X-Socrata-RequestId", "n/a")
            elapsed_seconds = time.perf_counter() - started_at
            SOCRATA_REQUESTS_TOTAL.labels(source_name, str(response.status_code)).inc()
            SOCRATA_REQUEST_DURATION_SECONDS.labels(source_name).observe(elapsed_seconds)
            if response.status_code in RETRYABLE_STATUS_CODES and attempt < MAX_RETRIES:
                delay = compute_retry_delay(response, attempt)
                delay = min(delay, max(0.0, remaining_budget_seconds(call_deadline) - 0.05))
                if delay <= 0:
                    raise RequestBudgetExceeded(f"Budget exhausted while retrying source {source_name}")
                logger.warning(
                    "Transient Socrata status source=%s status=%s request_id=%s retry_in=%.2fs attempt=%s/%s",
                    source_name,
                    response.status_code,
                    request_id,
                    delay,
                    attempt + 1,
                    MAX_RETRIES,
                )
                await asyncio.sleep(delay)
                continue
            response.raise_for_status()
            logger.info(
                "Socrata request ok source=%s status=%s request_id=%s",
                source_name,
                response.status_code,
                request_id,
            )
            return response
        except httpx.HTTPStatusError as exc:
            status_code = exc.response.status_code if exc.response else "n/a"
            request_id = exc.response.headers.get("X-Socrata-RequestId", "n/a") if exc.response else "n/a"
            SOCRATA_ERRORS_TOTAL.labels(source_name, "http_status").inc()
            logger.error(
                "Socrata HTTP status error source=%s status=%s request_id=%s",
                source_name,
                status_code,
                request_id,
            )
            raise
        except (httpx.TimeoutException, httpx.RequestError) as exc:
            SOCRATA_ERRORS_TOTAL.labels(source_name, "transport").inc()
            if attempt < MAX_RETRIES:
                delay = compute_retry_delay(None, attempt)
                delay = min(delay, max(0.0, remaining_budget_seconds(call_deadline) - 0.05))
                if delay <= 0:
                    raise RequestBudgetExceeded(f"Budget exhausted while retrying source {source_name}")
                logger.warning(
                    "Socrata transport error source=%s error=%s retry_in=%.2fs attempt=%s/%s",
                    source_name,
                    exc,
                    delay,
                    attempt + 1,
                    MAX_RETRIES,
                )
                await asyncio.sleep(delay)
                continue
            raise
        except RequestBudgetExceeded:
            SOCRATA_ERRORS_TOTAL.labels(source_name, "budget").inc()
            raise
    raise RuntimeError(f"Unexpected retry exhaustion in source {source_name}")


async def query_source_count(
    source_name: str,
    config: dict[str, Any],
    contractor: str,
    year: int,
    deadline: float | None = None,
) -> int:
    endpoint = f"https://www.datos.gov.co/resource/{config['id']}.json"
    col_map = config['cols']
    where_clause = build_where_clause(col_map, contractor, year)
    params = build_count_params(where_clause)
    response = await soda_get(endpoint, params, f"{source_name}:count", deadline=deadline)
    payload = response.json()
    if not payload:
        return 0
    try:
        return int(payload[0].get("total", 0))
    except (TypeError, ValueError):
        logger.warning("Invalid count payload for source=%s payload=%s", source_name, payload)
        return 0


async def query_source_rows(
    source_name: str,
    config: dict[str, Any],
    contractor: str,
    year: int,
    limit: int,
    deadline: float | None = None,
) -> pd.DataFrame:
    if limit <= 0:
        return pd.DataFrame()
    endpoint = f"https://www.datos.gov.co/resource/{config['id']}.json"
    col_map = config['cols']
    where_clause = build_where_clause(col_map, contractor, year)

    try:
        params = build_rows_params(col_map, where_clause, limit, use_nested_url=True)
        response = await soda_get(endpoint, params, f"{source_name}:rows", deadline=deadline)
    except httpx.HTTPStatusError as exc:
        # Fallback if URL subfield syntax is unsupported for a specific dataset.
        if exc.response is not None and exc.response.status_code == 400:
            logger.warning("Fallback to plain URL column for source=%s", source_name)
            params = build_rows_params(col_map, where_clause, limit, use_nested_url=False)
            response = await soda_get(endpoint, params, f"{source_name}:rows:fallback", deadline=deadline)
        else:
            raise

    data = response.json()
    if not data:
        return pd.DataFrame()
    df = pd.DataFrame(data)
    df['Origen'] = source_name.replace('_', ' ')
    return df


@app.get("/healthz")
async def healthz():
    return {"status": "ok", "service": "opensai-secops"}


@app.get("/healthz/upstream")
async def healthz_upstream():
    deadline = time.monotonic() + SOCRATA_HEALTH_TIMEOUT_SECONDS + 0.5
    ok, reason = await check_socrata_health(deadline=deadline)
    status_code = 200 if ok else 503
    return JSONResponse({"status": "ok" if ok else "degraded", "upstream": "datos.gov.co", "reason": reason}, status_code=status_code)


@app.get("/metrics", response_class=PlainTextResponse)
async def metrics() -> PlainTextResponse:
    return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)

# --- ROUTES ---
@app.get("/", response_class=HTMLResponse)
async def index(
    request: Request,
    contratista: str | None = Query(None),
    anio: int = Query(datetime.now().year),
    page: int = Query(1, ge=1),
):
    current_year = datetime.now().year
    warnings: list[str] = []
    context: dict[str, Any] = {
        "request": request,
        "csp_nonce": getattr(request.state, "csp_nonce", ""),
        "public_base_url": resolve_public_base_url(request),
        "c_val": contratista or "",
        "y_val": anio,
        "current_year": current_year,
        "table": None,
        "error": None,
        "no_results": False,
        "count": 0,
        "curr_page": page,
        "pages": 0,
        "warnings": warnings,
        "limited_results": False,
        "upstream_outage": False,
        "request_budget_seconds": int(REQUEST_BUDGET_SECONDS),
    }

    if not contratista:
        return templates.TemplateResponse("index.html", context)

    allowed, throttle_reason = await allow_search_request(request)
    if not allowed:
        context["error"] = THROTTLE_ERROR_MESSAGE
        response = templates.TemplateResponse("index.html", context, status_code=429)
        response.headers["Retry-After"] = str(THROTTLE_WINDOW_SECONDS)
        logger.warning(
            "throttle_blocked reason=%s ip=%s path=%s",
            throttle_reason,
            get_client_ip(request),
            request.url.path,
        )
        return response

    try:
        # Validation
        c_clean = clean_input(contratista)
        if len(c_clean) < 3:
            raise ValueError("Ingrese al menos 3 caracteres válidos.")
            
        if not (2000 <= anio <= current_year + 1):
            raise ValueError(f"El año debe estar entre 2000 y {current_year + 1}.")

        deadline = time.monotonic() + REQUEST_BUDGET_SECONDS
        socrata_ok, socrata_reason = await check_socrata_health(deadline=deadline)
        if not socrata_ok:
            logger.error("Socrata upstream health check failed: %s", socrata_reason)
            context["upstream_outage"] = True
            context["error"] = UPSTREAM_FAILURE_MESSAGE
            return templates.TemplateResponse("index.html", context)

        # 1) Query counts first to avoid loading unneeded rows.
        count_tasks = [query_source_count(name, conf, c_clean, anio, deadline=deadline) for name, conf in SOURCES.items()]
        count_results = await asyncio.gather(*count_tasks, return_exceptions=True)

        source_totals: dict[str, int] = {}
        for (source_name, _), result in zip(SOURCES.items(), count_results, strict=False):
            if isinstance(result, BaseException):
                warnings.append(f"No se pudo consultar {source_name}.")
                logger.error("Error counting source=%s: %s", source_name, result)
                source_totals[source_name] = 0
                continue
            source_totals[source_name] = int(cast(int, result))

        total_count = sum(source_totals.values())
        if total_count == 0 and len(warnings) == len(SOURCES):
            context["upstream_outage"] = True
            context["error"] = UPSTREAM_FAILURE_MESSAGE
            return templates.TemplateResponse("index.html", context)
        if total_count == 0:
            context["no_results"] = True
            return templates.TemplateResponse("index.html", context)

        reachable_count = min(total_count, MAX_QUERY_WINDOW)
        if total_count > MAX_QUERY_WINDOW:
            context["limited_results"] = True
            warnings.append(
                f"Por rendimiento, la navegación está limitada a los primeros {MAX_QUERY_WINDOW} resultados."
            )

        total_pages = (reachable_count + PER_PAGE - 1) // PER_PAGE if reachable_count > 0 else 0
        safe_page = max(1, min(page, total_pages)) if total_pages > 0 else 1
        rows_limit = min(safe_page * PER_PAGE, MAX_QUERY_WINDOW)

        # 2) Load only the required window per source for deterministic merged pagination.
        row_tasks = [
            query_source_rows(name, conf, c_clean, anio, min(rows_limit, source_totals[name]), deadline=deadline)
            for name, conf in SOURCES.items()
            if source_totals.get(name, 0) > 0
        ]
        row_results = await asyncio.gather(*row_tasks, return_exceptions=True)
        dfs: list[pd.DataFrame] = []
        for result in row_results:
            if isinstance(result, BaseException):
                warnings.append("Una fuente devolvió error al recuperar filas.")
                logger.error("Error loading rows: %s", result)
                continue
            frame = cast(pd.DataFrame, result)
            if not frame.empty:
                dfs.append(frame)

        if not dfs:
            if warnings:
                context["upstream_outage"] = True
                context["error"] = UPSTREAM_FAILURE_MESSAGE
            else:
                context["no_results"] = True
            return templates.TemplateResponse("index.html", context)

        final_df = pd.concat(dfs, ignore_index=True)
        # Keep global order stable for pagination across both sources.
        sort_fields = [c for c in ["fecha", "row_id"] if c in final_df.columns]
        if sort_fields:
            final_df = final_df.sort_values(by=sort_fields, ascending=[False] * len(sort_fields))

        # --- DATA PROCESSING ---
        if 'valor' in final_df.columns:
            final_df['valor'] = pd.to_numeric(final_df['valor'], errors='coerce').fillna(0)
            final_df['Valor (COP)'] = final_df['valor'].apply(lambda x: f"${x:,.0f}".replace(",", "."))

        cols_display = {
            'Origen': 'Fuente',
            'id_contrato': 'ID Proceso',
            'entidad': 'Entidad',
            'objeto': 'Objeto',
            'Valor (COP)': 'Valor (COP)',
            'contratista': 'Contratista',
            'fecha': 'Fecha'
        }

        valid_cols = [c for c in cols_display.keys() if c in final_df.columns]
        df_view = final_df[valid_cols].rename(columns=cols_display)

        if 'Fecha' in df_view.columns:
            df_view['Fecha'] = df_view['Fecha'].astype(str).str.split('T').str[0]

        start_idx = (safe_page - 1) * PER_PAGE
        end_idx = safe_page * PER_PAGE
        df_page = df_view.iloc[start_idx:end_idx].copy()
        df_page.insert(0, 'No.', range(start_idx + 1, start_idx + 1 + len(df_page)))

        # SECURITY: Escape HTML for all columns except the one we will add manually
        for col in df_page.columns:
            if df_page[col].dtype == object:
                df_page[col] = df_page[col].astype(str).apply(html.escape)

        # Now add the Link column safely
        if 'url' in final_df.columns:
            def extract_clean_url(val):
                if pd.isna(val):
                    return ''
                if isinstance(val, dict):
                    inner = val.get('url', '')
                    if pd.isna(inner):
                        return ''
                    return str(inner).strip()
                normalized = str(val).strip() if val else ''
                if normalized.lower() in {'nan', 'none'}:
                    return ''
                return normalized

            # Get the URLs for the current page
            urls = final_df.loc[df_page.index, 'url'].apply(extract_clean_url)
            df_page['Enlace'] = urls.apply(
                lambda x: (
                    f'<a href="{html.escape(x)}" target="_blank" rel="noopener noreferrer" '
                    f'class="btn btn-sm btn-outline-primary">Ver</a>'
                ) if x else ''
            ).to_numpy()

        table_html = df_page.to_html(
            classes='table table-hover table-striped align-middle mb-0 small',
            index=False,
            escape=False, # We already escaped the content manually
            border=0
        )

        context.update({
            "table": table_html,
            "count": total_count,
            "curr_page": safe_page,
            "pages": total_pages
        })

        return templates.TemplateResponse("index.html", context)

    except ValueError as e:
        context["error"] = str(e)
        return templates.TemplateResponse("index.html", context)
    except RequestBudgetExceeded:
        context["upstream_outage"] = True
        context["error"] = UPSTREAM_FAILURE_MESSAGE
        return templates.TemplateResponse("index.html", context)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        context["error"] = "Error interno del servidor al procesar la solicitud."
        return templates.TemplateResponse("index.html", context)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5000)
