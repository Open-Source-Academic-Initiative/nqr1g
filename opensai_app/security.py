import asyncio
import ipaddress
import time
from collections import deque
from urllib.parse import urlsplit

from fastapi import Request

from .config import settings
from .observability import logger


def build_csp_header(nonce: str) -> str:
    if settings.strict_security_mode:
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


def get_client_ip(request: Request) -> str:
    forwarded_for = request.headers.get("X-Forwarded-For", "")
    if forwarded_for:
        first_ip = forwarded_for.split(",")[0].strip()
        if first_ip:
            return first_ip
    return request.client.host if request.client else "unknown"


def is_local_metrics_request(request: Request) -> bool:
    client_host = request.client.host if request.client else ""
    try:
        if not ipaddress.ip_address(client_host).is_loopback:
            return False
    except ValueError:
        return False

    if request.headers.get("X-Forwarded-For") or request.headers.get("Forwarded") or request.headers.get("X-Real-IP"):
        return False

    host_header = request.headers.get("Host", "").strip()
    if host_header.startswith("["):
        close_idx = host_header.find("]")
        host = host_header[1:close_idx] if close_idx > 0 else host_header.strip("[]")
    else:
        host = host_header.split(":", 1)[0]
    host = host.strip().lower()
    return not host or host in {"127.0.0.1", "localhost", "::1"}


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
    configured = sanitize_public_base_url(settings.public_base_url_raw)
    if configured:
        return configured
    if settings.public_base_url_raw:
        logger.warning(
            "Invalid PUBLIC_BASE_URL=%s, using request base URL fallback",
            settings.public_base_url_raw,
        )
    request_base = f"{request.url.scheme}://{request.url.netloc}"
    return sanitize_public_base_url(request_base) or "http://localhost:5000"


class SlidingWindowLimiter:
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


class SearchThrottle:
    def __init__(self) -> None:
        self._global_limiter = SlidingWindowLimiter(
            limit=settings.throttle.global_requests,
            window_seconds=settings.throttle.window_seconds,
        )
        self._per_ip_limiter = PerIPSlidingWindowLimiter(
            limit=settings.throttle.per_ip_requests,
            window_seconds=settings.throttle.window_seconds,
            max_buckets=settings.throttle.max_tracked_ip_buckets,
        )

    async def allow_request(self, request: Request) -> tuple[bool, str]:
        now = time.monotonic()
        if not await self._global_limiter.allow(now):
            return False, "global"
        client_ip = get_client_ip(request)
        if not await self._per_ip_limiter.allow(client_ip, now):
            return False, "ip"
        return True, "ok"
