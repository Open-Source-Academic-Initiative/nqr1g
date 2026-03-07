import asyncio
import random
import time
from typing import Any

import httpx
import pandas as pd

from .config import SourceConfig, settings
from .observability import logger, record_socrata_error, record_socrata_request


class RequestBudgetExceeded(Exception):
    """Raised when query budget is exhausted before Socrata can reply."""


def remaining_budget_seconds(deadline: float | None) -> float:
    if deadline is None:
        return settings.search.request_budget_seconds
    return deadline - time.monotonic()


class SocrataClient:
    def __init__(self) -> None:
        self._request_semaphore = asyncio.Semaphore(settings.socrata.max_concurrent_requests)
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(
                connect=settings.socrata.connect_timeout_seconds,
                read=settings.socrata.read_timeout_seconds,
                write=settings.socrata.write_timeout_seconds,
                pool=settings.socrata.pool_timeout_seconds,
            ),
            limits=httpx.Limits(max_keepalive_connections=10, max_connections=20),
            headers={
                "User-Agent": "OpenSAI-Bot/3.0 (FastAPI)",
                "Accept": "application/json",
                "Accept-Encoding": "gzip, deflate",
                **({"X-App-Token": settings.socrata.app_token} if settings.socrata.app_token else {}),
            },
        )
        self._health_cache: dict[str, Any] = {"checked_at": 0.0, "ok": True, "reason": "not_checked"}
        self._health_lock = asyncio.Lock()

    async def close(self) -> None:
        await self._client.aclose()

    def build_request_timeout(self, remaining_seconds: float) -> httpx.Timeout:
        bounded = max(0.2, remaining_seconds)
        return httpx.Timeout(
            connect=min(settings.socrata.connect_timeout_seconds, bounded),
            read=min(settings.socrata.read_timeout_seconds, bounded),
            write=min(settings.socrata.write_timeout_seconds, bounded),
            pool=min(settings.socrata.pool_timeout_seconds, bounded),
        )

    def compute_retry_delay(self, response: httpx.Response | None, attempt: int) -> float:
        if response is not None:
            retry_after = response.headers.get("Retry-After")
            if retry_after:
                try:
                    return min(float(retry_after), settings.socrata.max_retry_delay_seconds)
                except ValueError:
                    pass
        backoff = settings.socrata.retry_base_seconds * (2 ** attempt)
        jitter = random.uniform(0, 0.3)
        return min(backoff + jitter, settings.socrata.max_retry_delay_seconds)

    async def check_health(self, deadline: float | None = None) -> tuple[bool, str]:
        now = time.monotonic()
        if now - float(self._health_cache["checked_at"]) <= settings.socrata.health_cache_seconds:
            return bool(self._health_cache["ok"]), str(self._health_cache["reason"])

        async with self._health_lock:
            now = time.monotonic()
            if now - float(self._health_cache["checked_at"]) <= settings.socrata.health_cache_seconds:
                return bool(self._health_cache["ok"]), str(self._health_cache["reason"])

            remaining = remaining_budget_seconds(deadline)
            if remaining <= 0:
                return False, "request_budget_exhausted"

            probe_timeout = max(0.2, min(settings.socrata.health_timeout_seconds, remaining))
            probe = httpx.Timeout(
                connect=probe_timeout,
                read=probe_timeout,
                write=probe_timeout,
                pool=probe_timeout,
            )
            try:
                response = await self._client.get(
                    "https://www.datos.gov.co/resource/rpmr-utcd.json",
                    params={"$select": ":id", "$limit": 1},
                    timeout=probe,
                )
                ok = 200 <= response.status_code < 300
                reason = f"http_{response.status_code}"
            except (httpx.TimeoutException, httpx.RequestError) as exc:
                ok = False
                reason = exc.__class__.__name__

            self._health_cache.update({"checked_at": time.monotonic(), "ok": ok, "reason": reason})
            return ok, reason

    async def soda_get(
        self,
        endpoint: str,
        params: dict[str, Any],
        source_name: str,
        deadline: float | None = None,
    ) -> httpx.Response:
        call_deadline = time.monotonic() + settings.socrata.request_max_wait_seconds
        if deadline is not None:
            call_deadline = min(call_deadline, deadline)

        for attempt in range(settings.socrata.max_retries + 1):
            try:
                remaining = remaining_budget_seconds(call_deadline)
                if remaining <= 0:
                    raise RequestBudgetExceeded(f"Budget exhausted before source {source_name}")
                started_at = time.perf_counter()
                async with self._request_semaphore:
                    response = await self._client.get(
                        endpoint,
                        params=params,
                        timeout=self.build_request_timeout(remaining),
                    )
                request_id = response.headers.get("X-Socrata-RequestId", "n/a")
                elapsed_seconds = time.perf_counter() - started_at
                record_socrata_request(source_name, response.status_code, elapsed_seconds)
                if (
                    response.status_code in settings.socrata.retryable_status_codes
                    and attempt < settings.socrata.max_retries
                ):
                    delay = self.compute_retry_delay(response, attempt)
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
                        settings.socrata.max_retries,
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
                record_socrata_error(source_name, "http_status")
                logger.error(
                    "Socrata HTTP status error source=%s status=%s request_id=%s",
                    source_name,
                    status_code,
                    request_id,
                )
                raise
            except (httpx.TimeoutException, httpx.RequestError) as exc:
                record_socrata_error(source_name, "transport")
                if attempt < settings.socrata.max_retries:
                    delay = self.compute_retry_delay(None, attempt)
                    delay = min(delay, max(0.0, remaining_budget_seconds(call_deadline) - 0.05))
                    if delay <= 0:
                        raise RequestBudgetExceeded(f"Budget exhausted while retrying source {source_name}")
                    logger.warning(
                        "Socrata transport error source=%s error=%s retry_in=%.2fs attempt=%s/%s",
                        source_name,
                        exc,
                        delay,
                        attempt + 1,
                        settings.socrata.max_retries,
                    )
                    await asyncio.sleep(delay)
                    continue
                raise
            except RequestBudgetExceeded:
                record_socrata_error(source_name, "budget")
                raise
        raise RuntimeError(f"Unexpected retry exhaustion in source {source_name}")

    async def query_source_count(
        self,
        source_name: str,
        source_config: SourceConfig,
        where_clause: str,
        deadline: float | None = None,
    ) -> int:
        endpoint = f"https://www.datos.gov.co/resource/{source_config.dataset_id}.json"
        params = {
            "$select": "count(*) as total",
            "$where": where_clause,
            "$limit": 1,
        }
        response = await self.soda_get(endpoint, params, f"{source_name}:count", deadline=deadline)
        payload = response.json()
        if not payload:
            return 0
        try:
            return int(payload[0].get("total", 0))
        except (TypeError, ValueError):
            logger.warning("Invalid count payload for source=%s payload=%s", source_name, payload)
            return 0

    async def query_source_rows(
        self,
        source_name: str,
        source_config: SourceConfig,
        where_clause: str,
        limit: int,
        deadline: float | None = None,
    ) -> pd.DataFrame:
        if limit <= 0:
            return pd.DataFrame()

        endpoint = f"https://www.datos.gov.co/resource/{source_config.dataset_id}.json"
        try:
            params = self._build_rows_params(source_config.cols, where_clause, limit, use_nested_url=True)
            response = await self.soda_get(endpoint, params, f"{source_name}:rows", deadline=deadline)
        except httpx.HTTPStatusError as exc:
            if exc.response is not None and exc.response.status_code == 400:
                logger.warning("Fallback to plain URL column for source=%s", source_name)
                params = self._build_rows_params(source_config.cols, where_clause, limit, use_nested_url=False)
                response = await self.soda_get(endpoint, params, f"{source_name}:rows:fallback", deadline=deadline)
            else:
                raise

        data = response.json()
        if not data:
            return pd.DataFrame()
        frame = pd.DataFrame(data)
        frame["Origen"] = source_name.replace("_", " ")
        return frame

    @staticmethod
    def _build_rows_params(
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
