import asyncio
import re
import time
import unicodedata
from dataclasses import dataclass, field
from typing import cast

import pandas as pd

from .config import SOURCES, settings, UPSTREAM_FAILURE_MESSAGE
from .observability import logger
from .socrata_client import RequestBudgetExceeded, SocrataClient


SourceTotals = dict[str, int]


@dataclass(frozen=True)
class PageWindow:
    total_pages: int
    safe_page: int
    rows_limit: int


@dataclass(frozen=True)
class CountPhaseResult:
    source_totals: SourceTotals
    successful_sources: set[str]
    total_count: int


@dataclass(frozen=True)
class RowPhaseResult:
    frames: list[pd.DataFrame]
    available_source_totals: SourceTotals


@dataclass
class SearchExecution:
    contractor: str | None
    year: int
    requested_page: int
    current_year: int
    warnings: list[str] = field(default_factory=list)
    error: str | None = None
    no_results: bool = False
    count: int = 0
    current_page: int = 1
    pages: int = 0
    limited_results: bool = False
    upstream_outage: bool = False
    request_budget_seconds: int = field(default_factory=lambda: int(settings.search.request_budget_seconds))
    final_df: pd.DataFrame | None = None

    @classmethod
    def initial(
        cls,
        contractor: str | None,
        year: int,
        requested_page: int,
        current_year: int,
    ) -> "SearchExecution":
        return cls(
            contractor=contractor,
            year=year,
            requested_page=requested_page,
            current_year=current_year,
            current_page=requested_page,
        )

def clean_input(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"[^a-zA-Z0-9\sñÑáéíóúÁÉÍÓÚ\.]", "", text).strip()


def format_soql_string(text: str) -> str:
    return text.replace("'", "''")


def remove_accents(text: str) -> str:
    normalized = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def build_contractor_search_expression(col_map: dict[str, str], contractor: str) -> str:
    if settings.search.use_unaccent:
        contractor_term = remove_accents(contractor).upper()
        field_expr = f"upper(unaccent({col_map['contratista']}))"
    else:
        contractor_term = contractor.upper()
        field_expr = f"upper({col_map['contratista']})"

    safe_contractor = format_soql_string(contractor_term)
    if settings.search.search_mode == "starts_with":
        return f"starts_with({field_expr}, '{safe_contractor}')"
    if settings.search.search_mode == "contains":
        return f"contains({field_expr}, '{safe_contractor}')"
    return (
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


def build_where_clause(col_map: dict[str, str], contractor: str, year: int) -> str:
    start_date = f"{year}-01-01T00:00:00"
    end_date = f"{year}-12-31T23:59:59"
    search_expr = build_contractor_search_expression(col_map, contractor)
    return (
        f"{col_map['fecha']} BETWEEN '{start_date}' AND '{end_date}' "
        f"AND ({search_expr})"
    )


def validate_search_inputs(contractor: str, year: int, current_year: int) -> str:
    cleaned_contractor = clean_input(contractor)
    if len(cleaned_contractor) < 3:
        raise ValueError("Ingrese al menos 3 caracteres válidos.")
    if not (2000 <= year <= current_year + 1):
        raise ValueError(f"El año debe estar entre 2000 y {current_year + 1}.")
    return cleaned_contractor


def calculate_page_window(total_count: int, page: int) -> PageWindow:
    reachable_count = min(total_count, settings.search.max_query_window)
    total_pages = (reachable_count + settings.search.per_page - 1) // settings.search.per_page if reachable_count > 0 else 0
    safe_page = max(1, min(page, total_pages)) if total_pages > 0 else 1
    rows_limit = min(safe_page * settings.search.per_page, settings.search.max_query_window)
    return PageWindow(total_pages=total_pages, safe_page=safe_page, rows_limit=rows_limit)


def merge_and_sort_result_frames(frames: list[pd.DataFrame]) -> pd.DataFrame:
    final_df = pd.concat(frames, ignore_index=True)
    sort_fields = [column for column in ["fecha", "row_id"] if column in final_df.columns]
    if sort_fields:
        final_df = final_df.sort_values(by=sort_fields, ascending=[False] * len(sort_fields))
    return final_df


def build_phase_deadline(total_deadline: float, phase_budget_seconds: float) -> float:
    now = time.monotonic()
    reserved_deadline = max(now + 0.2, total_deadline - settings.search.response_reserve_seconds)
    return min(reserved_deadline, now + phase_budget_seconds)


class SearchService:
    def __init__(self, socrata_client: SocrataClient) -> None:
        self._socrata_client = socrata_client

    async def execute_search(
        self,
        contractor: str | None,
        year: int,
        page: int,
        current_year: int,
    ) -> SearchExecution:
        execution = SearchExecution.initial(contractor, year, page, current_year)
        warnings = execution.warnings

        if not contractor:
            return execution

        try:
            cleaned_contractor = validate_search_inputs(contractor, year, current_year)
            request_deadline = time.monotonic() + settings.search.request_budget_seconds
            count_deadline = build_phase_deadline(request_deadline, settings.search.count_phase_budget_seconds)
            count_result = await self.collect_source_counts(cleaned_contractor, year, count_deadline, warnings)

            if not count_result.successful_sources:
                execution.upstream_outage = True
                execution.error = UPSTREAM_FAILURE_MESSAGE
                return execution
            if count_result.total_count == 0:
                execution.no_results = True
                return execution

            initial_page_window = calculate_page_window(count_result.total_count, page)
            rows_deadline = build_phase_deadline(request_deadline, settings.search.rows_phase_budget_seconds)
            row_result = await self.collect_source_rows(
                cleaned_contractor,
                year,
                initial_page_window.rows_limit,
                count_result.source_totals,
                count_result.successful_sources,
                rows_deadline,
                warnings,
            )

            if not row_result.frames:
                execution.upstream_outage = True
                execution.error = UPSTREAM_FAILURE_MESSAGE
                return execution

            available_total_count = sum(row_result.available_source_totals.values())
            if available_total_count <= 0:
                execution.upstream_outage = True
                execution.error = UPSTREAM_FAILURE_MESSAGE
                return execution

            if available_total_count > settings.search.max_query_window:
                execution.limited_results = True
                warnings.append(
                    f"Por rendimiento, la navegación está limitada a los primeros {settings.search.max_query_window} resultados."
                )

            final_page_window = calculate_page_window(available_total_count, page)
            execution.final_df = merge_and_sort_result_frames(row_result.frames)
            execution.count = available_total_count
            execution.current_page = final_page_window.safe_page
            execution.pages = final_page_window.total_pages
            return execution

        except ValueError as exc:
            execution.error = str(exc)
            return execution
        except RequestBudgetExceeded:
            execution.upstream_outage = True
            execution.error = UPSTREAM_FAILURE_MESSAGE
            return execution
        except Exception:
            logger.exception("Unexpected error while processing search")
            execution.error = "Error interno del servidor al procesar la solicitud."
            return execution

    async def collect_source_counts(
        self,
        contractor: str,
        year: int,
        deadline: float,
        warnings: list[str],
    ) -> CountPhaseResult:
        count_tasks = [
            self._socrata_client.query_source_count(
                source_name,
                source_config,
                build_where_clause(source_config.cols, contractor, year),
                deadline=deadline,
            )
            for source_name, source_config in SOURCES.items()
        ]
        count_results = await asyncio.gather(*count_tasks, return_exceptions=True)

        source_totals: SourceTotals = {}
        successful_sources: set[str] = set()
        for (source_name, _), result in zip(SOURCES.items(), count_results, strict=False):
            if isinstance(result, BaseException):
                warnings.append(f"No se pudo consultar {source_name}.")
                logger.error("Error counting source=%s: %s", source_name, result)
                source_totals[source_name] = 0
                continue
            source_totals[source_name] = int(cast(int, result))
            successful_sources.add(source_name)

        total_count = sum(source_totals[source_name] for source_name in successful_sources)
        return CountPhaseResult(
            source_totals=source_totals,
            successful_sources=successful_sources,
            total_count=total_count,
        )

    async def collect_source_rows(
        self,
        contractor: str,
        year: int,
        rows_limit: int,
        source_totals: SourceTotals,
        successful_sources: set[str],
        deadline: float,
        warnings: list[str],
    ) -> RowPhaseResult:
        source_names = [
            name
            for name, _ in SOURCES.items()
            if name in successful_sources and source_totals.get(name, 0) > 0
        ]
        row_tasks = [
            self._socrata_client.query_source_rows(
                source_name,
                source_config,
                build_where_clause(source_config.cols, contractor, year),
                min(rows_limit, source_totals[source_name]),
                deadline=deadline,
            )
            for source_name, source_config in SOURCES.items()
            if source_name in successful_sources and source_totals.get(source_name, 0) > 0
        ]
        row_results = await asyncio.gather(*row_tasks, return_exceptions=True)

        frames: list[pd.DataFrame] = []
        available_source_totals: SourceTotals = {}
        for source_name, result in zip(source_names, row_results, strict=False):
            if isinstance(result, BaseException):
                warnings.append(f"No se pudieron recuperar filas de {source_name}.")
                logger.error("Error loading rows for source=%s: %s", source_name, result)
                continue
            frame = cast(pd.DataFrame, result)
            if frame.empty:
                warnings.append(f"{source_name} devolvió conteo positivo pero no entregó filas utilizables.")
                logger.warning("Source=%s returned empty rows after positive count", source_name)
                continue
            available_source_totals[source_name] = source_totals[source_name]
            frames.append(frame)

        return RowPhaseResult(frames=frames, available_source_totals=available_source_totals)
