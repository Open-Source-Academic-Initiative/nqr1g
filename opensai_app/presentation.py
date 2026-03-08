import html
from typing import Any, TypedDict

import pandas as pd
from fastapi import Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from .config import DISPLAY_COLUMNS, settings
from .search_service import SearchExecution
from .security import resolve_public_base_url



class IndexTemplateContext(TypedDict):
    request: Request
    csp_nonce: str
    public_base_url: str
    c_val: str
    y_val: int
    current_year: int
    table: str | None
    error: str | None
    no_results: bool
    count: int
    curr_page: int
    pages: int
    warnings: list[str]
    limited_results: bool
    upstream_outage: bool
    request_budget_seconds: int


templates = Jinja2Templates(directory=settings.templates_directory)


def build_index_context(request: Request, execution: SearchExecution) -> IndexTemplateContext:
    table_html = None
    if execution.final_df is not None:
        table_html = build_results_table(execution.final_df, execution.current_page)

    return {
        "request": request,
        "csp_nonce": getattr(request.state, "csp_nonce", ""),
        "public_base_url": resolve_public_base_url(request),
        "c_val": execution.contractor or "",
        "y_val": execution.year,
        "current_year": execution.current_year,
        "table": table_html,
        "error": execution.error,
        "no_results": execution.no_results,
        "count": execution.count,
        "curr_page": execution.current_page,
        "pages": execution.pages,
        "warnings": execution.warnings,
        "limited_results": execution.limited_results,
        "upstream_outage": execution.upstream_outage,
        "request_budget_seconds": execution.request_budget_seconds,
    }


def render_search_page(
    request: Request,
    execution: SearchExecution,
    status_code: int = 200,
    headers: dict[str, str] | None = None,
) -> HTMLResponse:
    response = templates.TemplateResponse(
        "index.html",
        build_index_context(request, execution),
        status_code=status_code,
    )
    for header_name, header_value in (headers or {}).items():
        response.headers[header_name] = header_value
    return response


def format_currency_value(value: float) -> str:
    return f"${value:,.0f}".replace(",", ".")


def build_results_view(final_df: pd.DataFrame) -> pd.DataFrame:
    df_view = final_df.copy()
    if "valor" in df_view.columns:
        df_view["valor"] = pd.to_numeric(df_view["valor"], errors="coerce").fillna(0)
        df_view["Valor (COP)"] = df_view["valor"].apply(format_currency_value)

    valid_columns = [column for column in DISPLAY_COLUMNS if column in df_view.columns]
    df_view = df_view[valid_columns].rename(columns=DISPLAY_COLUMNS)
    if "Fecha" in df_view.columns:
        df_view["Fecha"] = df_view["Fecha"].astype(str).str.split("T").str[0]
    return df_view


def build_page_dataframe(df_view: pd.DataFrame, safe_page: int) -> pd.DataFrame:
    start_idx = (safe_page - 1) * settings.search.per_page
    end_idx = safe_page * settings.search.per_page
    df_page = df_view.iloc[start_idx:end_idx].copy()
    df_page.insert(0, "No.", range(start_idx + 1, start_idx + 1 + len(df_page)))
    return df_page


def escape_object_columns(df_page: pd.DataFrame) -> None:
    for column in df_page.columns:
        if df_page[column].dtype == object:
            df_page[column] = df_page[column].astype(str).apply(html.escape)


def extract_clean_url(value: Any) -> str:
    if pd.isna(value):
        return ""
    if isinstance(value, dict):
        nested_url = value.get("url", "")
        if pd.isna(nested_url):
            return ""
        return str(nested_url).strip()

    normalized = str(value).strip() if value else ""
    if normalized.lower() in {"nan", "none"}:
        return ""
    return normalized


def add_link_column(df_page: pd.DataFrame, final_df: pd.DataFrame) -> None:
    if "url" not in final_df.columns:
        return

    urls = final_df.loc[df_page.index, "url"].apply(extract_clean_url)
    df_page["Enlace"] = urls.apply(
        lambda url: (
            f'<a href="{html.escape(url)}" target="_blank" rel="noopener noreferrer" '
            f'class="btn btn-sm btn-outline-primary">Ver</a>'
        ) if url else ""
    ).to_numpy()


def build_results_table(final_df: pd.DataFrame, safe_page: int) -> str:
    df_view = build_results_view(final_df)
    df_page = build_page_dataframe(df_view, safe_page)
    escape_object_columns(df_page)
    add_link_column(df_page, final_df)
    return df_page.to_html(
        classes="table table-hover table-striped align-middle mb-0 small",
        index=False,
        escape=False,
        border=0,
    )
