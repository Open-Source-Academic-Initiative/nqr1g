import time
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

from .config import settings
from .middleware import configure_middleware
from .observability import configure_logging, logger, render_metrics
from .presentation import render_search_page
from .search_service import SearchExecution, SearchService
from .security import SearchThrottle, get_client_ip, is_local_metrics_request
from .socrata_client import SocrataClient


def create_app() -> FastAPI:
    configure_logging()
    socrata_client = SocrataClient()
    search_service = SearchService(socrata_client=socrata_client)
    search_throttle = SearchThrottle()

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        try:
            yield
        finally:
            await socrata_client.close()

    app = FastAPI(title="OpenSAI - SECOP API", version="3.0.0", lifespan=lifespan)
    app.state.socrata_client = socrata_client
    app.state.search_service = search_service
    app.state.search_throttle = search_throttle
    configure_middleware(app)
    app.mount("/static", StaticFiles(directory=settings.static_directory), name="static")

    @app.get("/healthz")
    async def healthz():
        return {"status": "ok", "service": "opensai-secops"}

    @app.get("/favicon.ico", include_in_schema=False)
    async def favicon() -> RedirectResponse:
        return RedirectResponse(url="/static/favicon.ico", status_code=307)

    @app.get("/healthz/upstream")
    async def healthz_upstream():
        deadline = time.monotonic() + settings.socrata.health_timeout_seconds + 0.5
        ok, reason = await socrata_client.check_health(deadline=deadline)
        status_code = 200 if ok else 503
        return JSONResponse(
            {"status": "ok" if ok else "degraded", "upstream": "datos.gov.co", "reason": reason},
            status_code=status_code,
        )

    @app.get("/metrics", response_class=PlainTextResponse, include_in_schema=False)
    async def metrics(request: Request) -> PlainTextResponse:
        if not is_local_metrics_request(request):
            logger.warning(
                "Blocked non-local metrics access client_ip=%s host=%s",
                request.client.host if request.client else "unknown",
                request.headers.get("Host", ""),
            )
            return PlainTextResponse("Forbidden", status_code=403)
        payload, content_type = render_metrics()
        return PlainTextResponse(payload, media_type=content_type)

    @app.get("/", response_class=HTMLResponse)
    async def index(
        request: Request,
        contratista: str | None = Query(None),
        anio: int | None = Query(None),
        page: int = Query(1, ge=1),
    ):
        current_year = datetime.now().year
        selected_year = anio if anio is not None else current_year
        if contratista:
            allowed, throttle_reason = await search_throttle.allow_request(request)
            if not allowed:
                logger.warning(
                    "throttle_blocked reason=%s ip=%s path=%s",
                    throttle_reason,
                    get_client_ip(request),
                    request.url.path,
                )
                execution = SearchExecution.initial(contratista, selected_year, page, current_year)
                execution.error = settings.throttle.error_message
                return render_search_page(
                    request,
                    execution,
                    status_code=429,
                    headers={"Retry-After": str(settings.throttle.window_seconds)},
                )

        execution = await search_service.execute_search(contratista, selected_year, page, current_year)
        return render_search_page(request, execution)

    return app


app = create_app()
