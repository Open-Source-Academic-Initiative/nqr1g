import logging
import os
from dataclasses import dataclass
from pathlib import Path


logger = logging.getLogger("OpenSAI.config")


def _is_truthy(value: str) -> bool:
    return value in {"1", "true", "yes", "on"}


def _is_falsy(value: str) -> bool:
    return value in {"0", "false", "no", "off"}


def _get_int_env(name: str, default: int, minimum: int) -> int:
    return max(minimum, int(os.getenv(name, str(default))))


def _get_float_env(name: str, default: float, minimum: float) -> float:
    return max(minimum, float(os.getenv(name, str(default))))


def _get_capped_float_env(name: str, default: float, minimum: float, maximum: float) -> float:
    return min(maximum, _get_float_env(name, default, minimum))


def parse_cors_origins(raw: str) -> list[str]:
    return [origin.strip() for origin in raw.split(",") if origin.strip()]


@dataclass(frozen=True)
class SourceConfig:
    dataset_id: str
    cols: dict[str, str]


@dataclass(frozen=True)
class SearchSettings:
    per_page: int
    max_query_window: int
    search_mode: str
    use_unaccent: bool
    request_budget_seconds: float
    count_phase_budget_seconds: float
    rows_phase_budget_seconds: float
    response_reserve_seconds: float


@dataclass(frozen=True)
class SocrataSettings:
    app_token: str | None
    max_concurrent_requests: int
    max_retries: int
    retry_base_seconds: float
    max_retry_delay_seconds: float
    request_max_wait_seconds: float
    connect_timeout_seconds: float
    read_timeout_seconds: float
    write_timeout_seconds: float
    pool_timeout_seconds: float
    health_timeout_seconds: float
    health_cache_seconds: float
    retryable_status_codes: set[int]


@dataclass(frozen=True)
class ThrottleSettings:
    window_seconds: int
    global_requests: int
    per_ip_requests: int
    max_tracked_ip_buckets: int
    error_message: str


@dataclass(frozen=True)
class AppSettings:
    app_env: str
    strict_security_mode: bool
    cors_allow_origins: list[str]
    public_base_url_raw: str
    templates_directory: str
    static_directory: str
    timeout_cap_seconds: float
    search: SearchSettings
    socrata: SocrataSettings
    throttle: ThrottleSettings


UPSTREAM_FAILURE_MESSAGE = (
    "El servicio de https://www.datos.gov.co/ ha fallado o no responde en este momento. "
    "Intente nuevamente más tarde."
)

THROTTLE_ERROR_MESSAGE = (
    "Se alcanzó temporalmente el límite de consultas. "
    "Por favor intente nuevamente en unos segundos."
)

DISPLAY_COLUMNS = {
    "Origen": "Fuente",
    "id_contrato": "ID Proceso",
    "entidad": "Entidad",
    "objeto": "Objeto",
    "Valor (COP)": "Valor (COP)",
    "contratista": "Contratista",
    "fecha": "Fecha",
}

SOURCES = {
    "SECOP_I": SourceConfig(
        dataset_id="rpmr-utcd",
        cols={
            "id_contrato": "numero_del_contrato",
            "entidad": "nombre_de_la_entidad",
            "objeto": "objeto_a_contratar",
            "valor": "valor_contrato",
            "contratista": "nom_raz_social_contratista",
            "fecha": "fecha_de_firma_del_contrato",
            "url": "url_contrato",
        },
    ),
    "SECOP_II": SourceConfig(
        dataset_id="jbjy-vk9h",
        cols={
            "id_contrato": "referencia_del_contrato",
            "entidad": "nombre_entidad",
            "objeto": "objeto_del_contrato",
            "valor": "valor_del_contrato",
            "contratista": "proveedor_adjudicado",
            "fecha": "fecha_de_firma",
            "url": "urlproceso",
        },
    ),
}


def _resolve_strict_security_mode(app_env: str) -> bool:
    security_strict_mode_raw = os.getenv("SECURITY_STRICT_MODE", "auto").strip().lower()
    if _is_truthy(security_strict_mode_raw):
        return True
    if _is_falsy(security_strict_mode_raw):
        return False
    return app_env in {"production", "prod"}


def _resolve_cors_allow_origins(strict_security_mode: bool) -> list[str]:
    cors_allow_origins_raw = os.getenv("CORS_ALLOW_ORIGINS", "*")
    if not strict_security_mode:
        return parse_cors_origins(cors_allow_origins_raw)

    strict_cors_raw = os.getenv("CORS_ALLOW_ORIGINS_STRICT", cors_allow_origins_raw)
    cors_allow_origins = [origin for origin in parse_cors_origins(strict_cors_raw) if origin != "*"]
    if not cors_allow_origins:
        logger.warning("Strict security mode enabled but no explicit CORS origins configured.")
    return cors_allow_origins


def _resolve_search_mode() -> str:
    search_mode = os.getenv("SOCRATA_SEARCH_MODE", "exact_or_composed").strip().lower()
    if search_mode not in {"contains", "starts_with", "exact_or_composed"}:
        logger.warning("Invalid SOCRATA_SEARCH_MODE=%s, using exact_or_composed", search_mode)
        return "exact_or_composed"
    return search_mode


def _load_search_settings(timeout_cap_seconds: float) -> SearchSettings:
    per_page = _get_int_env("PER_PAGE", 50, 1)
    return SearchSettings(
        per_page=per_page,
        max_query_window=max(per_page, int(os.getenv("MAX_QUERY_WINDOW", "5000"))),
        search_mode=_resolve_search_mode(),
        use_unaccent=os.getenv("SOCRATA_USE_UNACCENT", "0").strip().lower() in {"1", "true", "yes"},
        request_budget_seconds=_get_capped_float_env(
            "REQUEST_BUDGET_SECONDS",
            120.0,
            10.0,
            timeout_cap_seconds,
        ),
        count_phase_budget_seconds=_get_capped_float_env(
            "COUNT_PHASE_BUDGET_SECONDS",
            12.0,
            1.0,
            timeout_cap_seconds,
        ),
        rows_phase_budget_seconds=_get_capped_float_env(
            "ROWS_PHASE_BUDGET_SECONDS",
            20.0,
            1.0,
            timeout_cap_seconds,
        ),
        response_reserve_seconds=_get_capped_float_env(
            "RESPONSE_RESERVE_SECONDS",
            2.0,
            0.5,
            timeout_cap_seconds,
        ),
    )


def _load_socrata_settings(timeout_cap_seconds: float) -> SocrataSettings:
    return SocrataSettings(
        app_token=os.getenv("SOCRATA_APP_TOKEN", None),
        max_concurrent_requests=_get_int_env("MAX_CONCURRENT_REQUESTS", 5, 1),
        max_retries=_get_int_env("SOCRATA_MAX_RETRIES", 0, 0),
        retry_base_seconds=_get_float_env("SOCRATA_RETRY_BASE_SECONDS", 0.4, 0.1),
        max_retry_delay_seconds=_get_float_env("SOCRATA_MAX_RETRY_DELAY_SECONDS", 1.2, 0.2),
        request_max_wait_seconds=_get_capped_float_env(
            "SOCRATA_REQUEST_MAX_WAIT_SECONDS",
            120.0,
            1.0,
            timeout_cap_seconds,
        ),
        connect_timeout_seconds=_get_capped_float_env(
            "SOCRATA_CONNECT_TIMEOUT_SECONDS",
            5.0,
            0.2,
            timeout_cap_seconds,
        ),
        read_timeout_seconds=_get_capped_float_env(
            "SOCRATA_READ_TIMEOUT_SECONDS",
            120.0,
            0.2,
            timeout_cap_seconds,
        ),
        write_timeout_seconds=_get_capped_float_env(
            "SOCRATA_WRITE_TIMEOUT_SECONDS",
            10.0,
            0.2,
            timeout_cap_seconds,
        ),
        pool_timeout_seconds=_get_capped_float_env(
            "SOCRATA_POOL_TIMEOUT_SECONDS",
            5.0,
            0.2,
            timeout_cap_seconds,
        ),
        health_timeout_seconds=_get_capped_float_env(
            "SOCRATA_HEALTH_TIMEOUT_SECONDS",
            5.0,
            0.2,
            timeout_cap_seconds,
        ),
        health_cache_seconds=_get_float_env("SOCRATA_HEALTH_CACHE_SECONDS", 30.0, 1.0),
        retryable_status_codes={202, 429, 500, 502, 503, 504},
    )


def _load_throttle_settings() -> ThrottleSettings:
    return ThrottleSettings(
        window_seconds=_get_int_env("THROTTLE_WINDOW_SECONDS", 60, 1),
        global_requests=_get_int_env("THROTTLE_GLOBAL_REQUESTS", 240, 1),
        per_ip_requests=_get_int_env("THROTTLE_PER_IP_REQUESTS", 60, 1),
        max_tracked_ip_buckets=_get_int_env("MAX_TRACKED_IP_BUCKETS", 5000, 100),
        error_message=THROTTLE_ERROR_MESSAGE,
    )


def load_settings() -> AppSettings:
    timeout_cap_seconds = 120.0
    app_env = os.getenv("APP_ENV", "development").strip().lower()
    templates_directory = str(Path(__file__).resolve().parent.parent / "templates")
    strict_security_mode = _resolve_strict_security_mode(app_env)

    return AppSettings(
        app_env=app_env,
        strict_security_mode=strict_security_mode,
        cors_allow_origins=_resolve_cors_allow_origins(strict_security_mode),
        public_base_url_raw=os.getenv("PUBLIC_BASE_URL", "").strip(),
        templates_directory=templates_directory,
        static_directory=str(Path(__file__).resolve().parent.parent / "static"),
        timeout_cap_seconds=timeout_cap_seconds,
        search=_load_search_settings(timeout_cap_seconds),
        socrata=_load_socrata_settings(timeout_cap_seconds),
        throttle=_load_throttle_settings(),
    )


settings = load_settings()
