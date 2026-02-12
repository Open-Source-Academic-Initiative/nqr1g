"""
OpenSAI - SECOP Unified Query Microservice (I and II)
Migrated to FastAPI - Version 3.0.0
Date: February 2026
"""

import logging
import os
import re
import asyncio
from datetime import datetime
from typing import Optional, Tuple

from fastapi import FastAPI, Request, Query, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
import httpx
import pandas as pd

# --- CONFIGURATION ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
logger = logging.getLogger("OpenSAI")

app = FastAPI(title="OpenSAI - SECOP API", version="3.0.0")

# Security Headers Middleware
class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        response.headers['X-Content-Type-Options'] = 'nosniff'
        response.headers['X-Frame-Options'] = 'DENY'
        response.headers['Content-Security-Policy'] = (
            "default-src 'self'; "
            "script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net; "
            "style-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net https://cdnjs.cloudflare.com; "
            "font-src https://cdnjs.cloudflare.com; "
            "connect-src 'self' https://www.datos.gov.co;"
        )
        return response

app.add_middleware(SecurityHeadersMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Adjust in production
    allow_methods=["GET"],
    allow_headers=["*"],
)

templates = Jinja2Templates(directory="templates")

# Constants
SOCRATA_APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN", None)
TIMEOUT_SECONDS = 120 # Balanced timeout
MAX_CONCURRENT_REQUESTS = 5

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
# We use a global async client to benefit from connection pooling
async_client = httpx.AsyncClient(
    timeout=TIMEOUT_SECONDS,
    limits=httpx.Limits(max_keepalive_connections=10, max_connections=20),
    headers={
        "User-Agent": "OpenSAI-Bot/3.0 (FastAPI)",
        "Accept": "application/json",
        **({"X-App-Token": SOCRATA_APP_TOKEN} if SOCRATA_APP_TOKEN else {})
    }
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

async def query_soda(source_name: str, config: dict, contractor: str, year: int) -> pd.DataFrame:
    endpoint = f"https://www.datos.gov.co/resource/{config['id']}.json"
    col_map = config['cols']
    
    # Range filtering for performance (SARGable)
    start_date = f"{year}-01-01T00:00:00"
    end_date = f"{year}-12-31T23:59:59"
    
    # Security: Use escaped strings for the query
    safe_contractor = format_soql_string(contractor.upper())
    
    where_clause = (
        f"{col_map['fecha']} BETWEEN '{start_date}' AND '{end_date}' "
        f"AND contains(upper({col_map['contratista']}), '{safe_contractor}')"
    )
    
    params = {
        "$select": ",".join(col_map.values()),
        "$where": where_clause,
        "$limit": 1000,
        "$order": f"{col_map['fecha']} DESC"
    }

    try:
        resp = await async_client.get(endpoint, params=params)
        resp.raise_for_status()
        data = resp.json()
        
        if not data:
            return pd.DataFrame()
            
        df = pd.DataFrame(data)
        inv_map = {v: k for k, v in col_map.items()}
        df = df.rename(columns=inv_map)
        df['Origen'] = source_name.replace('_', ' ')
        return df
    except Exception as e:
        logger.error(f"Error querying {source_name}: {e}")
        return pd.DataFrame()

# --- ROUTES ---
@app.get("/", response_class=HTMLResponse)
async def index(
    request: Request,
    contratista: Optional[str] = Query(None),
    anio: Optional[int] = Query(datetime.now().year),
    page: int = Query(1, ge=1)
):
    current_year = datetime.now().year
    context = {
        "request": request,
        "c_val": contratista or "",
        "y_val": anio,
        "current_year": current_year,
        "table": None,
        "error": None,
        "no_results": False,
        "count": 0,
        "curr_page": page,
        "pages": 0
    }

    if not contratista:
        return templates.TemplateResponse("index.html", context)

    try:
        # Validation
        c_clean = clean_input(contratista)
        if len(c_clean) < 3:
            raise ValueError("Ingrese al menos 3 caracteres válidos.")
            
        if not (2000 <= anio <= current_year + 1):
            raise ValueError(f"El año debe estar entre 2000 y {current_year + 1}.")

        # Concurrent async requests
        tasks = [query_soda(name, conf, c_clean, anio) for name, conf in SOURCES.items()]
        results = await asyncio.gather(*tasks)
        
        dfs = [df for df in results if not df.empty]
        
        if not dfs:
            context["no_results"] = True
            return templates.TemplateResponse("index.html", context)

        final_df = pd.concat(dfs, ignore_index=True)
        
        # --- DATA PROCESSING (Maintain original logic) ---
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
            df_view = df_view.sort_values(by='Fecha', ascending=False)
            df_view['Fecha'] = df_view['Fecha'].str.split('T').str[0]

        df_view.insert(0, 'No.', range(1, len(df_view) + 1))

        # Pagination logic
        per_page = 50
        count = len(df_view)
        total_pages = (count + per_page - 1) // per_page
        safe_page = max(1, min(page, total_pages)) if total_pages > 0 else 1
        
        df_page = df_view.iloc[(safe_page-1)*per_page : safe_page*per_page].copy()

        # SECURITY: Escape HTML for all columns except the one we will add manually
        import html
        for col in df_page.columns:
            if df_page[col].dtype == object:
                df_page[col] = df_page[col].astype(str).apply(html.escape)

        # Now add the Link column safely
        if 'url' in final_df.columns:
            def extract_clean_url(val):
                if isinstance(val, dict): return val.get('url', '')
                return str(val) if val else ''
            
            # Get the URLs for the current page
            urls = final_df.iloc[df_page.index]['url'].apply(extract_clean_url)
            df_page['Enlace'] = urls.apply(
                lambda x: f'<a href="{html.escape(x)}" target="_blank" class="btn btn-sm btn-outline-primary">Ver</a>' if x else ''
            )

        table_html = df_page.to_html(
            classes='table table-hover table-striped align-middle mb-0 small', 
            index=False, 
            escape=False, # We already escaped the content manually
            border=0
        )
        
        context.update({
            "table": table_html,
            "count": count,
            "curr_page": safe_page,
            "pages": total_pages
        })
        
        return templates.TemplateResponse("index.html", context)

    except ValueError as e:
        context["error"] = str(e)
        return templates.TemplateResponse("index.html", context)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        context["error"] = "Error interno del servidor al procesar la solicitud."
        return templates.TemplateResponse("index.html", context)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5000)
