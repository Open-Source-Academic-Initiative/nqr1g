"""
OpenSAI - SECOP Unified Query Microservice (I and II)
Version: 2.4.1 (Production + Source Field)
Date: January 2026
"""

import logging
import os
import re
import concurrent.futures
from datetime import datetime
from functools import lru_cache

from flask import Flask, request, render_template
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
from werkzeug.middleware.proxy_fix import ProxyFix

# --- CONFIGURATION AND INITIALIZATION ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
logger = logging.getLogger("OpenSAI")

app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)

# Credentials and Constants
SOCRATA_APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN", None) 
TIMEOUT_SECONDS = 60
MAX_WORKERS = 2

# Official Datasets
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

# --- CONNECTION MANAGEMENT ---
def get_session():
    session = requests.Session()
    retry_strategy = Retry(
        total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504]
    )
    adapter = HTTPAdapter(pool_connections=10, pool_maxsize=10, max_retries=retry_strategy)
    session.mount("https://", adapter)
    
    headers = {"User-Agent": "OpenSAI-Bot/2.1", "Accept": "application/json"}
    if SOCRATA_APP_TOKEN:
        headers["X-App-Token"] = SOCRATA_APP_TOKEN
    
    session.headers.update(headers)
    return session

http_session = get_session()

# --- CONTEXT PROCESSOR ---
@app.context_processor
def inject_global_vars():
    return {'current_year': datetime.now().year}

# --- BUSINESS LOGIC ---
def validate_input(contractor: str, year: str) -> tuple[str, int]:
    if not contractor or len(contractor) < 3:
        raise ValueError("Ingrese al menos 3 caracteres.")
    clean_c = re.sub(r'[^a-zA-Z0-9\sñÑáéíóúÁÉÍÓÚ\.]', '', contractor).strip()
    try:
        y_int = int(year)
        if not (2000 <= y_int <= datetime.now().year + 1):
            raise ValueError("Año fuera de rango.")
    except ValueError:
        raise ValueError("Año inválido.")
    return clean_c, y_int

def query_source(source_name: str, config: dict, contractor: str, year: int) -> pd.DataFrame:
    endpoint = f"https://www.datos.gov.co/resource/{config['id']}.json"
    col_map = config['cols']
    
    select_clause = ",".join(col_map.values())
    where_clause = (
        f"contains(upper({col_map['contratista']}), '{contractor.upper()}') "
        f"AND date_extract_y({col_map['fecha']}) = {year}"
    )
    
    params = {
        "$select": select_clause,
        "$where": where_clause,
        "$limit": 1000, 
        "$order": f"{col_map['fecha']} DESC"
    }

    try:
        resp = http_session.get(endpoint, params=params, timeout=TIMEOUT_SECONDS)
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
        logger.error(f"Error {source_name}: {e}")
        return pd.DataFrame()

@lru_cache(maxsize=64)
def fetch_unified_data(contractor: str, year: int):
    futures = []
    results = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for name, conf in SOURCES.items():
            futures.append(executor.submit(query_source, name, conf, contractor, year))
        
        for future in concurrent.futures.as_completed(futures):
            try:
                res = future.result()
                if not res.empty:
                    results.append(res)
            except Exception as e:
                logger.error(e)

    if not results:
        return pd.DataFrame(), "No se pudo establecer conexión con los servicios de datos.gov.co. Intente más tarde."
        
    final_df = pd.concat(results, ignore_index=True)
    return final_df, None

# --- CONTROLLER ---
@app.route('/')
def index():
    raw_c = request.args.get('contratista', '').strip()
    raw_y = request.args.get('anio', str(datetime.now().year)).strip()
    
    if not raw_c:
        return render_template('index.html', c_val="", y_val=raw_y, table=None)

    try:
        c_clean, y_clean = validate_input(raw_c, raw_y)
        df, error = fetch_unified_data(c_clean, y_clean)
        
        if error:
             return render_template('index.html', error=error, c_val=raw_c, y_val=raw_y)
        
        if df.empty:
            return render_template('index.html', no_results=True, c_val=raw_c, y_val=raw_y)

        # --- PRESENTATION PROCESSING ---
        if 'valor' in df.columns:
            df['valor'] = pd.to_numeric(df['valor'], errors='coerce').fillna(0)
            df['Valor (COP)'] = df['valor'].apply(lambda x: f"${x:,.0f}".replace(",", "."))
            
        if 'url' in df.columns:
            def extract_clean_url(val):
                if isinstance(val, dict):
                    return val.get('url', '')
                return str(val) if val else ''

            df['url'] = df['url'].apply(extract_clean_url)
            df['Link'] = df['url'].apply(
                lambda x: f'<a href="{x}" target="_blank" class="btn btn-sm btn-outline-primary">Ver</a>' if x else ''
            )
            
        # --- EXACT DEFINITION OF COLUMNS TO DISPLAY ---
        # HERE WE ADD 'Origen' -> 'Fuente' AND 'id_contrato' -> 'ID Proceso'
        cols_display = {
            'Origen': 'Fuente',
            'id_contrato': 'ID Proceso',
            'entidad': 'Entidad',
            'objeto': 'Objeto',
            'Valor (COP)': 'Valor (COP)',
            'contratista': 'Contratista',
            'fecha': 'Fecha',
            'Link': 'Enlace'
        }
        
        valid_cols = [c for c in cols_display.keys() if c in df.columns]
        df_view = df[valid_cols].rename(columns=cols_display)
        
        if 'Fecha' in df_view.columns:
            df_view = df_view.sort_values(by='Fecha', ascending=False)
            df_view['Fecha'] = df_view['Fecha'].str.split('T').str[0]

        df_view.insert(0, 'No.', range(1, len(df_view) + 1))

        # --- PAGINATION ---
        try:
            page = int(request.args.get('page', 1))
        except ValueError:
            page = 1
            
        per_page = 50
        count = len(df_view)
        total_pages = (count + per_page - 1) // per_page
        page = max(1, min(page, total_pages)) if total_pages > 0 else 1
        
        df_page = df_view.iloc[(page-1)*per_page : page*per_page]

        # Delegate styling to CSS (no text-center class here)
        table_html = df_page.to_html(
            classes='table table-hover table-striped align-middle mb-0 small', 
            index=False, 
            escape=False,
            border=0
        )
        
        return render_template(
            'index.html', 
            table=table_html, 
            c_val=raw_c, 
            y_val=raw_y,
            count=count,
            curr_page=page,
            pages=total_pages
        )

    except ValueError as e:
        return render_template('index.html', error=str(e), c_val=raw_c, y_val=raw_y)

@app.after_request
def security_headers(response):
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

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)