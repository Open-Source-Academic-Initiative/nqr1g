import logging
import re
import secrets
from datetime import datetime
from functools import lru_cache
from flask import Flask, request, render_template_string
from flasgger import Swagger
import requests
import pandas as pd
import numpy as np

# --- CONFIGURACIÓN ESTRUCTURAL ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("nqr1g")

app = Flask(__name__)

# Configuración Swagger (OpenAPI 2.0)
app.config['SWAGGER'] = {
    'title': 'Consulta SECOP - OpenSAI',
    'uiversion': 3,
    'description': 'Microservicio optimizado para consulta de contratación pública.',
    'specs_route': '/apidocs/'
}
swagger = Swagger(app)

SOCRATA_ENDPOINT = "https://www.datos.gov.co/resource/rpmr-utcd.json"
MAX_RECORDS_SAFETY_CAP = 5000  # Límite duro para prevenir desbordamiento de memoria

# --- CAPA DE PRESENTACIÓN (UI) ---
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Buscador de Contratos - OpenSAI</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css">
    <style>
        body { background-color: #f8f9fa; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; }
        .header-banner { background-color: #2574af; color: white; padding: 2rem 0; margin-bottom: 2rem; border-bottom: 4px solid #1a5c8e; }
        .btn-primary { background-color: #2574af; border-color: #2574af; }
        .btn-primary:hover { background-color: #1a5c8e; }
    </style>
</head>
<body>
    <div class="header-banner text-center">
        <div class="container">
            <h1><i class="fas fa-file-contract"></i> Consulta de Contratación SECOP</h1>
            <p class="lead">Búsqueda en tiempo real sobre datos.gov.co</p>
        </div>
    </div>
    <div class="container">
        <div class="card shadow-sm border-0 mb-4">
            <div class="card-body">
                <form action="/" method="GET" class="row g-3">
                    <div class="col-md-6">
                        <label class="form-label fw-bold">Contratista</label>
                        <input type="text" class="form-control" name="contratista" value="{{ c_val }}" placeholder="Ej. OpenSAI" required>
                    </div>
                    <div class="col-md-4">
                        <label class="form-label fw-bold">Año</label>
                        <input type="number" class="form-control" name="anio" value="{{ y_val }}" min="2000" max="2030" required>
                    </div>
                    <div class="col-md-2 d-flex align-items-end">
                        <button type="submit" class="btn btn-primary w-100 fw-bold"><i class="fas fa-search"></i> Buscar</button>
                    </div>
                </form>
            </div>
        </div>

        {% if error %}
        <div class="alert alert-danger"><i class="fas fa-exclamation-triangle"></i> {{ error }}</div>
        {% endif %}

        {% if table %}
        <div class="card shadow-sm border-0 animate__animated animate__fadeIn">
            <div class="card-header bg-white fw-bold">
                Resultados <span class="badge bg-secondary float-end">{{ count }} encontrados</span>
            </div>
            <div class="card-body p-0 table-responsive">
                {{ table|safe }}
            </div>
            {% if pages > 1 %}
            <div class="card-footer bg-white py-3">
                <nav>
                    <ul class="pagination justify-content-center mb-0">
                        <li class="page-item {% if curr_page <= 1 %}disabled{% endif %}">
                            <a class="page-link" href="/?contratista={{ c_val }}&anio={{ y_val }}&page={{ curr_page-1 }}">Anterior</a>
                        </li>
                        <li class="page-item disabled"><span class="page-link">{{ curr_page }} / {{ pages }}</span></li>
                        <li class="page-item {% if curr_page >= pages %}disabled{% endif %}">
                            <a class="page-link" href="/?contratista={{ c_val }}&anio={{ y_val }}&page={{ curr_page+1 }}">Siguiente</a>
                        </li>
                    </ul>
                </nav>
            </div>
            {% endif %}
        </div>
        {% elif no_results %}
        <div class="alert alert-info"><i class="fas fa-info-circle"></i> No se encontraron resultados.</div>
        {% endif %}
        
        <div class="text-center mt-4 text-muted small">
            <a href="https://opensai.org/" target="_blank" rel="noopener noreferrer" class="text-muted text-decoration-none">&copy; 2025 OpenSAI.</a>
        </div>
    </div>
</body>
</html>
"""

# --- CAPA DE LÓGICA DE NEGOCIO (Validación y Procesamiento) ---

def validate_request(contractor, year):
    """Sanitización estricta (OWASP Input Validation)."""
    if not contractor or not isinstance(contractor, str):
        raise ValueError("Nombre inválido.")
    # Permitir solo alfanuméricos seguros
    clean_c = re.sub(r'[^a-zA-Z0-9\s\.\-]', '', contractor).strip()
    if len(clean_c) < 3:
        raise ValueError("Mínimo 3 caracteres requeridos.")
    
    try:
        y_int = int(year)
        if not (2000 <= y_int <= datetime.now().year + 2):
            raise ValueError("Año fuera de rango.")
    except ValueError:
        raise ValueError("Año inválido.")
    return clean_c, y_int

def safe_currency_fmt(val):
    """Formateo seguro de moneda COP."""
    try:
        return f"$ {float(val):,.0f}".replace(",", ".") if val else "$ 0"
    except:
        return "$ 0"

# --- CAPA DE DATOS (Caché Optimizado) ---
# Usamos lru_cache para gestión automática de memoria (LRU Eviction)
# maxsize=64 previene DoS por consumo de memoria.
@lru_cache(maxsize=64)
def fetch_data_cached(contractor, year):
    """
    Recupera datos de Socrata con gestión de caché interna.
    Retorna: (DataFrame, ErrorMessage)
    """
    soql = f"contains(upper(nom_raz_social_contratista), '{contractor.upper()}') AND date_extract_y(fecha_de_firma_del_contrato) = {year}"
    # Solicitamos hasta el tope de seguridad en una sola petición (más eficiente que paginar HTTP loop)
    params = {"$where": soql, "$limit": MAX_RECORDS_SAFETY_CAP}
    
    try:
        logger.info(f"API Request: {contractor} ({year})")
        resp = requests.get(SOCRATA_ENDPOINT, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if not data:
            return pd.DataFrame(), None
        return pd.DataFrame(data), None
    except Exception as e:
        logger.error(f"Fallo en API: {e}")
        return None, "Error de comunicación con datos.gov.co"

# --- CONTROLADOR PRINCIPAL ---

@app.route('/', methods=['GET'])
def index():
    raw_c = request.args.get('contratista', '').strip()
    raw_y = request.args.get('anio', '').strip()
    
    # Renderizado inicial
    if not raw_c and not raw_y:
        return render_template_string(HTML_TEMPLATE, c_val="", y_val="", table=None, error=None)

    try:
        c_clean, y_clean = validate_request(raw_c, raw_y)
        
        # Obtención de datos (Cacheada)
        df, error_msg = fetch_data_cached(c_clean, y_clean)
        
        if error_msg:
            return render_template_string(HTML_TEMPLATE, c_val=raw_c, y_val=raw_y, error=error_msg)
            
        if df.empty:
            return render_template_string(HTML_TEMPLATE, c_val=raw_c, y_val=raw_y, no_results=True)

        # Procesamiento Vectorizado (Pandas Optimizado)
        # Seleccionamos y renombramos solo lo necesario
        cols = {
            'nombre_de_la_entidad': 'Entidad',
            'objeto_a_contratar': 'Objeto',
            'valor_contrato': 'Valor (COP)',
            'nom_raz_social_contratista': 'Contratista',
            'fecha_de_firma_del_contrato': 'Fecha',
            'url_contrato': 'Link'
        }
        # Intersección de columnas existentes
        df = df[[c for c in cols.keys() if c in df.columns]].rename(columns=cols)
        
        # Transformaciones rápidas
        if 'Valor (COP)' in df:
            df['Valor (COP)'] = df['Valor (COP)'].apply(safe_currency_fmt)
        if 'Fecha' in df:
            df['Fecha'] = df['Fecha'].str.split('T').str[0]
        if 'Link' in df:
            # FIX DE SEGURIDAD: rel="noopener noreferrer"
            df['Link'] = df['Link'].apply(lambda x: f'<a href="{x}" target="_blank" rel="noopener noreferrer" class="btn btn-sm btn-outline-primary">Ver</a>' if x else '')
        
        # Numeración (1-based)
        df.insert(0, 'No.', range(1, len(df) + 1))

        # Paginación UI (Slicing en memoria)
        page = int(request.args.get('page', 1))
        per_page = 50
        total_pages = (len(df) + per_page - 1) // per_page
        page = max(1, min(page, total_pages))
        
        # Renderizado de Tabla
        table_html = df.iloc[(page-1)*per_page : page*per_page].to_html(
            classes='table table-hover table-striped align-middle mb-0 small',
            index=False, escape=False, border=0
        )

        return render_template_string(
            HTML_TEMPLATE, c_val=raw_c, y_val=raw_y, table=table_html,
            count=len(df), curr_page=page, pages=total_pages
        )

    except ValueError as e:
        return render_template_string(HTML_TEMPLATE, c_val=raw_c, y_val=raw_y, error=str(e))
    except Exception as e:
        logger.exception("Error no controlado")
        return render_template_string(HTML_TEMPLATE, c_val=raw_c, y_val=raw_y, error="Error interno del sistema.")

# --- SEGURIDAD HTTP (Middleware) ---
@app.after_request
def apply_security_headers(response):
    response.headers['X-Frame-Options'] = 'DENY'
    response.headers['X-Content-Type-Options'] = 'nosniff'
    response.headers['Strict-Transport-Security'] = 'max-age=63072000; includeSubDomains'
    # CSP Básico: Permitir scripts de Bootstrap/CDN y estilos inline (necesario para el template actual)
    csp = "default-src 'self'; script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net; style-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net https://cdnjs.cloudflare.com; font-src https://cdnjs.cloudflare.com;"
    response.headers['Content-Security-Policy'] = csp
    return response

if __name__ == '__main__':
    # Ejecución en modo seguro
    app.run(host='0.0.0.0', port=5000)