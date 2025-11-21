import logging
import re
from datetime import datetime
from flask import Flask, request, render_template_string
from flasgger import Swagger
import requests
import pandas as pd
import numpy as np

# Configuración de Logging (ISO 27001 - Trazabilidad)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Configuración de Swagger
app.config['SWAGGER'] = {
    'title': 'Consulta Interactiva SECOP',
    'uiversion': 3,
    'description': 'Interfaz para consultar contratos públicos en datos.gov.co',
    'specs_route': '/apidocs/'
}
swagger = Swagger(app)

# URL del Endpoint de Datos Abiertos (Socrata)
SOCRATA_ENDPOINT = "https://www.datos.gov.co/resource/rpmr-utcd.json"

# Plantilla HTML Responsiva (Bootstrap 5 + Jinja2)
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Buscador de Contratos Públicos</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css">
    <style>
        body { background-color: #f8f9fa; }
        .header-banner { 
            background-color: #2574af; 
            color: white; 
            padding: 2rem 0; 
            margin-bottom: 2rem; 
            border-bottom: 4px solid #1a5c8e; 
        }
        .search-card { box-shadow: 0 4px 6px rgba(0,0,0,0.1); border: none; }
        .table-card { box-shadow: 0 4px 6px rgba(0,0,0,0.05); border: none; margin-top: 2rem; }
        .footer { margin-top: 3rem; color: #6c757d; font-size: 0.9rem; }
        .text-right { text-align: right; }
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
        <div class="card search-card">
            <div class="card-body">
                <form action="/" method="GET" class="row g-3 needs-validation">
                    <div class="col-md-6">
                        <label for="contratista" class="form-label fw-bold">Nombre Contratista</label>
                        <input type="text" class="form-control" id="contratista" name="contratista" 
                               placeholder="Ej. OpenSAI" value="{{ contractor_val }}" required>
                    </div>
                    <div class="col-md-4">
                        <label for="anio" class="form-label fw-bold">Año de Firma</label>
                        <input type="number" class="form-control" id="anio" name="anio" 
                               placeholder="Ej. 2025" value="{{ year_val }}" min="2000" max="2030" required>
                    </div>
                    <div class="col-md-2 d-flex align-items-end">
                        <button type="submit" class="btn btn-primary w-100 fw-bold" style="background-color: #2574af; border-color: #2574af;">
                            <i class="fas fa-search"></i> Consultar
                        </button>
                    </div>
                </form>
            </div>
        </div>

        {% if error_msg %}
        <div class="alert alert-danger mt-4" role="alert">
            <i class="fas fa-exclamation-triangle"></i> {{ error_msg }}
        </div>
        {% endif %}

        {% if table_html %}
        <div class="card table-card animate__animated animate__fadeIn">
            <div class="card-header bg-white fw-bold border-bottom">
                Resultados de la Búsqueda
                <span class="badge bg-secondary float-end">{{ count }} registros encontrados</span>
            </div>
            <div class="card-body p-0">
                <div class="table-responsive">
                    {{ table_html|safe }}
                </div>
            </div>
        </div>
        {% elif show_no_results %}
        <div class="alert alert-info mt-4" role="alert">
            <i class="fas fa-info-circle"></i> No se encontraron contratos.
        </div>
        {% endif %}

        <div class="footer text-center pb-4">
            <a href="https://opensai.org/" target="_blank">OpenSAI</a>
        </div>
    </div>
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
</body>
</html>
"""

def validate_input(contractor, year):
    if not contractor or not isinstance(contractor, str):
        raise ValueError("El nombre del contratista es requerido.")
    clean_contractor = re.sub(r'[^a-zA-Z0-9\s\.\-]', '', contractor).strip()
    if len(clean_contractor) < 3:
        raise ValueError("El nombre debe tener al menos 3 caracteres.")
    try:
        year_int = int(year)
        current_year = datetime.now().year
        if year_int < 2000 or year_int > current_year + 2:
            raise ValueError("Año fuera de rango válido.")
    except ValueError:
        raise ValueError("El año debe ser un número válido.")
    return clean_contractor, year_int

def format_cop_currency(value):
    try:
        if pd.isna(value) or value == '':
            return "$ 0"
        val_float = float(value)
        formatted = "{:,.0f}".format(val_float)
        formatted = formatted.replace(",", ".")
        return f"$ {formatted}"
    except (ValueError, TypeError):
        return "$ 0"

@app.route('/', methods=['GET'])
def index():
    raw_contractor = request.args.get('contratista', '')
    raw_year = request.args.get('anio', '')
    
    if not raw_contractor and not raw_year:
        return render_template_string(HTML_TEMPLATE, contractor_val="", year_val="", table_html=None, error_msg=None)

    try:
        contractor, year = validate_input(raw_contractor, raw_year)
        soql_query = f"contains(upper(nom_raz_social_contratista), '{contractor.upper()}') AND date_extract_y(fecha_de_firma_del_contrato) = {year}"
        params = {"$where": soql_query, "$limit": 50} 

        logger.info(f"Consultando: {contractor}, {year}")
        response = requests.get(SOCRATA_ENDPOINT, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        table_html = ""
        show_no_results = True
        count = 0
        
        if data:
            df = pd.DataFrame(data)
            count = len(df)
            cols_map = {
                'nombre_de_la_entidad': 'Entidad',
                'objeto_a_contratar': 'Objeto del Contrato',
                'valor_contrato': 'Valor (COP)',
                'nom_raz_social_contratista': 'Contratista',
                'fecha_de_firma_del_contrato': 'Fecha Firma',
                'url_contrato': 'Enlace'
            }
            available_cols = [c for c in cols_map.keys() if c in df.columns]
            df_display = df[available_cols].rename(columns=cols_map)
            
            if 'Valor (COP)' in df_display.columns:
                df_display['Valor (COP)'] = df_display['Valor (COP)'].apply(format_cop_currency)

            if 'Enlace' in df_display.columns:
                df_display['Enlace'] = df_display['Enlace'].apply(lambda x: f'<a href="{x}" target="_blank" class="btn btn-sm btn-outline-primary">Ver</a>' if x else '')

            if 'Fecha Firma' in df_display.columns:
                df_display['Fecha Firma'] = df_display['Fecha Firma'].astype(str).str.split('T').str[0]

            table_html = df_display.to_html(
                classes='table table-hover table-striped align-middle mb-0',
                index=False, escape=False, border=0, justify='left'
            )
            show_no_results = False

        return render_template_string(
            HTML_TEMPLATE, contractor_val=raw_contractor, year_val=raw_year, 
            table_html=table_html, show_no_results=show_no_results, error_msg=None, count=count
        )

    except ValueError as ve:
        return render_template_string(HTML_TEMPLATE, contractor_val=raw_contractor, year_val=raw_year, table_html=None, error_msg=str(ve))
    except Exception as e:
        logger.error(f"Error crítico: {e}")
        return render_template_string(HTML_TEMPLATE, contractor_val=raw_contractor, year_val=raw_year, table_html=None, error_msg="Error de comunicación.")

# Headers de Seguridad
@app.after_request
def add_security_headers(response):
    response.headers['X-Frame-Options'] = 'DENY'
    response.headers['X-Content-Type-Options'] = 'nosniff'
    return response

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)