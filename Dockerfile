# Base sólida, ligera y mantenida (Python 3.12 / Debian 12)
FROM python:3.12-slim-bookworm

# Variables para optimizar Python en Docker
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Instalar dependencias del sistema (mínimas necesarias)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Instalar dependencias de Python
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# --- COPIA DEL CÓDIGO ---
# Copia nqr1g.py Y la carpeta templates/
COPY --chown=1000:1000 . .

# Seguridad: Ejecutar como usuario sin privilegios
USER 1000:1000

# Metadata del puerto
EXPOSE 5000

# Ejecución de producción con Gunicorn
# Timeout aumentado a 60s por latencia de APIs externas
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "nqr1g:app", "--workers", "2", "--threads", "4", "--timeout", "60"]