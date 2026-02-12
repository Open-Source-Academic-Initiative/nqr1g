# Base sólida (Python 3.12 / Debian 12)
FROM python:3.12-slim-bookworm

# Optimización de Python
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Dependencias del sistema
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Instalación de dependencias de Python
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copia de código y templates
COPY --chown=1000:1000 . .

# Seguridad: Usuario no privilegiado
USER 1000:1000

EXPOSE 5000

# Ejecución con Gunicorn + UvicornWorker
# -k uvicorn.workers.UvicornWorker permite manejar FastAPI de forma asíncrona
# Timeout de 120s alineado con la lógica de la aplicación
CMD ["gunicorn", \
     "--bind", "0.0.0.0:5000", \
     "nqr1g:app", \
     "--workers", "4", \
     "--worker-class", "uvicorn.workers.UvicornWorker", \
     "--timeout", "120"]
