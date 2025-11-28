# --- ACTUALIZACIÓN CRÍTICA: Cambio de 'buster' a 'bookworm' ---
# Usamos Debian 12 (Bookworm) que es la versión estable actual y tiene repositorios activos.
FROM python:3.9-slim-bookworm

# Variables de entorno para optimización
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Directorio de trabajo
WORKDIR /app

# Instalación de dependencias del sistema
# Nota: Bookworm tiene repositorios actualizados, esto ya no fallará.
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copiar e instalar dependencias Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código fuente correcto
COPY nqr1g.py .

# Let Podman remap your host UID/GID into the container
USER 1000:1000 

# Puerto
EXPOSE 5000

# Comando de ejecución
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "nqr1g:app", "--workers", "2", "--threads", "4"]
