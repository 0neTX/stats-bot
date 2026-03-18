# ── build stage ──────────────────────────────────────────────────────────────
# Instala dependencias en un prefijo aislado; la imagen final no lleva pip.
FROM python:3.12-slim-bookworm AS builder

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /build
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# ── runtime stage ─────────────────────────────────────────────────────────────
FROM python:3.12-slim-bookworm

# Usuario sin privilegios dedicado al bot
RUN groupadd --gid 1001 botuser && \
    useradd  --uid 1001 --gid 1001 --no-create-home --shell /sbin/nologin botuser

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Copia solo los paquetes instalados (sin pip, setuptools ni wheel)
COPY --from=builder /install /usr/local

# Código fuente propiedad de root — el proceso no puede modificarlo
COPY --chown=root:root bot_estadisticas.py init_historial.py /app/src/

# Directorio de datos escribible únicamente por botuser
RUN mkdir -p /app/data && chown botuser:botuser /app/data

WORKDIR /app/data
USER botuser

CMD ["python", "/app/src/bot_estadisticas.py"]
