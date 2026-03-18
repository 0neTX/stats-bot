FROM python:3.12-slim

# Install dependencies
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code to /app/src
COPY bot_estadisticas.py init_historial.py ./src/

# DB is created in the working directory; use /app/data as runtime workdir
WORKDIR /app/data

CMD ["python", "/app/src/bot_estadisticas.py"]
