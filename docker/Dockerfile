FROM python:3.11-slim

WORKDIR /app

# Dependencias del sistema
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Instalar dependencias Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar codigo
COPY . .

# Carpeta de logs
RUN mkdir -p /app/logs

EXPOSE 8000

CMD ["uvicorn", "api.api_server:app", "--host", "0.0.0.0", "--port", "8000"]
