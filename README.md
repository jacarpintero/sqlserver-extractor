# SQL Server Extractor & ETL Pipeline

Pipeline completo para extraer datos de SQL Server, subirlos a S3 y cargarlos en PostgreSQL.

```
Windows (SQL Server)
      │
      ▼
 extractor  ──►  S3  ──►  ETL Loader (Docker)  ──►  PostgreSQL
```

---

## Requisitos previos

### Windows (extractor)
- Python 3.6.3+
- ODBC Driver 17 for SQL Server ([descargar](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server))
- Acceso a SQL Server y credenciales AWS

### EC2 / Local (ETL API)
- Docker Desktop instalado y corriendo
- Acceso a PostgreSQL y credenciales AWS

---

## 1. Configuración inicial

### Clonar el repo
```bash
git clone <tu-repo>
cd sqlserver-extractor
```

### Crear el archivo `.env`
```bash
cp .env.example .env
```

Editar `.env` con los valores reales:

```env
# Windows
SQL_SERVER=NOMBRE_SERVIDOR
EC2_API_URL=http://localhost:8000
EC2_API_TOKEN=un_token_secreto

# EC2 / Docker
DB_HOST=host_postgres
DB_NAME=nombre_db
DB_USER=usuario
DB_PASSWORD=password

# Compartido
AWS_ACCESS_KEY_ID=tu_access_key
AWS_SECRET_ACCESS_KEY=tu_secret_key
AWS_REGION=us-east-1
AWS_S3_BUCKET=nombre-de-tu-bucket
```

---

## 2. Windows — Extractor (SQL Server → S3)

### Instalar dependencias
```bash
pip install -r requirements-windows.txt
```

### Correr extractor (solo extracción a S3)
```bash
# Detección automática de periodos
python -m extractor.extractor --client NOVO --database DB_NOVO

# Con servidor específico
python -m extractor.extractor --client NOVO --database DB_NOVO --server SERVIDOR\INSTANCIA

# Con periodo mensual manual
python -m extractor.extractor --client NOVO --database DB_NOVO --period 202601

# Solo algunas tablas
python -m extractor.extractor --client NOVO --database DB_NOVO --tables PERIODS.txt PRODUCTS.txt

# Con retención automática S3
python -m extractor.extractor --client NOVO --database DB_NOVO --retention-monthly 3 --retention-weekly 6
```

### Correr pipeline completo (extrae + notifica EC2 para cargar)
```bash
# Extrae a S3 y dispara la carga en EC2 automáticamente
python -m pipeline.pipeline_windows --client NOVO --database DB_NOVO

# Solo extrae, sin notificar EC2
python -m pipeline.pipeline_windows --client NOVO --database DB_NOVO --no-notify

# Con retención
python -m pipeline.pipeline_windows --client NOVO --database DB_NOVO --retention-monthly 3 --retention-weekly 6
```

---

## 3. Docker — ETL API (S3 → PostgreSQL)

### Levantar el servidor
```bash
# Desde la raíz del proyecto
docker compose -f docker/docker-compose.yml up -d
```

### Verificar que está corriendo
```bash
docker ps
# Deberías ver: etl-pipeline-api   Up   0.0.0.0:8000->8000/tcp

curl http://localhost:8000/health
# {"status":"ok"}
```

### Ver logs en tiempo real
```bash
docker logs -f etl-pipeline-api
```

### Detener
```bash
docker compose -f docker/docker-compose.yml down
```

### Rebuild después de cambios en el código
```bash
docker compose -f docker/docker-compose.yml up -d --build
```

---

## 4. EC2 / Local — ETL Pipeline (S3 → PostgreSQL)

### Opción A — Trigger vía API (automático desde Windows)
El pipeline completo se dispara automáticamente al correr `pipeline_windows.py`.

También se puede disparar manualmente con curl:
```bash
curl -X POST http://localhost:8000/trigger \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer un_token_secreto" \
  -d '{"client": "NOVO"}'
```

Con periodos manuales:
```bash
curl -X POST http://localhost:8000/trigger \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer un_token_secreto" \
  -d '{"client": "NOVO", "monthly_period": "202601", "weekly_period": "20260302_20260308"}'
```

### Consultar estado de un job
```bash
curl http://localhost:8000/status/<job_id> \
  -H "Authorization: Bearer un_token_secreto"
```

### Ver todos los jobs
```bash
curl http://localhost:8000/jobs \
  -H "Authorization: Bearer un_token_secreto"
```

### Opción C — Correr pipeline directamente (sin Docker)
```bash
# Auto-detecta los periodos más recientes desde S3
python -m pipeline.pipeline_ec2 --client NOVO

# Con periodos específicos
python -m pipeline.pipeline_ec2 --client NOVO --monthly-period 202601 --weekly-period 20260302_20260308

# Dry run (verifica S3 pero no carga datos)
python -m pipeline.pipeline_ec2 --client NOVO --dry-run
```

---

## 5. Estructura S3

```
bucket/
└── CLIENTE/
    ├── 202601/               ← datos mensuales
    │   ├── PERIODS.txt
    │   ├── PRODUCTS.txt
    │   ├── DDD.txt
    │   └── ...
    ├── 202602/
    └── WEEKLY/
        ├── 20260113_20260119/  ← datos semanales
        │   ├── PERIODS_WEEKLY.txt
        │   ├── OUTLETS_WEEKLY.txt
        │   ├── DDD_WEEKLY.txt
        │   └── PH360_WEEKLY.txt
        └── 20260120_20260126/
```

---

## 6. Estructura del proyecto

```
sqlserver-extractor/
├── extractor/              # Windows: SQL Server → S3
│   ├── config.py
│   └── extractor.py
├── etl_loader/             # EC2: S3 → PostgreSQL
│   ├── config.py
│   ├── etl_loader_s3.py
│   ├── utils.py
│   └── s3_manager.py
├── pipeline/               # Orquestadores
│   ├── pipeline_windows.py
│   └── pipeline_ec2.py
├── api/                    # API Server (FastAPI)
│   └── api_server.py
├── docker/
│   ├── Dockerfile
│   └── docker-compose.yml
├── .env.example
├── requirements.txt        # EC2 / Docker
└── requirements-windows.txt  # Windows
```

---

## 7. Variables de entorno

| Variable | Descripción | Lado |
|---|---|---|
| `SQL_SERVER` | Nombre o IP del servidor SQL Server | Windows |
| `EC2_API_URL` | URL del API server (ej: `http://1.2.3.4:8000`) | Windows |
| `EC2_API_TOKEN` | Token de autenticación del API | Ambos |
| `DB_HOST` | Host de PostgreSQL | EC2 |
| `DB_NAME` | Nombre de la base de datos | EC2 |
| `DB_USER` | Usuario PostgreSQL | EC2 |
| `DB_PASSWORD` | Contraseña PostgreSQL | EC2 |
| `AWS_ACCESS_KEY_ID` | Access key de AWS | Ambos |
| `AWS_SECRET_ACCESS_KEY` | Secret key de AWS | Ambos |
| `AWS_REGION` | Región AWS (default: `us-east-1`) | Ambos |
| `AWS_S3_BUCKET` | Nombre del bucket S3 | Ambos |
