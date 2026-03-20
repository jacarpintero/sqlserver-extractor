"""
Configuración para el sistema de carga mensual de datos
"""

import os
from dotenv import load_dotenv

# Cargar variables de entorno desde .env
load_dotenv()

# Credenciales de base de datos
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

# Mapeo de archivos a tablas
TABLE_MAPPING = {
    "PERIODS.txt": {
        "table": "periods",
        "delimiter": ";",
        "description": "Períodos temporales",
    },
    "CORPORATIONS.txt": {
        "table": "corporations",
        "delimiter": ";",
        "description": "Corporaciones",
    },
    "LABORATORIES.txt": {
        "table": "laboratories",
        "delimiter": ";",
        "description": "Laboratorios",
    },
    "MARKETS.txt": {"table": "markets", "delimiter": ";", "description": "Mercados"},
    "PRODUCTS.txt": {"table": "products", "delimiter": ";", "description": "Productos"},
    "PACKS.txt": {"table": "packs", "delimiter": ";", "description": "Paquetes"},
    "OUTLETS.txt": {
        "table": "outlets",
        "delimiter": ";",
        "description": "Puntos de venta",
    },
    "AUDITS.txt": {
        "table": "audit_reports",
        "delimiter": ";",
        "description": "Auditorías",
    },
    "MPA.txt": {
        "table": "market_pack_audit",
        "delimiter": ";",
        "description": "Market Pack Audit",
    },
    "ML.txt": {
        "table": "market_lines",
        "delimiter": ";",
        "description": "Market Lines",
    },
    "MLM.txt": {
        "table": "market_line_market",
        "delimiter": ";",
        "description": "Market Line Market",
    },
    "TS.txt": {
        "table": "ts_detail_reports",
        "delimiter": ";",
        "description": "TS Detail Reports",
    },
    "PH360.txt": {
        "table": "ph_detail_reports",
        "delimiter": ";",
        "description": "Reportes detallados PH360",
    },
    "DDD.txt": {
        "table": "ddd_detail_reports",
        "delimiter": ";",
        "description": "DDD Detail Reports",
    },
    "DOCTORS.txt": {"table": "doctors", "delimiter": ";", "description": "Doctores"},
    "PBS.txt": {"table": "pbs_detail_reports", "delimiter": ";", "description": "PBS"},
    # Weekly tables
    "PERIODS_WEEKLY.txt": {
        "table": "periods_weekly",
        "delimiter": ";",
        "description": "Períodos Semanales",
    },
    "OUTLETS_WEEKLY.txt": {
        "table": "outlets_weekly",
        "delimiter": ";",
        "description": "Puntos de Venta Semanales",
    },
    "DDD_WEEKLY.txt": {
        "table": "ddd_weekly_detail_reports",
        "delimiter": ";",
        "description": "DDD Weekly Detail Reports",
    },
    "PH360_WEEKLY.txt": {
        "table": "ph_weekly_detail_reports",
        "delimiter": ";",
        "description": "PH360 Weekly Detail Reports",
    },
}

# Orden de carga respetando foreign keys
# Las tablas padre deben cargarse antes que las tablas hijas
LOAD_ORDER = [
    "PERIODS.txt",  # Padre
    "CORPORATIONS.txt",  # Padre
    "LABORATORIES.txt",  # Padre
    "MARKETS.txt",  # Padre
    "PRODUCTS.txt",  # Padre
    "PACKS.txt",  # Padre
    "OUTLETS.txt",  # Padre
    "DOCTORS.txt",  # Padre
    "ML.txt",  # market_lines (padre)
    "MLM.txt",  # market_line_market (padre) ← DEBE IR ANTES DE AUDITS
    "AUDITS.txt",  # audit_reports (padre)
    "MPA.txt",  # market_pack_audit (hija de MLM) ✓
    "TS.txt",  # ts_detail_reports
    "DDD.txt",  # ddd_detail_reports (hija de AUDITS)
    "PBS.txt",  # pbs_detail_reports
    "PH360.txt",  # ph_detail_reports (hija, al final)
    # Weekly tables - deben ir después de sus dependencias
    "PERIODS_WEEKLY.txt",  # Padre (weekly)
    "OUTLETS_WEEKLY.txt",  # Depende de bricks
    "DDD_WEEKLY.txt",  # Depende de periods_weekly, market_pack_audit, bricks
    "PH360_WEEKLY.txt",  # Depende de periods_weekly, outlets_weekly, market_pack_audit
]

# Validaciones de foreign keys
# Define qué columnas del archivo deben existir en qué tablas
FOREIGN_KEY_VALIDATIONS = {
    "PH360.txt": [
        {
            "file_column": "market_pack_audit_id",
            "ref_table": "market_pack_audit",
            "ref_column": "id",
        },
        {"file_column": "outlet_id", "ref_table": "outlets", "ref_column": "code"},
        {"file_column": "period_id", "ref_table": "periods", "ref_column": "id"},
    ],
    "DDD.txt": [
        {
            "file_column": "market_pack_audit_id",
            "ref_table": "market_pack_audit",
            "ref_column": "id",
        },
        {"file_column": "period_id", "ref_table": "periods", "ref_column": "id"},
        {"file_column": "brick_id", "ref_table": "bricks", "ref_column": "code"},
    ],
    "TS.txt": [
        {
            "file_column": "market_pack_audit_id",
            "ref_table": "market_pack_audit",
            "ref_column": "id",
        },
        {"file_column": "period_id", "ref_table": "periods", "ref_column": "id"},
    ],
    "LABORATORIES.txt": [
        {
            "file_column": "corporation_id",
            "ref_table": "corporations",
            "ref_column": "id",
        },
    ],
    "LABORATORIES.txt": [
        {
            "file_column": "corporation_id",
            "ref_table": "corporations",
            "ref_column": "id",
        },
    ],
    "MLM.txt": [
        {
            "file_column": "market_id",
            "ref_table": "markets",
            "ref_column": "id",
        },
        {
            "file_column": "market_line_id",
            "ref_table": "market_lines",
            "ref_column": "id",
        },
    ],
    "AUDITS.txt": [
        {
            "file_column": "market_line_market_id",
            "ref_table": "market_line_market",
            "ref_column": "id",
        },
        {
            "file_column": "pack_id",
            "ref_table": "packs",
            "ref_column": "id",
        },
        {
            "file_column": "audit_id",
            "ref_table": "audit_reports",
            "ref_column": "id",
        },
    ],
    "OUTLETS.txt": [
        {"file_column": "brick_id", "ref_table": "bricks", "ref_column": "code"},
    ],
    "PACKS.txt": [
        {"file_column": "product_id", "ref_table": "products", "ref_column": "id"},
    ],
    "PRODUCTS.txt": [
        {
            "file_column": "laboratory_id",
            "ref_table": "laboratories",
            "ref_column": "id",
        },
    ],
    "OUTLETS_WEEKLY.txt": [
        {"file_column": "brick_id", "ref_table": "bricks", "ref_column": "code"},
    ],
    "DDD_WEEKLY.txt": [
        {
            "file_column": "market_pack_audit_id",
            "ref_table": "market_pack_audit",
            "ref_column": "id",
        },
        {
            "file_column": "period_weekly_id",
            "ref_table": "periods_weekly",
            "ref_column": "id",
        },
        {"file_column": "brick_id", "ref_table": "bricks", "ref_column": "code"},
    ],
    "PH360_WEEKLY.txt": [
        {
            "file_column": "market_pack_audit_id",
            "ref_table": "market_pack_audit",
            "ref_column": "id",
        },
        {
            "file_column": "outlet_weekly_id",
            "ref_table": "outlets_weekly",
            "ref_column": "code",
        },
        {
            "file_column": "period_weekly_id",
            "ref_table": "periods_weekly",
            "ref_column": "id",
        },
    ],
}

# Configuración de optimización
OPTIMIZATION_CONFIG = {
    "use_freeze": True,  # Usa FREEZE en COPY para mejor performance
    "disable_triggers": True,  # Desactiva triggers durante la carga
    "synchronous_commit": False,  # Desactiva commit síncrono
    "run_analyze": True,  # Ejecuta ANALYZE después de la carga
    "batch_size": 50000,  # Tamaño de lote para archivos muy grandes
    "truncate_mode": "safe",  # "safe" (sin CASCADE), "cascade", "delete", "skip"
}

# Configuración de logging
LOG_CONFIG = {
    "log_file": "etl_load.log",
    "log_level": "INFO",
    "console_output": True,
}
