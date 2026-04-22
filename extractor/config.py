"""
Configuracion del extractor SQL Server -> S3
Compatible con Python 3.6+
"""

import os

# Prefijo de tablas en SQL Server
TABLE_PREFIX = "L3_"
TABLE_SUFFIX = "_TB"
SCHEMA = "dbo"

# Mapeo: nombre_archivo_txt -> nombre_tabla_sqlserver (sin prefijo ni sufijo)
TABLE_MAPPING = {
    "PERIODS.txt":              "PERIODS",
    "CORPORATIONS.txt":         "CORPORATION",
    "LABORATORIES.txt":         "LABORATORIES",
    "MARKETS.txt":              "MARKETS",
    "PRODUCTS.txt":             "PRODUCTS",
    "PACKS.txt":                "PACKS",
    "OUTLETS.txt":              "OUTLETS",
    "DOCTORS.txt":              "DOCTORS",
    "ML.txt":                   "MARKET_LINES",
    "MLM.txt":                  "MARKET_LINES_MARKET",
    "AUDITS.txt":               "AUDITS",
    "MPA.txt":                  "MARKET_PACK_AUDIT",
    "TS.txt":                   "TS_DETAIL_REPORT",
    "DDD.txt":                  "DDD_DETAIL_REPORT",
    "PBS.txt":                  "PBS_DETAIL_REPORT",
    "PH360.txt":                "PH360_DETAIL_REPORT",
    "PERIODS_WEEKLY.txt":       "PERIODS_WEEKLY",
    "OUTLETS_WEEKLY.txt":       "OUTLETS_WEEKLY",
    "DDD_WEEKLY.txt":           "DDD_WEEKLY_DETAIL_REPORT",
    "PH360_WEEKLY.txt":         "PH360_WEEKLY_DETAIL_REPORT",
}

# Tablas con frecuencia semanal (van a CLIENTE/WEEKLY/PERIODO_SEMANAL/)
WEEKLY_TABLES = {
    "PERIODS_WEEKLY.txt",
    "OUTLETS_WEEKLY.txt",
    "DDD_WEEKLY.txt",
    "PH360_WEEKLY.txt",
}

# Orden de extraccion (respeta dependencias para consistencia)
EXTRACT_ORDER = [
    "PERIODS.txt",
    "CORPORATIONS.txt",
    "LABORATORIES.txt",
    "MARKETS.txt",
    "PRODUCTS.txt",
    "PACKS.txt",
    "OUTLETS.txt",
    "DOCTORS.txt",
    "ML.txt",
    "MLM.txt",
    "AUDITS.txt",
    "MPA.txt",
    "TS.txt",
    "DDD.txt",
    "PBS.txt",
    "PH360.txt",
    "PERIODS_WEEKLY.txt",
    "OUTLETS_WEEKLY.txt",
    "DDD_WEEKLY.txt",
    "PH360_WEEKLY.txt",
]

# Delimitador de los archivos de salida
DELIMITER = ";"

# Encoding de salida (debe coincidir con lo que espera el ETL loader)
ENCODING = "latin1"

# Tamano del batch al leer filas de SQL Server
BATCH_SIZE = 50000


def get_sql_table_name(table_key):
    """Retorna el nombre completo de la tabla en SQL Server"""
    base = TABLE_MAPPING[table_key]
    return "{schema}.{prefix}{table}{suffix}".format(
        schema=SCHEMA,
        prefix=TABLE_PREFIX,
        table=base,
        suffix=TABLE_SUFFIX,
    )


def get_connection_string(server, database):
    """
    Construye el connection string para Windows Authentication.
    Intenta primero con ODBC Driver 17, luego con el driver generico.
    """
    drivers = [
        "ODBC Driver 17 for SQL Server",
        "SQL Server",
    ]
    for driver in drivers:
        try:
            import pyodbc
            conn_str = (
                "DRIVER={{{driver}}};"
                "SERVER={server};"
                "DATABASE={database};"
                "Trusted_Connection=yes;"
            ).format(driver=driver, server=server, database=database)
            # Probar la conexion
            pyodbc.connect(conn_str, timeout=5)
            return conn_str
        except Exception:
            continue
    raise RuntimeError(
        "No se pudo conectar. Verifica que el servidor y la base de datos sean correctos."
    )
