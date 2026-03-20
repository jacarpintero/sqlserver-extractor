"""
Extractor SQL Server -> AWS S3
Lee tablas de SQL Server y las sube directamente a S3 en formato TXT (CSV con ;)

El periodo se detecta automaticamente desde la columna 'data' de la tabla AUDITS.
Estructura S3: s3://bucket/CLIENTE/202601/TABLA.txt

Uso:
    # Periodo auto-detectado desde AUDITS.data
    python extractor.py --client NOVO --database DB_NOVO

    # Periodo manual (override)
    python extractor.py --client NOVO --database DB_NOVO --period 202601

    # Solo algunas tablas
    python extractor.py --client NOVO --database DB_NOVO --tables PERIODS.txt PRODUCTS.txt

    # Servidor con instancia
    python extractor.py --client NOVO --database DB_NOVO --server SERVIDOR\\INSTANCIA

Compatible con Python 3.6+
"""

from __future__ import print_function

import argparse
import csv
import io
import logging
import os
import sys
import time
from datetime import datetime

import boto3
import pyodbc
from botocore.exceptions import ClientError
from dotenv import load_dotenv

from config import (
    BATCH_SIZE,
    DELIMITER,
    ENCODING,
    EXTRACT_ORDER,
    S3_MULTIPART_THRESHOLD,
    get_connection_string,
    get_sql_table_name,
)

load_dotenv()


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def setup_logging(client_name):
    log_format = "%(asctime)s [%(levelname)s] %(message)s"
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(
                "extractor_{}.log".format(client_name), encoding="utf-8"
            ),
        ],
    )
    return logging.getLogger("extractor")


# ---------------------------------------------------------------------------
# S3
# ---------------------------------------------------------------------------

def get_s3_client():
    """Crea cliente S3 usando credenciales del .env"""
    return boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION", "us-east-1"),
    )


def upload_to_s3(s3_client, bucket, s3_key, data_bytes, logger):
    """
    Sube bytes a S3.
    Usa multipart upload si el tamano supera S3_MULTIPART_THRESHOLD.
    """
    size_mb = len(data_bytes) / (1024 * 1024)
    logger.info("  Subiendo a S3: s3://{}/{} ({:.1f} MB)".format(bucket, s3_key, size_mb))

    try:
        if len(data_bytes) >= S3_MULTIPART_THRESHOLD:
            # Multipart upload para archivos grandes
            mpu = s3_client.create_multipart_upload(Bucket=bucket, Key=s3_key)
            upload_id = mpu["UploadId"]
            parts = []
            part_size = 8 * 1024 * 1024  # 8 MB por parte
            part_number = 1

            for offset in range(0, len(data_bytes), part_size):
                chunk = data_bytes[offset:offset + part_size]
                response = s3_client.upload_part(
                    Bucket=bucket,
                    Key=s3_key,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=chunk,
                )
                parts.append({"PartNumber": part_number, "ETag": response["ETag"]})
                logger.info("  Parte {}: {:.1f} MB subida".format(
                    part_number, len(chunk) / (1024 * 1024)
                ))
                part_number += 1

            s3_client.complete_multipart_upload(
                Bucket=bucket,
                Key=s3_key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )
        else:
            s3_client.put_object(Bucket=bucket, Key=s3_key, Body=data_bytes)

        logger.info("  OK subido exitosamente")
        return True

    except ClientError as e:
        logger.error("  ERROR subiendo a S3: {}".format(e))
        return False


# ---------------------------------------------------------------------------
# Periodo desde AUDITS
# ---------------------------------------------------------------------------

def get_period_from_audits(conn, logger):
    """
    Lee el valor MAX(data) de la tabla L3_AUDITS_TB para usarlo como
    nombre de carpeta en S3. Ejemplo: 202601
    """
    audits_table = get_sql_table_name("AUDITS.txt")
    logger.info("Detectando periodo desde {}...".format(audits_table))

    cursor = conn.cursor()
    try:
        cursor.execute("SELECT MAX(data) FROM {}".format(audits_table))
        row = cursor.fetchone()
        if not row or row[0] is None:
            raise ValueError("La tabla {} esta vacia o la columna 'data' no tiene valores".format(audits_table))
        period = str(row[0])
        logger.info("Periodo detectado: {}".format(period))
        return period
    finally:
        cursor.close()


# ---------------------------------------------------------------------------
# Extraccion
# ---------------------------------------------------------------------------

def extract_table(cursor, sql_table_name, logger):
    """
    Extrae una tabla de SQL Server y retorna los bytes del TXT.
    Procesa en batches para no cargar todo en memoria de una vez.
    """
    logger.info("  Ejecutando SELECT * FROM {}...".format(sql_table_name))

    cursor.execute("SELECT * FROM {}".format(sql_table_name))

    buffer = io.StringIO()
    writer = csv.writer(buffer, delimiter=DELIMITER, lineterminator="\n")

    # Header
    columns = [col[0] for col in cursor.description]
    writer.writerow(columns)

    total_rows = 0
    while True:
        rows = cursor.fetchmany(BATCH_SIZE)
        if not rows:
            break
        writer.writerows(rows)
        total_rows += len(rows)
        if total_rows % 100000 == 0:
            logger.info("  {} filas leidas...".format(total_rows))

    data_bytes = buffer.getvalue().encode(ENCODING, errors="replace")
    logger.info("  {} filas extraidas ({:.1f} MB)".format(
        total_rows, len(data_bytes) / (1024 * 1024)
    ))
    return data_bytes, total_rows


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="Extrae tablas de SQL Server y las sube a S3"
    )
    parser.add_argument(
        "--client",
        required=True,
        help="Nombre del cliente (usado como carpeta en S3). Ej: CLIENTE_A",
    )
    parser.add_argument(
        "--database",
        required=True,
        help="Nombre de la base de datos en SQL Server. Ej: DB_CLIENTE_A",
    )
    parser.add_argument(
        "--period",
        required=False,
        default=None,
        help="Periodo de los datos (opcional). Si no se indica, se detecta automaticamente desde MAX(data) de AUDITS. Ej: 202601",
    )
    parser.add_argument(
        "--server",
        default=os.getenv("SQL_SERVER", "localhost"),
        help="Servidor SQL Server. Default: variable SQL_SERVER del .env",
    )
    parser.add_argument(
        "--tables",
        nargs="+",
        default=None,
        help="Tablas especificas a extraer (opcional). Ej: PERIODS.txt PRODUCTS.txt",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    logger = setup_logging(args.client)

    bucket = os.getenv("AWS_S3_BUCKET")
    if not bucket:
        logger.error("AWS_S3_BUCKET no definido en el .env")
        sys.exit(1)

    # Conectar a SQL Server
    logger.info("Conectando a SQL Server...")
    try:
        conn_str = get_connection_string(args.server, args.database)
        conn = pyodbc.connect(conn_str)
        conn.autocommit = True
        logger.info("Conectado exitosamente")
    except Exception as e:
        logger.error("Error conectando a SQL Server: {}".format(e))
        sys.exit(1)

    # Detectar periodo desde AUDITS si no se paso manualmente
    if args.period:
        period = args.period
        logger.info("Periodo (manual): {}".format(period))
    else:
        try:
            period = get_period_from_audits(conn, logger)
        except Exception as e:
            logger.error("No se pudo detectar el periodo: {}".format(e))
            conn.close()
            sys.exit(1)

    logger.info("=" * 70)
    logger.info("EXTRACTOR SQL SERVER -> S3")
    logger.info("=" * 70)
    logger.info("Cliente  : {}".format(args.client))
    logger.info("Base datos: {}".format(args.database))
    logger.info("Servidor : {}".format(args.server))
    logger.info("Periodo  : {}".format(period))
    logger.info("=" * 70)

    # Conectar a S3
    logger.info("Conectando a S3...")
    try:
        s3_client = get_s3_client()
        s3_client.head_bucket(Bucket=bucket)
        logger.info("Bucket '{}' accesible".format(bucket))
    except Exception as e:
        logger.error("Error conectando a S3: {}".format(e))
        conn.close()
        sys.exit(1)

    # Determinar tablas a extraer
    tables_to_extract = args.tables if args.tables else EXTRACT_ORDER
    s3_prefix = "{}/{}/".format(args.client, period)

    logger.info("\nPrefijo S3: s3://{}/{}".format(bucket, s3_prefix))
    logger.info("Tablas a extraer: {}\n".format(len(tables_to_extract)))

    # Extraer y subir cada tabla
    success_count = 0
    error_count = 0
    total_start = time.time()

    for file_name in tables_to_extract:
        logger.info("-" * 70)
        logger.info("Procesando: {}".format(file_name))

        try:
            sql_table = get_sql_table_name(file_name)
            cursor = conn.cursor()
            start = time.time()

            data_bytes, total_rows = extract_table(cursor, sql_table, logger)
            cursor.close()

            s3_key = "{}{}".format(s3_prefix, file_name)
            uploaded = upload_to_s3(s3_client, bucket, s3_key, data_bytes, logger)

            duration = time.time() - start

            if uploaded:
                logger.info("  OK {} -> {} filas en {:.1f}s".format(
                    file_name, total_rows, duration
                ))
                success_count += 1
            else:
                error_count += 1

        except Exception as e:
            logger.error("  ERROR procesando {}: {}".format(file_name, e))
            error_count += 1

    conn.close()

    total_duration = time.time() - total_start
    logger.info("\n" + "=" * 70)
    logger.info("RESUMEN")
    logger.info("=" * 70)
    logger.info("Exitosas : {}".format(success_count))
    logger.info("Errores  : {}".format(error_count))
    logger.info("Duracion : {:.1f}s ({:.1f} min)".format(
        total_duration, total_duration / 60
    ))
    logger.info("S3 prefix: s3://{}/{}".format(bucket, s3_prefix))
    logger.info("Periodo  : {}".format(period))
    logger.info("=" * 70)

    if error_count > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
