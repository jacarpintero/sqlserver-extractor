"""
Extractor SQL Server -> AWS S3
Lee tablas de SQL Server y las sube directamente a S3 en formato TXT (CSV con ;)

Periodos:
  - Mensual: detectado desde MAX(data) de AUDITS     -> s3://bucket/CLIENTE/202601/TABLA.txt
  - Semanal: detectado desde PERIODS_WEEKLY id_period=1 -> s3://bucket/CLIENTE/WEEKLY/20260302_20260308/TABLA.txt

Uso:
    # Extraccion completa con periodos auto-detectados
    python extractor.py --client NOVO --database DB_NOVO

    # Periodo mensual manual (override)
    python extractor.py --client NOVO --database DB_NOVO --period 202601

    # Solo algunas tablas
    python extractor.py --client NOVO --database DB_NOVO --tables PERIODS.txt PRODUCTS.txt

    # Con retencion automatica en S3
    python extractor.py --client NOVO --database DB_NOVO --retention-monthly 3 --retention-weekly 6

    # Servidor con instancia
    python extractor.py --client NOVO --database DB_NOVO --server SERVIDOR\\INSTANCIA

Compatible con Python 3.6+
"""

from __future__ import print_function

import argparse
import csv
import logging
import os
import sys
import tempfile
import time
from datetime import datetime

import boto3
import pyodbc
from botocore.exceptions import ClientError
from dotenv import load_dotenv

from extractor.config import (
    BATCH_SIZE,
    DELIMITER,
    ENCODING,
    EXTRACT_ORDER,
    WEEKLY_TABLES,
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


def upload_to_s3(s3_client, bucket, s3_key, local_path, logger):
    """
    Sube un archivo local a S3.
    boto3 usa multipart automaticamente para archivos grandes.
    """
    size_bytes = os.path.getsize(local_path)
    size_mb = size_bytes / (1024 * 1024)
    logger.info(
        "  Subiendo a S3: s3://{}/{} ({:.1f} MB)".format(bucket, s3_key, size_mb)
    )

    try:
        s3_client.upload_file(local_path, bucket, s3_key)
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
            raise ValueError(
                "La tabla {} esta vacia o la columna 'data' no tiene valores".format(
                    audits_table
                )
            )
        period = str(row[0])
        logger.info("Periodo detectado: {}".format(period))
        return period
    finally:
        cursor.close()


# ---------------------------------------------------------------------------
# Periodo semanal desde PERIODS_WEEKLY
# ---------------------------------------------------------------------------


def get_weekly_period_from_periods_weekly(conn, logger):
    """
    Lee week_start_date y week_end_date de L3_PERIODS_WEEKLY_TB donde id_period = 1.
    Retorna string con formato YYYYMMDD_YYYYMMDD o None si la tabla no existe.
    """
    table = get_sql_table_name("PERIODS_WEEKLY.txt")
    logger.info("Detectando periodo semanal desde {}...".format(table))

    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT week_start_date, week_end_date FROM {} WHERE id_period = 1".format(
                table
            )
        )
        row = cursor.fetchone()
        if not row or row[0] is None or row[1] is None:
            logger.warning("No se encontro fila con id_period = 1 en {}".format(table))
            return None
        start = str(row[0]).replace("-", "")[:8]
        end = str(row[1]).replace("-", "")[:8]
        weekly_period = "{}_{}".format(start, end)
        logger.info("Periodo semanal detectado: {}".format(weekly_period))
        return weekly_period
    except Exception as e:
        logger.warning("Tabla semanal no disponible ({}): {}".format(table, e))
        return None
    finally:
        cursor.close()


# ---------------------------------------------------------------------------
# Retencion S3
# ---------------------------------------------------------------------------


def list_s3_period_folders(s3_client, bucket, prefix, logger):
    """
    Lista las subcarpetas directas bajo un prefijo S3.
    Retorna lista de strings ordenada ascendentemente.
    """
    paginator = s3_client.get_paginator("list_objects_v2")
    folders = set()
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            folder = cp["Prefix"][len(prefix) :].rstrip("/")
            if folder:
                folders.add(folder)
    return sorted(folders)


def delete_s3_folder(s3_client, bucket, prefix, logger):
    """Borra todos los objetos bajo un prefijo S3."""
    paginator = s3_client.get_paginator("list_objects_v2")
    deleted = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        objects = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
        if objects:
            s3_client.delete_objects(Bucket=bucket, Delete={"Objects": objects})
            deleted += len(objects)
    logger.info(
        "  Eliminados {} objetos bajo s3://{}/{}".format(deleted, bucket, prefix)
    )


def apply_retention(
    s3_client, bucket, client_name, retention_monthly, retention_weekly, logger
):
    """
    Aplica politica de retencion eliminando periodos viejos de S3.
    - retention_monthly: numero de periodos mensuales a conservar (0 = no borrar)
    - retention_weekly: numero de periodos semanales a conservar (0 = no borrar)
    """
    if retention_monthly and retention_monthly > 0:
        monthly_prefix = "{}/".format(client_name)
        folders = [
            f
            for f in list_s3_period_folders(s3_client, bucket, monthly_prefix, logger)
            if f != "WEEKLY"
        ]
        to_delete = (
            folders[:-retention_monthly] if len(folders) > retention_monthly else []
        )
        if to_delete:
            logger.info(
                "Retencion mensual: eliminando {} periodo(s) antiguo(s)...".format(
                    len(to_delete)
                )
            )
            for folder in to_delete:
                prefix = "{}/{}/".format(client_name, folder)
                logger.info("  Borrando {}".format(prefix))
                delete_s3_folder(s3_client, bucket, prefix, logger)
        else:
            logger.info(
                "Retencion mensual: nada que eliminar ({} periodos en S3)".format(
                    len(folders)
                )
            )

    if retention_weekly and retention_weekly > 0:
        weekly_prefix = "{}/WEEKLY/".format(client_name)
        folders = list_s3_period_folders(s3_client, bucket, weekly_prefix, logger)
        to_delete = (
            folders[:-retention_weekly] if len(folders) > retention_weekly else []
        )
        if to_delete:
            logger.info(
                "Retencion semanal: eliminando {} periodo(s) antiguo(s)...".format(
                    len(to_delete)
                )
            )
            for folder in to_delete:
                prefix = "{}/WEEKLY/{}/".format(client_name, folder)
                logger.info("  Borrando {}".format(prefix))
                delete_s3_folder(s3_client, bucket, prefix, logger)
        else:
            logger.info(
                "Retencion semanal: nada que eliminar ({} periodos en S3)".format(
                    len(folders)
                )
            )


# ---------------------------------------------------------------------------
# Extraccion
# ---------------------------------------------------------------------------


def check_table(conn, sql_table_name, logger):
    """
    Verifica si una tabla existe en SQL Server y tiene filas.
    Retorna: ('ok', count), ('not_found', 0) o ('empty', 0)
    """
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT COUNT(*) FROM {}".format(sql_table_name))
        count = cursor.fetchone()[0]
        if count == 0:
            return "empty", 0
        return "ok", count
    except Exception as e:
        error_str = str(e)
        if "42S02" in error_str or "Invalid object name" in error_str:
            return "not_found", 0
        raise
    finally:
        cursor.close()


def extract_table(cursor, sql_table_name, logger):
    """
    Extrae una tabla de SQL Server a un archivo temporal (TXT CSV con ;).
    Escribe por batches: el resultado no se acumula entero en RAM (solo batches
    de fetchmany + buffer de escritura del SO).
    """
    logger.info("  Ejecutando SELECT * FROM {}...".format(sql_table_name))

    cursor.execute("SELECT * FROM {}".format(sql_table_name))

    fd, path = tempfile.mkstemp(suffix=".txt", prefix="sql_extract_")
    os.close(fd)
    try:
        with open(path, "w", encoding=ENCODING, errors="replace", newline="") as out:
            writer = csv.writer(out, delimiter=DELIMITER, lineterminator="\n")

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

        size_bytes = os.path.getsize(path)
        logger.info(
            "  {} filas extraidas ({:.1f} MB)".format(
                total_rows, size_bytes / (1024 * 1024)
            )
        )
        return path, total_rows
    except Exception:
        try:
            os.unlink(path)
        except OSError:
            pass
        raise


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
    parser.add_argument(
        "--retention-monthly",
        type=int,
        default=0,
        metavar="N",
        help="Elimina periodos mensuales en S3 que excedan los ultimos N. Ej: 3",
    )
    parser.add_argument(
        "--retention-weekly",
        type=int,
        default=0,
        metavar="N",
        help="Elimina periodos semanales en S3 que excedan los ultimos N. Ej: 6",
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

    # Detectar periodo mensual desde AUDITS si no se paso manualmente
    if args.period:
        period = args.period
        logger.info("Periodo mensual (manual): {}".format(period))
    else:
        try:
            period = get_period_from_audits(conn, logger)
        except Exception as e:
            logger.error("No se pudo detectar el periodo mensual: {}".format(e))
            conn.close()
            sys.exit(1)

    # Detectar periodo semanal desde PERIODS_WEEKLY (None si el cliente no tiene weekly)
    weekly_period = get_weekly_period_from_periods_weekly(conn, logger)
    if weekly_period is None:
        logger.warning("Cliente sin datos semanales. Las tablas WEEKLY seran omitidas.")

    logger.info("=" * 70)
    logger.info("EXTRACTOR SQL SERVER -> S3")
    logger.info("=" * 70)
    logger.info("Cliente         : {}".format(args.client))
    logger.info("Base datos      : {}".format(args.database))
    logger.info("Servidor        : {}".format(args.server))
    logger.info("Periodo mensual : {}".format(period))
    logger.info("Periodo semanal : {}".format(weekly_period or "N/A (sin datos semanales)"))
    logger.info("Retencion mens. : {}".format(args.retention_monthly or "sin limite"))
    logger.info("Retencion sem.  : {}".format(args.retention_weekly or "sin limite"))
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

    logger.info(
        "\nPrefijo mensual : s3://{}/{}/{}/".format(bucket, args.client, period)
    )
    if weekly_period:
        logger.info(
            "Prefijo semanal : s3://{}/{}/WEEKLY/{}/".format(
                bucket, args.client, weekly_period
            )
        )
    logger.info("Tablas a extraer: {}\n".format(len(tables_to_extract)))

    # Extraer y subir cada tabla
    success_count = 0
    error_count = 0
    total_start = time.time()

    skipped_count = 0

    for file_name in tables_to_extract:
        logger.info("-" * 70)
        logger.info("Procesando: {}".format(file_name))

        # Omitir tablas weekly si el cliente no tiene datos semanales
        if file_name in WEEKLY_TABLES and weekly_period is None:
            logger.warning("  OMITIDA (cliente sin datos semanales): {}".format(file_name))
            skipped_count += 1
            continue

        tmp_path = None
        try:
            sql_table = get_sql_table_name(file_name)

            # Verificar existencia y filas antes de extraer
            status, row_count = check_table(conn, sql_table, logger)
            if status == "not_found":
                logger.warning("  OMITIDA (tabla no existe en SQL Server): {}".format(sql_table))
                skipped_count += 1
                continue
            if status == "empty":
                logger.warning("  OMITIDA (tabla vacia): {}".format(sql_table))
                skipped_count += 1
                continue

            cursor = conn.cursor()
            start = time.time()

            tmp_path, total_rows = extract_table(cursor, sql_table, logger)
            cursor.close()

            if file_name in WEEKLY_TABLES:
                s3_key = "{}/WEEKLY/{}/{}".format(args.client, weekly_period, file_name)
            else:
                s3_key = "{}/{}/{}".format(args.client, period, file_name)
            uploaded = upload_to_s3(s3_client, bucket, s3_key, tmp_path, logger)

            duration = time.time() - start

            if uploaded:
                logger.info(
                    "  OK {} -> {} filas en {:.1f}s".format(
                        file_name, total_rows, duration
                    )
                )
                success_count += 1
            else:
                error_count += 1

        except Exception as e:
            err_text = str(e).strip() or repr(e)
            logger.error(
                "  ERROR procesando {}: {} ({})".format(
                    file_name, type(e).__name__, err_text
                ),
                exc_info=True,
            )
            error_count += 1
        finally:
            if tmp_path and os.path.isfile(tmp_path):
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass

    conn.close()

    total_duration = time.time() - total_start

    # Aplicar retencion si se solicitó (despues de subir todo)
    if args.retention_monthly or args.retention_weekly:
        logger.info("\n" + "=" * 70)
        logger.info("RETENCION S3")
        logger.info("=" * 70)
        apply_retention(
            s3_client,
            bucket,
            args.client,
            args.retention_monthly,
            args.retention_weekly,
            logger,
        )

    logger.info("\n" + "=" * 70)
    logger.info("RESUMEN")
    logger.info("=" * 70)
    logger.info("Exitosas        : {}".format(success_count))
    logger.info("Omitidas        : {}".format(skipped_count))
    logger.info("Errores         : {}".format(error_count))
    logger.info(
        "Duracion        : {:.1f}s ({:.1f} min)".format(
            total_duration, total_duration / 60
        )
    )
    logger.info("Periodo mensual : s3://{}/{}/{}/".format(bucket, args.client, period))
    if weekly_period:
        logger.info(
            "Periodo semanal : s3://{}/{}/WEEKLY/{}/".format(
                bucket, args.client, weekly_period
            )
        )
    logger.info("=" * 70)

    if error_count > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
