"""
Orquestador EC2: verifica S3 -> carga a PostgreSQL -> reporta

Uso:
    python -m pipeline.pipeline_ec2 --client NOVO --monthly-period 202601 --weekly-period 20260302_20260308
    python -m pipeline.pipeline_ec2 --client NOVO  # auto-detecta periodos desde S3
"""

from __future__ import print_function

import argparse
import logging
import os
import sys

import boto3
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("pipeline_ec2")

WEEKLY_TABLES = {
    "PERIODS_WEEKLY.txt",
    "OUTLETS_WEEKLY.txt",
    "DDD_WEEKLY.txt",
    "PH360_WEEKLY.txt",
}

# Tablas minimas que deben existir en S3 para considerar valida la subida
REQUIRED_MONTHLY = ["PERIODS.txt", "AUDITS.txt", "PRODUCTS.txt"]
REQUIRED_WEEKLY = ["PERIODS_WEEKLY.txt", "DDD_WEEKLY.txt"]


def get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION", "us-east-1"),
    )


def detect_latest_period(s3_client, bucket, client, prefix_type="monthly"):
    """
    Auto-detecta el periodo mas reciente en S3.
    prefix_type: 'monthly' o 'weekly'
    """
    if prefix_type == "monthly":
        prefix = "{}/".format(client)
        exclude = {"WEEKLY"}
    else:
        prefix = "{}/WEEKLY/".format(client)
        exclude = set()

    paginator = s3_client.get_paginator("list_objects_v2")
    folders = set()
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            folder = cp["Prefix"][len(prefix):].rstrip("/")
            if folder and folder not in exclude:
                folders.add(folder)

    if not folders:
        return None
    return sorted(folders)[-1]


def verify_s3_data(s3_client, bucket, client, monthly_period, weekly_period):
    """
    Verifica que los archivos requeridos existan en S3.
    Retorna (ok, lista_de_errores)
    """
    errors = []

    for table in REQUIRED_MONTHLY:
        key = "{}/{}/{}".format(client, monthly_period, table)
        try:
            s3_client.head_object(Bucket=bucket, Key=key)
            logger.info("  OK {}".format(key))
        except Exception:
            errors.append("Falta archivo mensual: {}".format(key))
            logger.error("  FALTA {}".format(key))

    if weekly_period:
        for table in REQUIRED_WEEKLY:
            key = "{}/WEEKLY/{}/{}".format(client, weekly_period, table)
            try:
                s3_client.head_object(Bucket=bucket, Key=key)
                logger.info("  OK {}".format(key))
            except Exception:
                errors.append("Falta archivo semanal: {}".format(key))
                logger.error("  FALTA {}".format(key))

    return len(errors) == 0, errors


def run_etl(client, monthly_period, weekly_period, dry_run=False):
    """Instancia y corre el ETLLoaderS3."""
    from etl_loader.etl_loader_s3 import ETLLoaderS3

    bucket = os.getenv("AWS_S3_BUCKET")
    s3_prefix = "{}/{}".format(client, monthly_period)
    weekly_s3_prefix = "{}/WEEKLY/{}".format(client, weekly_period) if weekly_period else None

    loader = ETLLoaderS3(
        s3_prefix=s3_prefix,
        weekly_s3_prefix=weekly_s3_prefix,
        dry_run=dry_run,
    )
    return loader.load_all_files(truncate_first=True)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Pipeline EC2: verifica S3 y carga a PostgreSQL"
    )
    parser.add_argument("--client", required=True, help="Nombre del cliente. Ej: NOVO")
    parser.add_argument("--monthly-period", default=None, help="Ej: 202601 (auto si no se pasa)")
    parser.add_argument("--weekly-period", default=None, help="Ej: 20260302_20260308 (auto si no se pasa)")
    parser.add_argument("--dry-run", action="store_true", help="Solo verifica, no carga")
    parser.add_argument("--skip-verify", action="store_true", help="Omite verificacion S3")
    return parser.parse_args()


def main():
    args = parse_args()
    bucket = os.getenv("AWS_S3_BUCKET")

    if not bucket:
        logger.error("AWS_S3_BUCKET no definido en el .env")
        sys.exit(1)

    s3_client = get_s3_client()

    # Auto-detectar periodos si no se pasaron
    monthly_period = args.monthly_period
    weekly_period = args.weekly_period

    if not monthly_period:
        logger.info("Auto-detectando periodo mensual desde S3...")
        monthly_period = detect_latest_period(s3_client, bucket, args.client, "monthly")
        if not monthly_period:
            logger.error("No se encontraron periodos mensuales en S3 para cliente {}".format(args.client))
            sys.exit(1)
        logger.info("Periodo mensual detectado: {}".format(monthly_period))

    if not weekly_period:
        logger.info("Auto-detectando periodo semanal desde S3...")
        weekly_period = detect_latest_period(s3_client, bucket, args.client, "weekly")
        if weekly_period:
            logger.info("Periodo semanal detectado: {}".format(weekly_period))
        else:
            logger.warning("No se encontraron periodos semanales. Se omitiran tablas weekly.")

    logger.info("=" * 70)
    logger.info("PIPELINE EC2 — S3 -> PostgreSQL")
    logger.info("=" * 70)
    logger.info("Cliente         : {}".format(args.client))
    logger.info("Periodo mensual : {}".format(monthly_period))
    logger.info("Periodo semanal : {}".format(weekly_period or "N/A"))
    logger.info("Dry run         : {}".format(args.dry_run))
    logger.info("=" * 70)

    # Verificacion S3
    if not args.skip_verify:
        logger.info("\nVerificando datos en S3...")
        ok, errors = verify_s3_data(s3_client, bucket, args.client, monthly_period, weekly_period)
        if not ok:
            logger.error("Verificacion S3 fallo con {} error(es):".format(len(errors)))
            for err in errors:
                logger.error("  - {}".format(err))
            sys.exit(1)
        logger.info("Verificacion S3 OK\n")

    # Carga ETL
    success = run_etl(args.client, monthly_period, weekly_period, dry_run=args.dry_run)

    if success:
        logger.info("Pipeline EC2 completado exitosamente.")
    else:
        logger.error("Pipeline EC2 finalizo con errores.")
        sys.exit(1)


if __name__ == "__main__":
    main()
