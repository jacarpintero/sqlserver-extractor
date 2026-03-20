"""
Orquestador Windows: SQL Server -> S3 -> notifica EC2

Uso:
    # Completo (extrae + notifica EC2 para que cargue)
    python -m pipeline.pipeline_windows --client NOVO --database DB_NOVO

    # Solo extraccion, sin notificar EC2
    python -m pipeline.pipeline_windows --client NOVO --database DB_NOVO --no-notify

    # Con retencion S3
    python -m pipeline.pipeline_windows --client NOVO --database DB_NOVO --retention-monthly 3 --retention-weekly 6
"""

from __future__ import print_function

import argparse
import logging
import os
import subprocess
import sys

import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("pipeline_windows")


def run_extractor(args):
    """Corre el extractor como subproceso y retorna (exitoso, period, weekly_period)."""
    cmd = [
        sys.executable,
        "-m", "extractor.extractor",
        "--client", args.client,
        "--database", args.database,
        "--server", args.server,
    ]
    if args.period:
        cmd += ["--period", args.period]
    if args.tables:
        cmd += ["--tables"] + args.tables
    if args.retention_monthly:
        cmd += ["--retention-monthly", str(args.retention_monthly)]
    if args.retention_weekly:
        cmd += ["--retention-weekly", str(args.retention_weekly)]

    logger.info("Ejecutando extractor...")
    logger.info("Comando: {}".format(" ".join(cmd)))

    # capture_output no existe en Python 3.6, se omite para que
    # stdout/stderr salgan directamente a la consola
    result = subprocess.run(cmd)
    return result.returncode == 0


def notify_ec2(args, period, weekly_period):
    """Notifica al EC2 para que inicie la carga ETL."""
    ec2_url = os.getenv("EC2_API_URL", "").rstrip("/")
    ec2_token = os.getenv("EC2_API_TOKEN", "")

    if not ec2_url:
        logger.error("EC2_API_URL no definida en el .env. No se puede notificar al EC2.")
        return False

    payload = {
        "client": args.client,
        "monthly_period": period,
        "weekly_period": weekly_period,
    }
    headers = {}
    if ec2_token:
        headers["Authorization"] = "Bearer {}".format(ec2_token)

    logger.info("Notificando EC2: {} /trigger".format(ec2_url))
    try:
        response = requests.post(
            "{}/trigger".format(ec2_url),
            json=payload,
            headers=headers,
            timeout=30,
        )
        if response.status_code == 200:
            data = response.json()
            logger.info("EC2 aceptó el trigger. Job ID: {}".format(data.get("job_id", "N/A")))
            logger.info("Usa GET {}/status/{} para ver el progreso.".format(
                ec2_url, data.get("job_id", "")
            ))
            return True
        else:
            logger.error("EC2 respondio con status {}: {}".format(
                response.status_code, response.text
            ))
            return False
    except requests.exceptions.ConnectionError:
        logger.error("No se pudo conectar al EC2 en {}".format(ec2_url))
        return False
    except Exception as e:
        logger.error("Error notificando EC2: {}".format(e))
        return False


def parse_args():
    parser = argparse.ArgumentParser(
        description="Pipeline Windows: extrae de SQL Server, sube a S3, notifica EC2"
    )
    parser.add_argument("--client", required=True, help="Nombre del cliente. Ej: NOVO")
    parser.add_argument("--database", required=True, help="Base de datos SQL Server. Ej: DB_NOVO")
    parser.add_argument("--server", default=os.getenv("SQL_SERVER", "localhost"))
    parser.add_argument("--period", default=None, help="Periodo mensual manual. Ej: 202601")
    parser.add_argument("--tables", nargs="+", default=None)
    parser.add_argument("--retention-monthly", type=int, default=0, metavar="N")
    parser.add_argument("--retention-weekly", type=int, default=0, metavar="N")
    parser.add_argument(
        "--no-notify",
        action="store_true",
        help="Solo extrae a S3, no notifica al EC2",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    logger.info("=" * 70)
    logger.info("PIPELINE WINDOWS — SQL Server -> S3 -> EC2")
    logger.info("=" * 70)
    logger.info("Cliente  : {}".format(args.client))
    logger.info("Database : {}".format(args.database))
    logger.info("Notificar EC2: {}".format(not args.no_notify))
    logger.info("=" * 70)

    # Paso 1: Extraccion
    success = run_extractor(args)
    if not success:
        logger.error("La extraccion fallo. Abortando pipeline.")
        sys.exit(1)

    logger.info("Extraccion completada exitosamente.")

    if args.no_notify:
        logger.info("--no-notify activo. Pipeline finalizado.")
        return

    # Paso 2: Detectar periodos para notificar EC2
    # Los periodos los detecta el extractor; los leemos del log o los re-detectamos.
    # Para simplicidad, se pasan via env o se re-detectan en EC2 desde S3.
    # El EC2 recibe client + optional periods y puede auto-detectar desde S3.
    period = args.period or "auto"
    weekly_period = "auto"

    # Paso 3: Notificar EC2
    notified = notify_ec2(args, period, weekly_period)
    if not notified:
        logger.warning("No se pudo notificar al EC2. Los datos ya estan en S3.")
        logger.warning("Puedes iniciar la carga manualmente desde el EC2.")
        sys.exit(1)

    logger.info("Pipeline completado exitosamente.")


if __name__ == "__main__":
    main()
