"""
Utilidades para el sistema ETL
"""

import os
import sys
import logging
import psycopg2
from io import StringIO, BytesIO
from typing import Optional, Union, Tuple
from etl_loader.config import DB_CONFIG, LOG_CONFIG


def setup_logging() -> logging.Logger:
    """Configura el sistema de logging"""
    logger = logging.getLogger('ETL')
    logger.setLevel(getattr(logging, LOG_CONFIG['log_level']))

    # Evitar duplicar handlers
    if logger.handlers:
        return logger

    # Handler para archivo
    file_handler = logging.FileHandler(LOG_CONFIG['log_file'])
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    # Handler para consola
    if LOG_CONFIG['console_output']:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter('%(message)s')
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

    return logger


def get_db_connection():
    """Obtiene una conexión a la base de datos"""
    return psycopg2.connect(**DB_CONFIG)


def validate_file_exists(file_path: str, logger: logging.Logger) -> bool:
    if not os.path.exists(file_path):
        logger.error(f"Archivo no encontrado: {file_path}")
        return False
    return True


def count_file_lines(file_path: str) -> int:
    try:
        with open(file_path, 'r', encoding='latin1', errors='replace') as f:
            return sum(1 for _ in f)
    except Exception:
        return 0


def count_stream_lines(stream) -> int:
    try:
        content = stream.read()
        if isinstance(content, bytes):
            content = content.decode('latin1', errors='replace')
        return len(content.splitlines())
    except Exception:
        return 0


def validate_foreign_keys(
    file_path: str,
    file_column: str,
    ref_table: str,
    ref_column: str,
    delimiter: str,
    logger: logging.Logger
) -> Tuple[bool, set, set]:
    try:
        file_ids = set()
        with open(file_path, 'r', encoding='latin1', errors='replace') as f:
            header = f.readline().strip().split(delimiter)
            if file_column not in header:
                logger.error(f"Columna {file_column} no encontrada en archivo")
                return False, set(), set()

            col_idx = header.index(file_column)
            for line in f:
                values = line.strip().split(delimiter)
                if len(values) > col_idx and values[col_idx]:
                    file_ids.add(values[col_idx])

        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(f"SELECT {ref_column} FROM {ref_table};")
        db_ids = {str(row[0]) for row in cur.fetchall()}
        cur.close()
        conn.close()

        missing_ids = file_ids - db_ids

        if missing_ids:
            logger.warning(
                f"IDs faltantes en {ref_table}.{ref_column}: "
                f"{len(missing_ids)} de {len(file_ids)}"
            )
            if len(missing_ids) <= 10:
                logger.warning(f"IDs faltantes: {missing_ids}")
            return False, file_ids, missing_ids

        return True, file_ids, set()

    except Exception as e:
        logger.error(f"Error validando foreign keys: {e}")
        return False, set(), set()


def get_table_row_count(table_name: str, logger: logging.Logger) -> int:
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {table_name};")
        count = cur.fetchone()[0]
        cur.close()
        conn.close()
        return count
    except Exception as e:
        logger.error(f"Error obteniendo conteo de {table_name}: {e}")
        return 0


def format_duration(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.2f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = seconds % 60
        return f"{minutes}m {secs:.0f}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours}h {minutes}m"


def format_number(number: int) -> str:
    return f"{number:,}"


class LoadStats:
    """Clase para trackear estadísticas de carga"""

    def __init__(self):
        self.successes = []
        self.failures = []
        self.total_rows = 0
        self.total_duration = 0.0

    def add_success(self, table: str, file: str, rows: int, duration: float):
        self.successes.append({
            'table': table,
            'file': file,
            'rows': rows,
            'duration': duration
        })
        self.total_rows += rows
        self.total_duration += duration

    def add_failure(self, table: str, file: str, error: str):
        self.failures.append({
            'table': table,
            'file': file,
            'error': error
        })

    def get_summary(self) -> str:
        lines = [
            "\n" + "=" * 80,
            " RESUMEN DE CARGA",
            "=" * 80,
            f"\nExitosas: {len(self.successes)}",
            f"Fallidas: {len(self.failures)}",
            f"Total filas: {format_number(self.total_rows)}",
            f"Duracion total: {format_duration(self.total_duration)}",
        ]

        if self.successes:
            lines.append("\nTablas cargadas exitosamente:")
            for s in self.successes:
                lines.append(
                    f"  OK {s['table']:<30} {format_number(s['rows']):>10} filas "
                    f"en {format_duration(s['duration'])}"
                )

        if self.failures:
            lines.append("\nTablas con errores:")
            for f in self.failures:
                lines.append(f"  ERROR {f['table']:<30} {f['error']}")

        lines.append("=" * 80 + "\n")
        return "\n".join(lines)
