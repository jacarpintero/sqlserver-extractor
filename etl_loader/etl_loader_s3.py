"""
ETL Loader con soporte para streaming desde AWS S3
Soporta tablas mensuales y semanales con prefijos S3 separados.

Estructura S3 esperada:
  Mensual : s3://bucket/CLIENTE/202601/TABLA.txt
  Semanal : s3://bucket/CLIENTE/WEEKLY/20260302_20260308/TABLA.txt
"""

import csv
import os
import time
from io import StringIO
from typing import List, Optional, Sequence

import psycopg2

from etl_loader.config import (
    DB_CONFIG,
    TABLE_MAPPING,
    LOAD_ORDER,
    FOREIGN_KEY_VALIDATIONS,
    OPTIMIZATION_CONFIG,
    COPY_INTEGER_DECIMAL_FIX,
)
from etl_loader.utils import (
    setup_logging,
    get_db_connection,
    validate_file_exists,
    count_file_lines,
    get_table_row_count,
    format_duration,
    format_number,
    LoadStats,
)
from etl_loader.s3_manager import S3Manager, S3FileWrapper

# Tablas con frecuencia semanal (van al weekly_s3_prefix)
WEEKLY_TABLES = {
    "PERIODS_WEEKLY.txt",
    "OUTLETS_WEEKLY.txt",
    "DDD_WEEKLY.txt",
    "PH360_WEEKLY.txt",
}


def _parse_csv_header_row(first_line: str, delimiter: str) -> List[str]:
    """Primera fila del archivo como nombres de columna (respeta comillas CSV, quita BOM)."""
    first_line = first_line.lstrip("\ufeff")
    row = next(csv.reader(StringIO(first_line.strip()), delimiter=delimiter))
    return [c.strip() for c in row]


def _indices_for_integer_decimal_fix(columns: Sequence[str], names: Sequence[str]) -> List[int]:
    """Índices de columnas a normalizar (123.0 -> 123); nombres sin distinguir mayúsculas."""
    lower_to_idx = {c.strip().lower(): i for i, c in enumerate(columns)}
    out: List[int] = []
    for name in names:
        idx = lower_to_idx.get(name.strip().lower())
        if idx is not None and idx not in out:
            out.append(idx)
    return out


def _merge_unique_indices(*groups: Sequence[int]) -> List[int]:
    seen = set()
    merged: List[int] = []
    for group in groups:
        for i in group:
            if i not in seen:
                seen.add(i)
                merged.append(i)
    return merged


class ETLLoaderS3:
    """
    Cargador ETL con soporte para streaming desde S3.
    Maneja tablas mensuales y semanales con prefijos S3 distintos.
    """

    def __init__(
        self,
        data_folder: Optional[str] = None,
        s3_prefix: Optional[str] = None,
        weekly_s3_prefix: Optional[str] = None,
        validate_fk: bool = True,
        dry_run: bool = False,
    ):
        self.data_folder = data_folder
        self.s3_prefix = s3_prefix
        self.weekly_s3_prefix = weekly_s3_prefix
        self.validate_fk = validate_fk
        self.dry_run = dry_run
        self.logger = setup_logging()
        self.stats = LoadStats()

        if s3_prefix:
            self.mode = "S3"
            self.s3_manager = S3Manager()
            self.logger.info("Modo: Streaming desde AWS S3")
            self.logger.info("Bucket: {}".format(self.s3_manager.bucket_name))
            self.logger.info("Prefijo mensual : {}".format(s3_prefix))
            self.logger.info("Prefijo semanal : {}".format(weekly_s3_prefix or "N/A"))
        elif data_folder:
            self.mode = "LOCAL"
            self.s3_manager = None
            self.logger.info("Modo: Archivos locales")
            self.logger.info("Carpeta: {}".format(data_folder))
        else:
            raise ValueError("Debe especificar data_folder o s3_prefix")

        self.logger.info("=" * 80)
        self.logger.info("INICIANDO SISTEMA ETL DE CARGA DE DATOS")
        self.logger.info("=" * 80)
        self.logger.info("Validar FK : {}".format(validate_fk))
        self.logger.info("Modo dry-run: {}".format(dry_run))

    def _get_connection_options(self) -> str:
        if OPTIMIZATION_CONFIG["synchronous_commit"]:
            return ""
        return "-c synchronous_commit=off"

    def _get_file_path_or_key(self, file_name: str) -> str:
        """Obtiene la ruta local o S3 key segun el modo y tipo de tabla (monthly/weekly)."""
        if self.mode == "LOCAL":
            return os.path.join(self.data_folder, file_name)
        else:
            if file_name in WEEKLY_TABLES and self.weekly_s3_prefix:
                prefix = self.weekly_s3_prefix.rstrip("/") + "/"
            else:
                prefix = self.s3_prefix.rstrip("/") + "/"
            return "{}{}".format(prefix, file_name)

    def _file_exists(self, file_path_or_key: str) -> bool:
        if self.mode == "LOCAL":
            return validate_file_exists(file_path_or_key, self.logger)
        else:
            exists = self.s3_manager.file_exists(file_path_or_key)
            if not exists:
                self.logger.error("Archivo no encontrado en S3: {}".format(file_path_or_key))
            return exists

    def _count_lines(self, file_path_or_key: str) -> int:
        if self.mode == "LOCAL":
            return count_file_lines(file_path_or_key)
        else:
            return self.s3_manager.count_file_lines(file_path_or_key)

    def _open_file(self, file_path_or_key: str, encoding: str = "latin1"):
        if self.mode == "LOCAL":
            return open(file_path_or_key, "r", encoding=encoding, errors="replace")
        else:
            return S3FileWrapper(self.s3_manager, file_path_or_key, encoding)

    def upload_table(
        self,
        table_name: str,
        file_path_or_key: str,
        delimiter: str = ";",
    ):
        """
        Carga datos desde archivo local o S3 a tabla PostgreSQL.
        Retorna (exito, filas_cargadas, duracion).
        """
        start_time = time.time()
        rows_loaded = 0

        try:
            if not self._file_exists(file_path_or_key):
                return False, 0, 0.0

            file_lines = self._count_lines(file_path_or_key)
            self.logger.info("Archivo tiene {} lineas".format(format_number(file_lines)))

            if self.dry_run:
                self.logger.info("[DRY RUN] Saltando carga de {}".format(table_name))
                return True, file_lines, 0.0

            options = self._get_connection_options()
            conn = psycopg2.connect(**DB_CONFIG, options=options) if options else get_db_connection()
            cur = conn.cursor()

            if OPTIMIZATION_CONFIG["disable_triggers"]:
                self.logger.info("Desactivando triggers en {}".format(table_name))
                cur.execute("ALTER TABLE {} DISABLE TRIGGER ALL;".format(table_name))

            truncate_mode = OPTIMIZATION_CONFIG.get("truncate_mode", "safe")

            if truncate_mode == "skip":
                self.logger.info("Saltando truncate de {}".format(table_name))
            elif truncate_mode == "delete":
                self.logger.info("Vaciando tabla {} (DELETE)...".format(table_name))
                cur.execute("DELETE FROM public.{};".format(table_name))
            elif truncate_mode == "cascade":
                self.logger.info("Vaciando tabla {} (CASCADE)...".format(table_name))
                cur.execute("TRUNCATE TABLE public.{} RESTART IDENTITY CASCADE;".format(table_name))
            else:
                self.logger.info("Vaciando tabla {} (SAFE)...".format(table_name))
                cur.execute("TRUNCATE TABLE public.{} RESTART IDENTITY;".format(table_name))

            conn.commit()

            self.logger.info("Cargando datos en {}...".format(table_name))

            with self._open_file(file_path_or_key) as f:
                header_line = f.readline()
                if not header_line:
                    self.logger.error("Archivo vacio: {}".format(file_path_or_key))
                    return False, 0, time.time() - start_time
                columns = _parse_csv_header_row(header_line, delimiter)
                columns_str = ", ".join(columns)

            detail_idx: List[int] = []
            if table_name.endswith("_detail_reports"):
                detail_idx = _indices_for_integer_decimal_fix(columns, ("units", "data"))
            table_idx = _indices_for_integer_decimal_fix(
                columns, COPY_INTEGER_DECIMAL_FIX.get(table_name, ())
            )
            integer_indices = _merge_unique_indices(detail_idx, table_idx)

            needs_decimal_fix = bool(integer_indices)
            if integer_indices:
                col_names = [columns[idx] for idx in integer_indices]
                self.logger.info("Columnas INTEGER a convertir: {}".format(col_names))

            use_freeze = OPTIMIZATION_CONFIG["use_freeze"] and truncate_mode != "skip"
            freeze_option = "FREEZE true," if use_freeze else ""

            copy_query = """
                COPY {table} ({columns})
                FROM STDIN
                WITH (
                    FORMAT csv,
                    DELIMITER '{delimiter}',
                    NULL '',
                    QUOTE '"',
                    HEADER true,
                    {freeze}
                    ENCODING 'LATIN1'
                );
            """.format(
                table=table_name,
                columns=columns_str,
                delimiter=delimiter,
                freeze=freeze_option,
            )

            if needs_decimal_fix:
                self.logger.info("Aplicando conversion de decimales en streaming...")

                def convert_line_generator():
                    with self._open_file(file_path_or_key) as f_in:
                        header_raw = f_in.readline()
                        if not header_raw.endswith("\n"):
                            header_raw += "\n"
                        yield header_raw
                        line_count = 0
                        for line in f_in:
                            line_count += 1
                            if not line.strip():
                                yield line if line.endswith("\n") else line + "\n"
                                continue
                            row_text = line.rstrip("\r\n")
                            try:
                                parts = next(csv.reader(StringIO(row_text), delimiter=delimiter))
                            except StopIteration:
                                yield line if line.endswith("\n") else line + "\n"
                                continue
                            for idx in integer_indices:
                                if len(parts) > idx and parts[idx]:
                                    raw = parts[idx].strip()
                                    try:
                                        float_val = float(raw)
                                        if float_val == int(float_val):
                                            parts[idx] = str(int(float_val))
                                    except (ValueError, OverflowError):
                                        pass
                            out = StringIO()
                            w = csv.writer(
                                out,
                                delimiter=delimiter,
                                quoting=csv.QUOTE_MINIMAL,
                                lineterminator="\n",
                            )
                            w.writerow(parts)
                            yield out.getvalue()
                            if line_count % 100000 == 0:
                                self.logger.info("Procesando linea {:,}...".format(line_count))

                class GeneratorFile:
                    def __init__(self, generator):
                        self.generator = generator
                        self.buffer = ""

                    def read(self, size=-1):
                        while len(self.buffer) < size or size == -1:
                            try:
                                self.buffer += next(self.generator)
                                if size == -1:
                                    continue
                            except StopIteration:
                                break
                        if size == -1:
                            result, self.buffer = self.buffer, ""
                        else:
                            result, self.buffer = self.buffer[:size], self.buffer[size:]
                        return result

                    def readline(self):
                        while "\n" not in self.buffer:
                            try:
                                self.buffer += next(self.generator)
                            except StopIteration:
                                break
                        if "\n" in self.buffer:
                            line, self.buffer = self.buffer.split("\n", 1)
                            return line + "\n"
                        result, self.buffer = self.buffer, ""
                        return result

                cur.copy_expert(copy_query, GeneratorFile(convert_line_generator()))
            else:
                with self._open_file(file_path_or_key) as f:
                    cur.copy_expert(copy_query, f)

            self.logger.info("Actualizando secuencias de {}...".format(table_name))
            cur.execute("""
                SELECT setval(
                    pg_get_serial_sequence('{table}', column_name),
                    (SELECT MAX({pk}) FROM {table})
                )
                FROM information_schema.columns
                WHERE table_name = '{table}'
                AND column_default LIKE 'nextval%%'
                AND column_name = '{pk}';
            """.format(table=table_name, pk=columns[0]))

            if OPTIMIZATION_CONFIG["disable_triggers"]:
                self.logger.info("Reactivando triggers en {}".format(table_name))
                cur.execute("ALTER TABLE {} ENABLE TRIGGER ALL;".format(table_name))

            if OPTIMIZATION_CONFIG["run_analyze"]:
                self.logger.info("Actualizando estadisticas de {}".format(table_name))
                cur.execute("ANALYZE {};".format(table_name))

            conn.commit()

            rows_loaded = get_table_row_count(table_name, self.logger)
            duration = time.time() - start_time
            self.logger.info(
                "OK {} completado: {} filas en {}".format(
                    table_name, format_number(rows_loaded), format_duration(duration)
                )
            )
            return True, rows_loaded, duration

        except Exception as e:
            duration = time.time() - start_time
            self.logger.error("ERROR cargando {}: {}".format(table_name, e))
            if "conn" in locals():
                conn.rollback()
            return False, 0, duration
        finally:
            if "cur" in locals():
                cur.close()
            if "conn" in locals():
                conn.close()

    def truncate_all_tables(self) -> bool:
        self.logger.info("\n" + "=" * 80)
        self.logger.info("TRUNCANDO TODAS LAS TABLAS (orden inverso)")
        self.logger.info("=" * 80)

        try:
            conn = get_db_connection()
            cur = conn.cursor()

            if OPTIMIZATION_CONFIG["disable_triggers"]:
                self.logger.info("Desactivando triggers en todas las tablas...")
                for file_name in LOAD_ORDER:
                    if file_name in TABLE_MAPPING:
                        table_name = TABLE_MAPPING[file_name]["table"]
                        cur.execute("ALTER TABLE {} DISABLE TRIGGER ALL;".format(table_name))

            table_list = [
                "public.{}".format(TABLE_MAPPING[f]["table"])
                for f in LOAD_ORDER
                if f in TABLE_MAPPING
            ]
            self.logger.info("Truncando {} tablas...".format(len(table_list)))
            cur.execute("TRUNCATE TABLE {} RESTART IDENTITY CASCADE;".format(", ".join(table_list)))
            conn.commit()

            if OPTIMIZATION_CONFIG["disable_triggers"]:
                self.logger.info("Reactivando triggers...")
                for file_name in LOAD_ORDER:
                    if file_name in TABLE_MAPPING:
                        table_name = TABLE_MAPPING[file_name]["table"]
                        cur.execute("ALTER TABLE {} ENABLE TRIGGER ALL;".format(table_name))
                conn.commit()

            self.logger.info("OK Todas las tablas truncadas\n")
            return True

        except Exception as e:
            self.logger.error("ERROR truncando tablas: {}".format(e))
            if "conn" in locals():
                conn.rollback()
            return False
        finally:
            if "cur" in locals():
                cur.close()
            if "conn" in locals():
                conn.close()

    def sync_user_laboratory_for_owners(self) -> bool:
        self.logger.info("\n" + "=" * 80)
        self.logger.info("SINCRONIZANDO USER_LABORATORY PARA LABORATORIES CON IS_OWNER")
        self.logger.info("=" * 80)

        try:
            conn = get_db_connection()
            cur = conn.cursor()

            cur.execute("SELECT id, name FROM laboratories WHERE is_owner = true;")
            owner_labs = cur.fetchall()

            if not owner_labs:
                self.logger.info("No hay laboratorios con is_owner = true.")
                return True

            self.logger.info("{} laboratorios con is_owner = true".format(len(owner_labs)))
            cur.execute("SELECT id, email FROM users;")
            all_users = cur.fetchall()

            if not all_users:
                self.logger.warning("No hay usuarios. Saltando sincronizacion.")
                return True

            lab_ids = [lab[0] for lab in owner_labs]
            placeholders = ",".join(["%s"] * len(lab_ids))
            cur.execute(
                "DELETE FROM user_laboratory WHERE laboratory_id IN ({});".format(placeholders),
                lab_ids,
            )
            conn.commit()

            total_inserted = 0
            for lab_id, lab_name in owner_labs:
                for user_id, _ in all_users:
                    cur.execute(
                        "INSERT INTO user_laboratory (user_id, laboratory_id) VALUES (%s, %s);",
                        (user_id, lab_id),
                    )
                    total_inserted += 1
                conn.commit()
                self.logger.info("Laboratorio {}: {} relaciones creadas".format(lab_name, len(all_users)))

            self.logger.info("OK Sincronizacion completada: {} registros\n".format(total_inserted))
            return True

        except Exception as e:
            self.logger.error("ERROR sincronizando user_laboratory: {}".format(e))
            if "conn" in locals():
                conn.rollback()
            return False
        finally:
            if "cur" in locals():
                cur.close()
            if "conn" in locals():
                conn.close()

    def load_all_files(self, truncate_first: bool = True) -> bool:
        self.logger.info("\n" + "=" * 80)
        self.logger.info("INICIANDO PROCESO DE CARGA")
        self.logger.info("=" * 80 + "\n")

        if truncate_first and not self.dry_run:
            if not self.truncate_all_tables():
                self.logger.error("Error truncando tablas. Abortando.")
                return False
            original_mode = OPTIMIZATION_CONFIG.get("truncate_mode", "safe")
            OPTIMIZATION_CONFIG["truncate_mode"] = "skip"

        all_success = True

        for file_name in LOAD_ORDER:
            if file_name not in TABLE_MAPPING:
                self.logger.warning("{} no esta en TABLE_MAPPING, saltando...".format(file_name))
                continue

            config = TABLE_MAPPING[file_name]
            table_name = config["table"]
            delimiter = config["delimiter"]
            description = config.get("description", "")
            file_path_or_key = self._get_file_path_or_key(file_name)

            self.logger.info("\n" + "-" * 80)
            self.logger.info("Procesando: {}".format(file_name))
            self.logger.info("Tabla: {} - {}".format(table_name, description))
            self.logger.info("Fuente: {}".format(file_path_or_key))
            self.logger.info("-" * 80)

            if not self._file_exists(file_path_or_key):
                self.logger.warning("Archivo no encontrado, saltando...")
                continue

            success, rows, duration = self.upload_table(table_name, file_path_or_key, delimiter)

            if success:
                self.stats.add_success(table_name, file_name, rows, duration)
                if file_name == "LABORATORIES.txt" and not self.dry_run:
                    self.logger.info("\nEjecutando post-procesamiento para LABORATORIES...")
                    if not self.sync_user_laboratory_for_owners():
                        self.logger.warning("Sincronizacion de user_laboratory fallo")
            else:
                self.stats.add_failure(table_name, file_name, "Error durante la carga (ver logs)")
                all_success = False

        self.logger.info(self.stats.get_summary())

        if truncate_first and not self.dry_run:
            OPTIMIZATION_CONFIG["truncate_mode"] = original_mode

        return all_success
