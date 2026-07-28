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
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import StringIO
from typing import List, Optional, Sequence

import psycopg2

from etl_loader.config import (
    DB_CONFIG,
    TABLE_MAPPING,
    LOAD_ORDER,
    OPTIMIZATION_CONFIG,
    COPY_INTEGER_DECIMAL_FIX,
    UPSERT_TABLES,
    DELETE_MODE_TABLES,
)
from etl_loader.utils import (
    setup_logging,
    get_db_connection,
    validate_file_exists,
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

# Tablas que se cargan en paralelo (no tienen FK entre si)
PARALLEL_TABLES = {"TS.txt", "DDD.txt", "PBS.txt", "PH360.txt"}
PARALLEL_TABLES_WEEKLY = {"DDD_WEEKLY.txt", "PH360_WEEKLY.txt"}

MAX_COPY_RETRIES = 3
RETRY_BASE_DELAY = 10  # segundos; backoff exponencial: 10s, 20s, 40s


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
        db_name = DB_CONFIG.get("database") or "(no configurada)"
        db_host = DB_CONFIG.get("host") or "(no configurado)"
        self.logger.info(
            "Destino ETL — PostgreSQL: base de datos '{}' en host '{}'".format(db_name, db_host)
        )

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

    def _read_header_line(self, file_path_or_key: str, encoding: str = "latin1") -> str:
        """Lee solo la primera línea del archivo de forma eficiente (range request en S3)."""
        if self.mode == "LOCAL":
            with open(file_path_or_key, "r", encoding=encoding, errors="replace") as f:
                return f.readline()
        return self.s3_manager.get_file_header_line(file_path_or_key, encoding)

    def _open_file(self, file_path_or_key: str, encoding: str = "latin1"):
        if self.mode == "LOCAL":
            return open(file_path_or_key, "r", encoding=encoding, errors="replace")
        else:
            return S3FileWrapper(self.s3_manager, file_path_or_key, encoding)

    def _get_file_size_mb(self, file_path_or_key: str) -> float:
        """Tamaño del archivo en MB sin descargarlo (head_object en S3, getsize en local)."""
        if self.mode == "LOCAL":
            try:
                return os.path.getsize(file_path_or_key) / (1024 * 1024)
            except OSError:
                return 0.0
        return self.s3_manager.get_file_size_bytes(file_path_or_key) / (1024 * 1024)

    def upload_table(
        self,
        table_name: str,
        file_path_or_key: str,
        delimiter: str = ";",
    ):
        """Carga con reintento automatico (hasta MAX_COPY_RETRIES intentos)."""
        pk_column = UPSERT_TABLES.get(table_name)
        if pk_column:
            self.logger.info(
                "Estrategia de carga para {}: UPSERT (tabla protegida, nunca se borra)".format(table_name)
            )
            load_once = lambda: self._upsert_table_once(table_name, file_path_or_key, delimiter, pk_column)
        else:
            self.logger.info(
                "Estrategia de carga para {}: COPY directo (ya vaciada en truncate_all_tables)".format(table_name)
            )
            load_once = lambda: self._upload_table_once(table_name, file_path_or_key, delimiter)

        for attempt in range(1, MAX_COPY_RETRIES + 1):
            success, rows, duration = load_once()
            if success:
                return success, rows, duration
            if attempt < MAX_COPY_RETRIES:
                wait = RETRY_BASE_DELAY * (2 ** (attempt - 1))
                self.logger.warning(
                    "Intento {}/{} fallido para {}. Reintentando en {}s...".format(
                        attempt, MAX_COPY_RETRIES, table_name, wait
                    )
                )
                time.sleep(wait)
        return False, 0, 0.0

    def _upsert_table_once(
        self,
        table_name: str,
        file_path_or_key: str,
        delimiter: str,
        pk_column: str,
    ):
        """
        Carga por upsert (INSERT ... ON CONFLICT DO UPDATE): nunca borra filas
        existentes. Usado para tablas de referencia (outlets, packs, laboratories,
        markets) de las que dependen tablas externas con datos vivos de usuario
        (inventories, inventory_items, user_laboratory, user_market, outlet_reports).
        Una fila que ya no venga en el extracto de SQL Server simplemente no se toca,
        en vez de borrarse y dejar huerfano el dato del usuario.
        """
        start_time = time.time()

        try:
            if not self._file_exists(file_path_or_key):
                return False, 0, 0.0

            size_mb = self._get_file_size_mb(file_path_or_key)
            self.logger.info("Archivo: {:.1f} MB".format(size_mb))

            if self.dry_run:
                self.logger.info("[DRY RUN] Saltando upsert de {}".format(table_name))
                return True, 0, 0.0

            options = self._get_connection_options()
            conn = psycopg2.connect(**DB_CONFIG, options=options) if options else get_db_connection()
            cur = conn.cursor()

            cur.execute("""
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = %s
            """, (table_name,))
            if cur.fetchone() is None:
                self.logger.warning(
                    "Tabla '{}' no existe en la BD, omitiendo...".format(table_name)
                )
                return True, 0, 0.0

            header_line = self._read_header_line(file_path_or_key)
            if not header_line:
                self.logger.error("Archivo vacio: {}".format(file_path_or_key))
                return False, 0, time.time() - start_time
            columns = _parse_csv_header_row(header_line, delimiter)
            columns_str = ", ".join(columns)

            staging_table = "staging_{}".format(table_name)
            self.logger.info("Creando tabla temporal {}...".format(staging_table))
            cur.execute(
                "CREATE TEMP TABLE {} (LIKE public.{} INCLUDING DEFAULTS) ON COMMIT DROP;".format(
                    staging_table, table_name
                )
            )

            copy_query = """
                COPY {table} ({columns})
                FROM STDIN
                WITH (
                    FORMAT csv,
                    DELIMITER '{delimiter}',
                    NULL '',
                    QUOTE '"',
                    HEADER true,
                    FREEZE true,
                    ENCODING 'LATIN1'
                );
            """.format(table=staging_table, columns=columns_str, delimiter=delimiter)

            self.logger.info("Cargando datos a tabla temporal...")
            with self._open_file(file_path_or_key) as f:
                cur.copy_expert(copy_query, f)

            update_cols = [c for c in columns if c.lower() != pk_column.lower()]
            set_clause = ", ".join("{c} = EXCLUDED.{c}".format(c=c) for c in update_cols)

            self.logger.info("Aplicando upsert en {} (INSERT ... ON CONFLICT)...".format(table_name))
            upsert_query = """
                INSERT INTO public.{table} ({columns})
                SELECT {columns} FROM {staging}
                ON CONFLICT ({pk}) DO UPDATE SET {set_clause};
            """.format(
                table=table_name,
                columns=columns_str,
                staging=staging_table,
                pk=pk_column,
                set_clause=set_clause,
            )
            cur.execute(upsert_query)

            self.logger.info("Actualizando secuencia de {}...".format(table_name))
            cur.execute("""
                SELECT setval(
                    pg_get_serial_sequence('{table}', column_name),
                    (SELECT MAX({pk}) FROM {table})
                )
                FROM information_schema.columns
                WHERE table_name = '{table}'
                AND column_default LIKE 'nextval%%'
                AND column_name = '{pk}';
            """.format(table=table_name, pk=pk_column))

            if OPTIMIZATION_CONFIG["run_analyze"]:
                self.logger.info("Actualizando estadisticas de {}".format(table_name))
                cur.execute("ANALYZE {};".format(table_name))

            conn.commit()

            rows_loaded = get_table_row_count(table_name, self.logger)
            duration = time.time() - start_time
            self.logger.info(
                "OK {} completado (upsert): {} filas en {}".format(
                    table_name, format_number(rows_loaded), format_duration(duration)
                )
            )
            return True, rows_loaded, duration

        except Exception as e:
            duration = time.time() - start_time
            self.logger.error("ERROR cargando (upsert) {}: {}".format(table_name, e))
            if "conn" in locals():
                conn.rollback()
            return False, 0, duration
        finally:
            if "cur" in locals():
                cur.close()
            if "conn" in locals():
                conn.close()

    def _upload_table_once(
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

            size_mb = self._get_file_size_mb(file_path_or_key)
            self.logger.info("Archivo: {:.1f} MB".format(size_mb))

            if self.dry_run:
                self.logger.info("[DRY RUN] Saltando carga de {}".format(table_name))
                return True, 0, 0.0

            options = self._get_connection_options()
            conn = psycopg2.connect(**DB_CONFIG, options=options) if options else get_db_connection()
            cur = conn.cursor()

            # Verificar que la tabla existe en la BD antes de intentar cargar
            cur.execute("""
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = %s
            """, (table_name,))
            if cur.fetchone() is None:
                self.logger.warning(
                    "Tabla '{}' no existe en la BD, omitiendo...".format(table_name)
                )
                return True, 0, 0.0

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

            header_line = self._read_header_line(file_path_or_key)
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
                        f_in.readline()  # avanzar pasado el header (ya leído vía range request)
                        header_raw = header_line if header_line.endswith("\n") else header_line + "\n"
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
                        self._parts = []  # lista de strings; join es O(n) vs O(n²) de concatenar

                    def read(self, size=-1):
                        if size == -1:
                            for chunk in self.generator:
                                self._parts.append(chunk)
                            result = "".join(self._parts)
                            self._parts = []
                            return result
                        while sum(len(p) for p in self._parts) < size:
                            try:
                                self._parts.append(next(self.generator))
                            except StopIteration:
                                break
                        combined = "".join(self._parts)
                        result, remainder = combined[:size], combined[size:]
                        self._parts = [remainder] if remainder else []
                        return result

                    def readline(self):
                        while True:
                            combined = "".join(self._parts)
                            if "\n" in combined:
                                line, remainder = combined.split("\n", 1)
                                self._parts = [remainder] if remainder else []
                                return line + "\n"
                            try:
                                self._parts.append(next(self.generator))
                            except StopIteration:
                                break
                        result = "".join(self._parts)
                        self._parts = []
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

    def _get_existing_tables(self, cur) -> set:
        """Retorna el conjunto de tablas que realmente existen en PostgreSQL."""
        cur.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
        """)
        return {row[0] for row in cur.fetchall()}

    def truncate_all_tables(self) -> bool:
        self.logger.info("\n" + "=" * 80)
        self.logger.info("TRUNCANDO TODAS LAS TABLAS")
        self.logger.info("=" * 80)

        phase_start = time.time()

        try:
            conn = get_db_connection()
            cur = conn.cursor()

            existing = self._get_existing_tables(cur)

            # Filtrar solo las tablas que existen en la BD.
            # Las tablas en UPSERT_TABLES (outlets, packs, laboratories, markets) se
            # excluyen: se cargan por upsert y nunca se truncan (ver upload_table).
            tables_to_truncate = []
            for file_name in LOAD_ORDER:
                if file_name in TABLE_MAPPING:
                    table_name = TABLE_MAPPING[file_name]["table"]
                    if table_name in UPSERT_TABLES:
                        continue
                    if table_name in existing:
                        tables_to_truncate.append(table_name)
                    else:
                        self.logger.warning(
                            "Tabla '{}' no existe en la BD, omitiendo...".format(table_name)
                        )

            if not tables_to_truncate:
                self.logger.warning("No hay tablas para truncar.")
                return True

            # DELETE_MODE_TABLES (products, corporations) son padres de una tabla
            # protegida (packs, laboratories) y no se pueden truncar sin incluir esa
            # tabla protegida en el mismo TRUNCATE. El resto no tiene ese problema y
            # usa TRUNCATE: es una operacion de metadatos (milisegundos, sin importar
            # si la tabla tiene 0 o 12 millones de filas) y libera el espacio en disco
            # al SO de inmediato, sin necesitar VACUUM FULL despues.
            truncate_group = [t for t in tables_to_truncate if t not in DELETE_MODE_TABLES]
            delete_group = [t for t in tables_to_truncate if t in DELETE_MODE_TABLES]

            self.logger.info(
                "Via TRUNCATE ({} tablas): {}".format(
                    len(truncate_group), ", ".join(truncate_group) or "ninguna"
                )
            )
            self.logger.info(
                "Via DELETE+VACUUM FULL ({} tablas, referenciadas por tablas protegidas): {}".format(
                    len(delete_group), ", ".join(delete_group) or "ninguna"
                )
            )

            if truncate_group:
                step_start = time.time()
                table_list = ", ".join("public.{}".format(t) for t in truncate_group)
                cur.execute("TRUNCATE TABLE {} RESTART IDENTITY;".format(table_list))
                conn.commit()
                self.logger.info(
                    "OK TRUNCATE de {} tablas en {}".format(
                        len(truncate_group), format_duration(time.time() - step_start)
                    )
                )

            if delete_group:
                step_start = time.time()

                # Desactivar triggers para que el DELETE no falle por el FK interno
                # hacia la tabla protegida (packs/laboratories) que no se toca.
                for table_name in delete_group:
                    cur.execute("ALTER TABLE public.{} DISABLE TRIGGER ALL;".format(table_name))

                for table_name in reversed(delete_group):
                    cur.execute("DELETE FROM public.{};".format(table_name))

                # Reset de secuencias (equivalente al RESTART IDENTITY del TRUNCATE)
                cur.execute("""
                    SELECT seq.relname
                    FROM pg_class seq
                    JOIN pg_depend d ON d.objid = seq.oid AND d.deptype = 'a'
                    JOIN pg_class tbl ON tbl.oid = d.refobjid
                    WHERE seq.relkind = 'S' AND tbl.relname = ANY(%s)
                """, (delete_group,))
                for (seq_name,) in cur.fetchall():
                    cur.execute("SELECT setval(%s, 1, false);", (seq_name,))

                for table_name in delete_group:
                    cur.execute("ALTER TABLE public.{} ENABLE TRIGGER ALL;".format(table_name))

                conn.commit()
                self.logger.info(
                    "OK DELETE de {} tablas en {}".format(
                        len(delete_group), format_duration(time.time() - step_start)
                    )
                )

            cur.close()
            conn.close()

            # VACUUM FULL solo para el grupo DELETE: en una tabla ya vacia es rapido
            # (reescribe un archivo practicamente vacio) y libera el espacio al SO,
            # cosa que un VACUUM sin FULL no hace (solo marca paginas reutilizables
            # dentro de PostgreSQL). El grupo TRUNCATE ya libero el espacio solo.
            if delete_group:
                step_start = time.time()
                self.logger.info("Ejecutando VACUUM FULL sobre el grupo DELETE...")
                vacuum_conn = get_db_connection()
                vacuum_conn.autocommit = True
                vacuum_cur = vacuum_conn.cursor()
                try:
                    for table_name in delete_group:
                        self.logger.info("  VACUUM FULL {}...".format(table_name))
                        vacuum_cur.execute("VACUUM FULL {};".format(table_name))
                    self.logger.info(
                        "OK VACUUM FULL completado en {}".format(
                            format_duration(time.time() - step_start)
                        )
                    )
                finally:
                    vacuum_cur.close()
                    vacuum_conn.close()

            self.logger.info(
                "OK Todas las tablas truncadas en {}\n".format(
                    format_duration(time.time() - phase_start)
                )
            )
            return True

        except Exception as e:
            self.logger.error("ERROR truncando tablas: {}".format(e))
            if "conn" in locals():
                try:
                    conn.rollback()
                except Exception:
                    pass
            return False
        finally:
            if "cur" in locals():
                try:
                    cur.close()
                except Exception:
                    pass
            if "conn" in locals():
                try:
                    conn.close()
                except Exception:
                    pass

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

    def _load_table_task(self, file_name: str):
        """
        Carga una tabla individual. Retorna (file_name, success, rows, duration).
        success=None significa que el archivo no existia (skip, no es un error).

        Todo el cuerpo esta protegido por un try/except generico: en las fases
        paralelas (ThreadPoolExecutor), una excepcion sin atrapar aqui (ej. un
        error de red que boto3 no envuelve como ClientError, ver _file_exists)
        se propagaria hasta future.result() en load_all_files() y abortaria todo
        el pipeline, perdiendo el resultado de las tablas hermanas que ya habian
        terminado bien en paralelo. Igual que upload_table, cualquier fallo
        inesperado se convierte en un resultado fallido de esta tabla puntual.
        """
        start_time = time.time()
        try:
            if file_name not in TABLE_MAPPING:
                self.logger.warning("{} no esta en TABLE_MAPPING, saltando...".format(file_name))
                return file_name, None, 0, 0.0

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
                return file_name, None, 0, 0.0

            success, rows, duration = self.upload_table(table_name, file_path_or_key, delimiter)
            return file_name, success, rows, duration

        except Exception as e:
            self.logger.error(
                "ERROR inesperado procesando {}: {}".format(file_name, e), exc_info=True
            )
            return file_name, False, 0, time.time() - start_time

    def _process_task_result(self, file_name, success, rows, duration):
        """Aplica el resultado de _load_table_task al stats y retorna True si fue exitoso."""
        if success is None:
            return True  # archivo no encontrado, no es fallo

        table_name = TABLE_MAPPING[file_name]["table"]
        if success:
            mode = "UPSERT" if table_name in UPSERT_TABLES else "COPY"
            self.stats.add_success(table_name, file_name, rows, duration, mode)
            if file_name == "LABORATORIES.txt" and not self.dry_run:
                self.logger.info("\nEjecutando post-procesamiento para LABORATORIES...")
                if not self.sync_user_laboratory_for_owners():
                    self.logger.warning("Sincronizacion de user_laboratory fallo")
            return True
        else:
            self.stats.add_failure(table_name, file_name, "Error durante la carga (ver logs)")
            return False

    def load_all_files(self, truncate_first: bool = True) -> bool:
        wall_start = time.time()

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

        sequential = [f for f in LOAD_ORDER if f not in PARALLEL_TABLES and f not in PARALLEL_TABLES_WEEKLY]
        parallel_monthly = [f for f in LOAD_ORDER if f in PARALLEL_TABLES]
        parallel_weekly = [f for f in LOAD_ORDER if f in PARALLEL_TABLES_WEEKLY]

        # --- Fase 1: tablas padre y de referencia (orden secuencial respeta FK) ---
        for file_name in sequential:
            fn, success, rows, duration = self._load_table_task(file_name)
            if not self._process_task_result(fn, success, rows, duration):
                all_success = False

        # --- Fase 2: detail reports mensuales en paralelo (sin FK entre si) ---
        if parallel_monthly:
            self.logger.info("\n" + "=" * 80)
            self.logger.info("CARGANDO DETAIL REPORTS EN PARALELO ({} workers)".format(len(parallel_monthly)))
            self.logger.info("=" * 80)
            with ThreadPoolExecutor(max_workers=len(parallel_monthly)) as executor:
                futures = {executor.submit(self._load_table_task, f): f for f in parallel_monthly}
                for future in as_completed(futures):
                    fn, success, rows, duration = future.result()
                    if not self._process_task_result(fn, success, rows, duration):
                        all_success = False

        # --- Fase 3: weekly detail reports en paralelo ---
        if parallel_weekly:
            self.logger.info("\n" + "=" * 80)
            self.logger.info("CARGANDO WEEKLY DETAIL REPORTS EN PARALELO ({} workers)".format(len(parallel_weekly)))
            self.logger.info("=" * 80)
            with ThreadPoolExecutor(max_workers=len(parallel_weekly)) as executor:
                futures = {executor.submit(self._load_table_task, f): f for f in parallel_weekly}
                for future in as_completed(futures):
                    fn, success, rows, duration = future.result()
                    if not self._process_task_result(fn, success, rows, duration):
                        all_success = False

        self.stats.wall_clock_total = time.time() - wall_start
        self.logger.info(self.stats.get_summary())

        if truncate_first and not self.dry_run:
            OPTIMIZATION_CONFIG["truncate_mode"] = original_mode

        return all_success
