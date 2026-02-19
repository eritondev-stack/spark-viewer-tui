from __future__ import annotations

import logging
import os
import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType, DoubleType, FloatType,
    BooleanType, TimestampType, DateType, ShortType, ByteType,
)


def _suppress_spark_logs() -> None:
    """Suppress all Spark/Py4J logging before session creation."""
    for name in ("pyspark", "py4j", "py4j.java_gateway"):
        logging.getLogger(name).setLevel(logging.ERROR)
    # Configure environment variables
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ.setdefault("SPARK_LOG_LEVEL", "OFF")


class SparkManager:
    def __init__(self):
        self._session: SparkSession | None = None

    @property
    def session(self) -> SparkSession | None:
        return self._session

    @property
    def is_active(self) -> bool:
        return self._session is not None

    def start_session(self, metastore_db: str, warehouse_dir: str) -> SparkSession:
        packages = ["io.delta:delta-spark_2.12:3.1.0"]

        _suppress_spark_logs()

        # Redirect both stdout and stderr to suppress JVM log output on the TUI
        old_stdout = os.dup(1)
        old_stderr = os.dup(2)
        devnull = os.open(os.devnull, os.O_WRONLY)
        os.dup2(devnull, 1)  # Redirect stdout
        os.dup2(devnull, 2)  # Redirect stderr

        try:
            builder = (
                SparkSession.builder
                .appName("LocalDeltaPipeline")
                .master("local[*]")
                .config("spark.jars.packages", ",".join(packages))
                .config("spark.ui.showConsoleProgress", "false")
                .config(
                    "spark.driver.extraJavaOptions",
                    "-Dlog4j.logger.org.apache.spark=OFF "
                    "-Dlog4j.logger.org.apache.hadoop=OFF "
                    "-Dlog4j.rootLogger=OFF",
                )
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.sql.warehouse.dir", warehouse_dir)
                .config("spark.sql.catalogImplementation", "hive")
                .config("spark.hadoop.javax.jdo.option.ConnectionURL",
                        f"jdbc:derby:;databaseName={metastore_db};create=true")
                .enableHiveSupport()
            )

            self._session = SparkSession.getActiveSession() or builder.getOrCreate()
            self._session.sparkContext.setLogLevel("OFF")
        finally:
            os.dup2(old_stdout, 1)  # Restore stdout
            os.dup2(old_stderr, 2)  # Restore stderr
            os.close(old_stdout)
            os.close(old_stderr)
            os.close(devnull)

        return self._session

    def stop_session(self) -> None:
        if self._session:
            self._session.stop()
            self._session = None

    def list_databases(self) -> list[str]:
        return [db.name for db in self._session.catalog.listDatabases()]

    def list_tables(self, database: str) -> list[str]:
        return [t.name for t in self._session.catalog.listTables(database)]

    def scan_and_register_paths(self, scan_paths: list[dict]) -> dict[str, list[str]]:
        """Scan paths for Delta/Parquet folders and register as Spark tables.

        Each entry: {"path": "/data/...", "db_name": "my_db"}
        Drops and recreates each database on every call (folders are live).
        """
        catalog_data: dict[str, list[str]] = {}
        for entry in scan_paths:
            base_path = entry.get("path", "")
            db_name = entry.get("db_name", "")
            if not base_path or not db_name:
                continue
            if not os.path.isdir(base_path):
                continue
            # Drop and recreate — folders are live, always refresh
            self._session.sql(f"DROP DATABASE IF EXISTS `{db_name}` CASCADE")
            self._session.sql(f"CREATE DATABASE `{db_name}`")
            tables: list[str] = []
            for item in os.listdir(base_path):
                full_path = os.path.join(base_path, item)
                if not os.path.isdir(full_path):
                    continue
                # Delta: has _delta_log subdirectory
                if os.path.isdir(os.path.join(full_path, "_delta_log")):
                    self._session.sql(
                        f"CREATE TABLE `{db_name}`.`{item}` "
                        f"USING DELTA LOCATION '{full_path}'"
                    )
                    tables.append(item)
                    continue
                # Parquet: has .parquet files
                has_parquet = any(
                    f.endswith(".parquet")
                    for f in os.listdir(full_path)
                    if os.path.isfile(os.path.join(full_path, f))
                )
                if has_parquet:
                    self._session.sql(
                        f"CREATE TABLE `{db_name}`.`{item}` "
                        f"USING PARQUET LOCATION '{full_path}'"
                    )
                    tables.append(item)
            if tables:
                catalog_data[db_name] = sorted(tables)
        return catalog_data

    # ── Live tables (global temp views) ──────────────────────────────────────

    def register_live_table(
        self,
        table_name: str,
        schema: list[list[str]],
        rows: list[list[str | None]],
    ) -> None:
        """Registra um DataFrame como global temp view (sem escrita em disco).

        Acessível via ``SELECT * FROM global_temp.<table_name>``.
        """
        if not self._session:
            raise RuntimeError("Spark session não iniciada.")

        spark_schema = StructType([
            StructField(col_name, _spark_type(col_type), nullable=True)
            for col_name, col_type in schema
        ])
        typed_rows = [
            [_cast_value(val, col_type) for val, (_, col_type) in zip(row, schema)]
            for row in rows
        ]
        df = self._session.createDataFrame(typed_rows, schema=spark_schema)
        df.createOrReplaceGlobalTempView(table_name)

    def list_live_tables(self) -> list[str]:
        """Retorna os nomes das tabelas no banco global_temp (banco 'live')."""
        if not self._session:
            return []
        try:
            return [t.name for t in self._session.catalog.listTables("global_temp")]
        except Exception:
            return []

    def execute_sql(self, query: str) -> tuple[list[tuple[str, str]], list[list[str]]]:
        df: DataFrame = self._session.sql(query)
        schema = [(field.name, field.dataType.simpleString()) for field in df.schema.fields]
        rows = []
        for row in df.collect():
            rows.append([str(val) if val is not None else None for val in row])
        return schema, rows


# ── Helpers para register_live_table ─────────────────────────────────────────

_TYPE_MAP: dict[str, object] = {
    "string":    StringType(),
    "int":       IntegerType(),
    "bigint":    LongType(),
    "double":    DoubleType(),
    "float":     FloatType(),
    "boolean":   BooleanType(),
    "timestamp": TimestampType(),
    "date":      DateType(),
    "smallint":  ShortType(),
    "tinyint":   ByteType(),
}


def _spark_type(col_type: str):
    return _TYPE_MAP.get(col_type.lower(), StringType())


def _cast_value(val: str | None, col_type: str) -> object:
    """Converte string recebida do cliente para o tipo Python correspondente."""
    if val is None:
        return None
    ct = col_type.lower()
    try:
        if ct in ("int", "smallint", "tinyint"):
            return int(val)
        if ct == "bigint":
            return int(val)
        if ct in ("double", "float"):
            return float(val)
        if ct == "boolean":
            return val.lower() in ("true", "1", "yes")
    except (ValueError, TypeError):
        return None
    return val
