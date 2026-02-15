from pyspark.sql import SparkSession, DataFrame


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
        self._session = (
            SparkSession.getActiveSession()
            or SparkSession.builder
            .appName("LocalDeltaPipeline")
            .master("local[*]")
            .config("spark.jars.packages", ",".join(packages))
            .config("spark.sql.extensions",
                    "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.warehouse.dir", warehouse_dir)
            .config("spark.sql.catalogImplementation", "hive")
            .config("spark.hadoop.javax.jdo.option.ConnectionURL",
                    f"jdbc:derby:;databaseName={metastore_db};create=true")
            .enableHiveSupport()
            .getOrCreate()
        )
        return self._session

    def stop_session(self) -> None:
        if self._session:
            self._session.stop()
            self._session = None

    def list_databases(self) -> list[str]:
        return [db.name for db in self._session.catalog.listDatabases()]

    def list_tables(self, database: str) -> list[str]:
        return [t.name for t in self._session.catalog.listTables(database)]

    def execute_sql(self, query: str) -> tuple[list[tuple[str, str]], list[list[str]]]:
        df: DataFrame = self._session.sql(query)
        schema = [(field.name, field.dataType.simpleString()) for field in df.schema.fields]
        rows = []
        for row in df.collect():
            rows.append([str(val) if val is not None else "" for val in row])
        return schema, rows
