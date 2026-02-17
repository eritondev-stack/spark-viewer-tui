"""
Seed script: creates 6 Delta tables with 500 rows each.

Usage:
    uv run python seed.py --metastore-db ./metastore_db --warehouse-dir ./spark-warehouse
"""
import argparse
import sys

from .config import load_config
from .spark_manager import SparkManager


def create_tables(spark_mgr: SparkManager) -> None:
    session = spark_mgr.session

    tables_ddl = {
        "employees": """
            CREATE TABLE IF NOT EXISTS default.employees (
                id INT, name STRING, email STRING, department STRING,
                salary DOUBLE, hire_date DATE, is_active BOOLEAN
            ) USING DELTA
        """,
        "products": """
            CREATE TABLE IF NOT EXISTS default.products (
                id INT, name STRING, category STRING, price DECIMAL(10,2),
                stock INT, weight FLOAT, created_at TIMESTAMP
            ) USING DELTA
        """,
        "orders": """
            CREATE TABLE IF NOT EXISTS default.orders (
                id INT, customer_id INT, product_id INT, quantity INT,
                total DECIMAL(10,2), status STRING, order_date TIMESTAMP
            ) USING DELTA
        """,
        "customers": """
            CREATE TABLE IF NOT EXISTS default.customers (
                id INT, first_name STRING, last_name STRING, email STRING,
                phone STRING, city STRING, country STRING, registered_at DATE
            ) USING DELTA
        """,
        "logs": """
            CREATE TABLE IF NOT EXISTS default.logs (
                id BIGINT, level STRING, message STRING, source STRING,
                log_timestamp TIMESTAMP, duration_ms INT
            ) USING DELTA
        """,
        "metrics": """
            CREATE TABLE IF NOT EXISTS default.metrics (
                id INT, metric_name STRING, value DOUBLE, unit STRING,
                dimension STRING, recorded_at TIMESTAMP, is_anomaly BOOLEAN
            ) USING DELTA
        """,
    }

    tables_insert = {
        "employees": """
            INSERT INTO default.employees
            SELECT
                cast(id as INT),
                concat('Employee_', cast(id as STRING)),
                concat('emp', cast(id as STRING), '@company.com'),
                CASE
                    WHEN rand() < 0.25 THEN 'Engineering'
                    WHEN rand() < 0.5 THEN 'Marketing'
                    WHEN rand() < 0.75 THEN 'Sales'
                    ELSE 'HR'
                END,
                round(rand() * 50000 + 30000, 2),
                date_add('2020-01-01', cast(rand() * 1500 as INT)),
                CASE WHEN rand() > 0.2 THEN true ELSE false END
            FROM range(1, 501)
        """,
        "products": """
            INSERT INTO default.products
            SELECT
                cast(id as INT),
                concat('Product_', cast(id as STRING)),
                CASE
                    WHEN rand() < 0.33 THEN 'Electronics'
                    WHEN rand() < 0.66 THEN 'Clothing'
                    ELSE 'Home'
                END,
                cast(round(rand() * 500 + 10, 2) as DECIMAL(10,2)),
                cast(rand() * 1000 as INT),
                cast(round(rand() * 50, 2) as FLOAT),
                cast(date_add('2023-01-01', cast(rand() * 365 as INT)) as TIMESTAMP)
            FROM range(1, 501)
        """,
        "orders": """
            INSERT INTO default.orders
            SELECT
                cast(id as INT),
                cast(rand() * 500 + 1 as INT),
                cast(rand() * 500 + 1 as INT),
                cast(rand() * 10 + 1 as INT),
                cast(round(rand() * 2000 + 50, 2) as DECIMAL(10,2)),
                CASE
                    WHEN rand() < 0.3 THEN 'pending'
                    WHEN rand() < 0.6 THEN 'shipped'
                    WHEN rand() < 0.9 THEN 'delivered'
                    ELSE 'cancelled'
                END,
                cast(date_add('2024-01-01', cast(rand() * 365 as INT)) as TIMESTAMP)
            FROM range(1, 501)
        """,
        "customers": """
            INSERT INTO default.customers
            SELECT
                cast(id as INT),
                concat('First_', cast(id as STRING)),
                concat('Last_', cast(id as STRING)),
                concat('customer', cast(id as STRING), '@mail.com'),
                concat('+55 11 9', cast(cast(rand() * 9000 + 1000 as INT) as STRING), '-', cast(cast(rand() * 9000 + 1000 as INT) as STRING)),
                CASE
                    WHEN rand() < 0.2 THEN 'Sao Paulo'
                    WHEN rand() < 0.4 THEN 'Rio de Janeiro'
                    WHEN rand() < 0.6 THEN 'Belo Horizonte'
                    WHEN rand() < 0.8 THEN 'Curitiba'
                    ELSE 'Salvador'
                END,
                'Brasil',
                date_add('2020-01-01', cast(rand() * 1500 as INT))
            FROM range(1, 501)
        """,
        "logs": """
            INSERT INTO default.logs
            SELECT
                cast(id as BIGINT),
                CASE
                    WHEN rand() < 0.4 THEN 'INFO'
                    WHEN rand() < 0.7 THEN 'WARN'
                    WHEN rand() < 0.9 THEN 'ERROR'
                    ELSE 'DEBUG'
                END,
                concat('Log message number ', cast(id as STRING)),
                CASE
                    WHEN rand() < 0.33 THEN 'api-server'
                    WHEN rand() < 0.66 THEN 'worker'
                    ELSE 'scheduler'
                END,
                cast(date_add('2024-06-01', cast(rand() * 180 as INT)) as TIMESTAMP),
                cast(rand() * 5000 as INT)
            FROM range(1, 501)
        """,
        "metrics": """
            INSERT INTO default.metrics
            SELECT
                cast(id as INT),
                CASE
                    WHEN rand() < 0.25 THEN 'cpu_usage'
                    WHEN rand() < 0.5 THEN 'memory_usage'
                    WHEN rand() < 0.75 THEN 'disk_io'
                    ELSE 'network_throughput'
                END,
                round(rand() * 100, 2),
                CASE
                    WHEN rand() < 0.5 THEN 'percent'
                    ELSE 'mbps'
                END,
                CASE
                    WHEN rand() < 0.33 THEN 'production'
                    WHEN rand() < 0.66 THEN 'staging'
                    ELSE 'development'
                END,
                cast(date_add('2024-06-01', cast(rand() * 180 as INT)) as TIMESTAMP),
                CASE WHEN rand() > 0.9 THEN true ELSE false END
            FROM range(1, 501)
        """,
    }

    for table_name, ddl in tables_ddl.items():
        print(f"Creating table {table_name}...")
        session.sql(ddl)

    for table_name, insert_sql in tables_insert.items():
        print(f"Inserting 500 rows into {table_name}...")
        session.sql(insert_sql)

    print("\nSeed complete! 6 tables created with 500 rows each.")


def main():
    parser = argparse.ArgumentParser(description="Seed Delta tables for Spark TUI")
    parser.add_argument("--metastore-db", help="Path to metastore_db")
    parser.add_argument("--warehouse-dir", help="Path to warehouse directory")
    args = parser.parse_args()

    config = load_config()
    metastore = args.metastore_db or config.get("metastore_db")
    warehouse = args.warehouse_dir or config.get("warehouse_dir")

    if not metastore or not warehouse:
        print("ERROR: metastore-db and warehouse-dir are required.")
        print("Provide via CLI args or configure in the TUI app first (Ctrl+P).")
        sys.exit(1)

    spark_mgr = SparkManager()
    print("Starting Spark session...")
    spark_mgr.start_session(metastore, warehouse)
    print("Spark session started. Creating tables...\n")
    create_tables(spark_mgr)
    spark_mgr.stop_session()
    print("Done.")


if __name__ == "__main__":
    main()
