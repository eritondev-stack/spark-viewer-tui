"""
spark-viewer-example — envia 3 DataFrames Spark para o TUI via print_df.

Uso:
    # Terminal 1 — sobe o TUI
    spark-viewer

    # Terminal 2 — roda o exemplo (aguarde "Spark iniciado!" no TUI)
    spark-viewer-example
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import rand, round as spark_round, when, col, lit

from ..client import print_df


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("spark-viewer-example")
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("OFF")

    print("Spark iniciado. Enviando DataFrames para o TUI...\n")

    # ── 1. Vendas por produto ─────────────────────────────────────────────────
    vendas = spark.range(1, 51).select(
        col("id").alias("produto_id"),
        when(col("id") % 3 == 0, "Eletrônicos")
        .when(col("id") % 3 == 1, "Roupas")
        .otherwise("Casa").alias("categoria"),
        spark_round(rand() * 5000 + 200, 2).alias("receita"),
        (rand() * 200 + 10).cast("int").alias("unidades_vendidas"),
        spark_round(rand() * 0.35 + 0.05, 4).alias("margem"),
    )

    print_df(vendas, "vendas")
    print("✓ live.vendas enviado")

    # ── 2. Métricas de servidor ───────────────────────────────────────────────
    metricas = spark.range(1, 31).select(
        col("id").alias("instancia_id"),
        when(col("id") <= 10, "web-server")
        .when(col("id") <= 20, "api-server")
        .otherwise("worker").alias("tipo"),
        spark_round(rand() * 95 + 5, 1).alias("cpu_pct"),
        spark_round(rand() * 80 + 10, 1).alias("memoria_pct"),
        (rand() * 10000 + 100).cast("long").alias("requisicoes_hora"),
        when(rand() > 0.85, lit(True)).otherwise(lit(False)).alias("alerta"),
    )

    print_df(metricas, "metricas_servidor")
    print("✓ live.metricas_servidor enviado")

    # ── 3. Agregação por categoria ────────────────────────────────────────────
    vendas.createOrReplaceTempView("_vendas_tmp")
    resumo = spark.sql("""
        SELECT
            categoria,
            COUNT(*)                     AS total_produtos,
            ROUND(SUM(receita), 2)       AS receita_total,
            ROUND(AVG(receita), 2)       AS ticket_medio,
            SUM(unidades_vendidas)       AS unidades_total,
            ROUND(AVG(margem) * 100, 2)  AS margem_media_pct
        FROM _vendas_tmp
        GROUP BY categoria
        ORDER BY receita_total DESC
    """)

    print_df(resumo, "resumo_categorias")
    print("✓ live.resumo_categorias enviado")

    spark.stop()
    print("\nPronto! Veja o banco 'live' na sidebar do TUI.")
    print("Dica: SELECT * FROM global_temp.vendas LIMIT 10")


if __name__ == "__main__":
    main()
