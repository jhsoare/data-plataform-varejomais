# Databricks notebook source
"""
Notebook: 02_bronze_ecommerce_pedidos
Origem: /Volumes/data-platform/default/landing-zone-varejo/02_ecommerce_pedidos.csv
Tabela gerada: data-platform.bronze_ecommerce.ecommerce_pedidos
Camada: Bronze — dados crus, append-only, sem transformação de negócio.
Formato: CSV padrão (vírgula). Timestamps em ISO 8601.
Idempotência: replaceWhere na partição ingestion_dt = data atual.
"""

# COMMAND ----------

from datetime import date
from pyspark.sql.functions import col, current_date, current_timestamp

CATALOG = "data-platform"
VOLUME_PATH = f"/Volumes/{CATALOG}/default/landing-zone-varejo/02_ecommerce_pedidos.csv"
TARGET_TABLE = f"`{CATALOG}`.bronze_ecommerce.ecommerce_pedidos"

# COMMAND ----------

df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("mode", "PERMISSIVE")
    .load(VOLUME_PATH)
    .withColumn("ingestion_ts", current_timestamp())
    .withColumn("ingestion_dt", current_date())
    .withColumn("source_file", col("_metadata.file_path"))
)

# COMMAND ----------

(
    df.write.format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"ingestion_dt = '{date.today()}'")
    .option("mergeSchema", "true")
    .partitionBy("ingestion_dt")
    .saveAsTable(TARGET_TABLE)
)

count = spark.table(TARGET_TABLE).count()
print(f"Bronze ingestão concluída: {TARGET_TABLE} — {count} linhas")
