# Databricks notebook source
"""
Notebook: 03_bronze_app_eventos
Origem: /Volumes/data-platform/default/landing-zone-varejo/03_app_eventos.jsonl
Tabela gerada: data-platform.bronze_app.app_eventos
Camada: Bronze — dados crus, append-only, sem transformação de negócio.
Formato: JSONL (um objeto JSON por linha). CPF parcialmente mascarado preservado como string.
Idempotência: replaceWhere na partição ingestion_dt = data atual.
"""

# COMMAND ----------

from datetime import date
from pyspark.sql.functions import col, current_date, current_timestamp

CATALOG = "data-platform"
VOLUME_PATH = f"/Volumes/{CATALOG}/default/landing-zone-varejo/03_app_eventos.jsonl"
TARGET_TABLE = f"`{CATALOG}`.bronze_app.app_eventos"

# COMMAND ----------

df = (
    spark.read.format("json")
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
