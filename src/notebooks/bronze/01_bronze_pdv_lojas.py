# Databricks notebook source
"""
Notebook: 01_bronze_pdv_lojas
Origem: /Volumes/data-platform/default/landing-zone-varejo/01_pdv_lojas.csv
Tabela gerada: data-platform.bronze_pdv.pdv_lojas
Camada: Bronze — dados crus, append-only, sem transformação de negócio.
Separador: ponto-e-vírgula (;). Decimais BR com vírgula preservados como string.
Idempotência: replaceWhere na partição ingestion_dt = data atual.
"""

# COMMAND ----------

from datetime import date
from pyspark.sql.functions import col, current_date, current_timestamp

CATALOG = "data-platform"
VOLUME_PATH = f"/Volumes/{CATALOG}/default/landing-zone-varejo/01_pdv_lojas.csv"
TARGET_TABLE = f"`{CATALOG}`.bronze_pdv.pdv_lojas"

# COMMAND ----------

df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("sep", ";")
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
