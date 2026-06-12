# Databricks notebook source
"""
Notebook: 06_bronze_rede_adquirida
Origem: /Volumes/data-platform/default/landing-zone-varejo/05_rede_adquirida_planilha.csv
Tabela gerada: data-platform.bronze_rede_adquirida.rede_adquirida
Camada: Bronze — dados crus com mínima limpeza de nomes de colunas (espaços → underscore).
Dados de baixa qualidade: datas em formatos mistos, CPF ausente, valores inconsistentes.
Tudo preservado como string para tratamento na camada Silver.
Idempotência: replaceWhere na partição ingestion_dt = data atual.
"""

# COMMAND ----------

import re

from datetime import date
from pyspark.sql.functions import col, current_date, current_timestamp

CATALOG = "data-platform"
VOLUME_PATH = f"/Volumes/{CATALOG}/default/landing-zone-varejo/05_rede_adquirida_planilha.csv"
TARGET_TABLE = f"`{CATALOG}`.bronze_rede_adquirida.rede_adquirida"

# COMMAND ----------


def _clean_col_name(name: str) -> str:
    """Converte nome de coluna para snake_case válido."""
    return re.sub(r"[^a-z0-9_]", "", name.strip().lower().replace(" ", "_"))


# COMMAND ----------

raw = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "false")  # tudo como string: datas/valores inconsistentes
    .option("mode", "PERMISSIVE")
    .load(VOLUME_PATH)
)

# Renomeia colunas com espaço para snake_case
clean_cols = [_clean_col_name(c) for c in raw.columns]
df = (
    raw.toDF(*clean_cols)
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
