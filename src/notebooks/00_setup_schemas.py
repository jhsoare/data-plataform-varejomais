# Databricks notebook source
"""
Notebook: 00_setup_schemas
Origens lidas: nenhuma
Tabelas geradas: apenas schemas e volume de checkpoints
Objetivo: Criar todos os schemas da arquitetura Medallion no catalog data-platform
          e garantir que o diretório de checkpoints do Auto Loader exista.
"""

# COMMAND ----------

CATALOG = "data-platform"
CHECKPOINT_BASE = f"/Volumes/{CATALOG}/default/checkpoints"

schemas = [
    "bronze_pdv",
    "bronze_ecommerce",
    "bronze_app",
    "bronze_sap",
    "bronze_rede_adquirida",
    "silver",
    "gold",
]

for schema in schemas:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{schema}`")
    print(f"Schema garantido: {CATALOG}.{schema}")

# COMMAND ----------

# Volume de checkpoints (criado no schema default do catalog)
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`default`.`checkpoints`")
print(f"Volume de checkpoints garantido: {CHECKPOINT_BASE}")
