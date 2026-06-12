# Databricks notebook source
"""
Notebook: 12_gold_base_previsao_demanda
Origens lidas:
  - data-platform.silver.pedidos_consolidados  (somente canal='pdv', com cod_loja)
  - data-platform.bronze_sap.sap_estoque       (estoque atual por produto e loja)
  - data-platform.silver.dim_produto           (mapeamento produto_id → cod_sap, nome, categoria)
Tabela gerada: data-platform.gold.base_previsao_demanda
Granularidade: 1 linha por (produto_id, cod_loja, data_pedido_dt)
Idempotência: overwrite completo diário
"""

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, StringType

CATALOG = "data-platform"
TARGET_TABLE = f"`{CATALOG}`.gold.base_previsao_demanda"

# COMMAND ----------

df_pedidos_pdv = (
    spark.table(f"`{CATALOG}`.silver.pedidos_consolidados")
    .filter(F.col("canal") == "pdv")
    .filter(F.col("produto_id").isNotNull())
    .groupBy("produto_id", "cod_loja", "data_pedido_dt")
    .agg(F.sum("quantidade").alias("qtd_vendida"))
)

df_produto = spark.table(f"`{CATALOG}`.silver.dim_produto").select(
    "produto_id", "cod_sap", "nome_padrao", "categoria"
)

df_estoque = (
    spark.table(f"`{CATALOG}`.bronze_sap.sap_estoque")
    .select(
        F.col("COD_SAP").cast(StringType()).alias("cod_sap"),
        F.col("COD_LOJA").alias("cod_loja"),
        F.col("ESTOQUE_ATUAL").cast("integer").alias("estoque_atual"),
        F.col("ESTOQUE_MINIMO").cast("integer").alias("estoque_minimo"),
        F.col("CUSTO_MEDIO").cast(DecimalType(12, 2)).alias("custo_medio"),
    )
    .dropDuplicates(["cod_sap", "cod_loja"])
)

# COMMAND ----------

df_base = (
    df_pedidos_pdv.join(df_produto, "produto_id", "left")
    .join(
        df_estoque,
        (df_produto["cod_sap"] == df_estoque["cod_sap"]) & (df_pedidos_pdv["cod_loja"] == df_estoque["cod_loja"]),
        "left",
    )
    .select(
        df_pedidos_pdv["produto_id"],
        df_produto["cod_sap"],
        df_produto["nome_padrao"].alias("nome_produto"),
        df_produto["categoria"],
        df_pedidos_pdv["cod_loja"],
        df_pedidos_pdv["data_pedido_dt"],
        df_pedidos_pdv["qtd_vendida"],
        df_estoque["estoque_atual"],
        df_estoque["estoque_minimo"],
        df_estoque["custo_medio"],
    )
)

# COMMAND ----------

# Granularidade: 1 linha por (produto_id, cod_loja, data_pedido_dt)
(
    df_base.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

count = spark.table(TARGET_TABLE).count()
print(f"Gold base_previsao_demanda: {count} linhas em {TARGET_TABLE}")
