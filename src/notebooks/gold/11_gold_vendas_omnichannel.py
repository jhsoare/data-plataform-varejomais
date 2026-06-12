# Databricks notebook source
"""
Notebook: 11_gold_vendas_omnichannel
Origens lidas:
  - data-platform.silver.pedidos_consolidados  (pedidos unificados omnichannel)
  - data-platform.silver.dim_produto           (categoria do produto)
Tabela gerada: data-platform.gold.vendas_omnichannel
Granularidade: 1 linha por (canal, categoria, mes_ano)
Idempotência: overwrite completo diário
"""

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

CATALOG = "data-platform"
TARGET_TABLE = f"`{CATALOG}`.gold.vendas_omnichannel"

# COMMAND ----------

df_pedidos = spark.table(f"`{CATALOG}`.silver.pedidos_consolidados")
df_produto = spark.table(f"`{CATALOG}`.silver.dim_produto").select("produto_id", "categoria")

# COMMAND ----------

df_vendas = (
    df_pedidos.join(df_produto, "produto_id", "left")
    .withColumn("mes_ano", F.date_trunc("month", F.col("data_pedido_dt")))
    .groupBy("canal", F.coalesce(F.col("categoria"), F.lit("Sem categoria")).alias("categoria"), "mes_ano")
    .agg(
        F.count("pedido_id").alias("qtd_pedidos"),
        F.sum("valor_total").cast(DecimalType(14, 2)).alias("receita_total"),
        F.avg("valor_total").cast(DecimalType(14, 2)).alias("ticket_medio"),
        F.countDistinct("cliente_unico_id").alias("clientes_unicos"),
    )
    .filter(F.col("mes_ano").isNotNull())
)

# COMMAND ----------

# Granularidade: 1 linha por (canal, categoria, mes_ano)
(
    df_vendas.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

count = spark.table(TARGET_TABLE).count()
print(f"Gold vendas_omnichannel: {count} linhas em {TARGET_TABLE}")
