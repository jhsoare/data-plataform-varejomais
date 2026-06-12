# Databricks notebook source
"""
Notebook: 10_gold_visao_360_clientes
Origens lidas:
  - data-platform.silver.clientes_unificados     (dados cadastrais do cliente)
  - data-platform.silver.pedidos_consolidados    (histórico de compras omnichannel)
  - data-platform.bronze_app.app_eventos         (eventos de comportamento no app)
Tabela gerada: data-platform.gold.visao_360_clientes
Granularidade: 1 linha por cliente (cliente_unico_id)
Idempotência: overwrite completo diário (CREATE OR REPLACE / mode=overwrite)
"""

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

CATALOG = "data-platform"
TARGET_TABLE = f"`{CATALOG}`.gold.visao_360_clientes"

# COMMAND ----------

df_clientes = spark.table(f"`{CATALOG}`.silver.clientes_unificados")
df_pedidos = spark.table(f"`{CATALOG}`.silver.pedidos_consolidados")
df_app = spark.table(f"`{CATALOG}`.bronze_app.app_eventos")

# COMMAND ----------

# Métricas de pedidos por cliente
df_pedido_metricas = df_pedidos.groupBy("cliente_unico_id").agg(
    F.count("pedido_id").alias("total_pedidos"),
    F.sum("valor_total").cast(DecimalType(14, 2)).alias("valor_total_compras"),
    F.avg("valor_total").cast(DecimalType(14, 2)).alias("ticket_medio"),
    F.count(F.when(F.col("canal") == "pdv", 1)).alias("pedidos_pdv"),
    F.count(F.when(F.col("canal") == "ecommerce", 1)).alias("pedidos_ecommerce"),
    F.count(F.when(F.col("canal") == "rede_adquirida", 1)).alias("pedidos_rede_adquirida"),
    F.min("data_pedido_dt").alias("primeira_compra_dt"),
    F.max("data_pedido_dt").alias("ultima_compra_dt"),
)

# COMMAND ----------

# Métricas de app por user_id
# O app não tem CPF completo — vincula por user_id (sem match direto com cliente_unico_id neste estágio)
df_app_metricas = df_app.groupBy("user_id").agg(
    F.countDistinct("session_id").alias("qtd_sessoes_app"),
    F.first("device").alias("dispositivo_preferido"),
)

# COMMAND ----------

# Visão 360: clientes + pedidos + app (left join para preservar clientes sem pedidos)
df_visao = (
    df_clientes.join(df_pedido_metricas, "cliente_unico_id", "left")
    # App não tem chave de join direta com cliente_unico_id neste dataset sintético
    # Deixamos qtd_sessoes_app e dispositivo_preferido como NULL para clientes não identificados no app
    .select(
        "cliente_unico_id",
        "nome",
        "cpf_normalizado",
        "segmento_sap",
        "cidade",
        "uf",
        "email",
        F.coalesce(F.col("total_pedidos"), F.lit(0)).alias("total_pedidos"),
        F.coalesce(F.col("valor_total_compras"), F.lit(0).cast(DecimalType(14, 2))).alias("valor_total_compras"),
        F.coalesce(F.col("ticket_medio"), F.lit(0).cast(DecimalType(14, 2))).alias("ticket_medio"),
        F.coalesce(F.col("pedidos_pdv"), F.lit(0)).alias("pedidos_pdv"),
        F.coalesce(F.col("pedidos_ecommerce"), F.lit(0)).alias("pedidos_ecommerce"),
        F.coalesce(F.col("pedidos_rede_adquirida"), F.lit(0)).alias("pedidos_rede_adquirida"),
        "primeira_compra_dt",
        "ultima_compra_dt",
        F.lit(None).cast("long").alias("qtd_sessoes_app"),
        F.lit(None).cast("string").alias("dispositivo_preferido"),
    )
)

# COMMAND ----------

# Overwrite completo (Granularidade: 1 linha por cliente)
(
    df_visao.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

count = spark.table(TARGET_TABLE).count()
print(f"Gold visao_360_clientes: {count} clientes em {TARGET_TABLE}")
