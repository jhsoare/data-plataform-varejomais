# Databricks notebook source
"""
Notebook: 09_silver_pedidos_consolidados
Origens lidas:
  - data-platform.bronze_pdv.pdv_lojas                   (pedidos de loja física)
  - data-platform.bronze_ecommerce.ecommerce_pedidos     (pedidos de e-commerce)
  - data-platform.bronze_rede_adquirida.rede_adquirida   (pedidos de rede adquirida)
  - data-platform.silver.clientes_unificados             (para resolução de cliente_unico_id)
  - data-platform.silver.dim_produto                     (para resolução de produto_id)
Tabela gerada: data-platform.silver.pedidos_consolidados
Granularidade: 1 linha por item de pedido (pedido_id via sha2 de chaves naturais)
Idempotência: overwrite completo com overwriteSchema=True
"""

# COMMAND ----------

from datetime import date

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, StringType

CATALOG = "data-platform"
TARGET_TABLE = f"`{CATALOG}`.silver.pedidos_consolidados"

# COMMAND ----------


def _normalize_cpf(col_expr):
    """Remove máscara e aplica lpad para 11 dígitos."""
    digits = F.regexp_replace(col_expr, r"\D", "")
    return F.when(
        (F.length(digits) >= 10) & (F.length(digits) <= 11),
        F.lpad(digits, 11, "0"),
    )


def _normalize_nome_produto(col_expr):
    """Remove especiais e converte para upper para match com dim_produto."""
    return F.upper(F.trim(F.regexp_replace(col_expr, r"[^a-zA-Z0-9 ]", "")))


def _to_decimal(col_expr):
    """Remove símbolos monetários/espaços, normaliza separador BR (vírgula→ponto), cast Decimal."""
    clean = F.regexp_replace(col_expr.cast(StringType()), r"[^\d,.\-]", "")
    # Sem ponto: vírgula é separador decimal BR → substitui por ponto
    # Com ponto: ponto já é decimal → remove vírgulas (milhares)
    normalized = F.when(
        F.instr(clean, ".") == 0,
        F.regexp_replace(clean, ",", "."),
    ).otherwise(
        F.regexp_replace(clean, ",", ""),
    )
    return F.when(F.length(F.trim(normalized)) > 0, normalized.cast(DecimalType(12, 2)))


# COMMAND ----------

df_clientes = spark.table(f"`{CATALOG}`.silver.clientes_unificados").select(
    "cliente_unico_id", "cpf_normalizado"
)

df_produto = spark.table(f"`{CATALOG}`.silver.dim_produto").select(
    "produto_id", "nome_padrao", "sku_loja", "sku_ecom"
)

# COMMAND ----------

# 1. PDV — pedidos de loja física
df_pdv = (
    spark.table(f"`{CATALOG}`.bronze_pdv.pdv_lojas")
    .withColumn("cpf_norm", _normalize_cpf(F.col("CPF")))
    .join(
        df_clientes.withColumnRenamed("cliente_unico_id", "cuid"),
        F.col("cpf_norm") == F.col("cpf_normalizado"),
        "left",
    )
    .withColumn("nome_norm", _normalize_nome_produto(F.col("PRODUTO")))
    .join(
        df_produto.withColumnRenamed("produto_id", "pid"),
        F.col("nome_norm") == F.col("nome_padrao"),
        "left",
    )
    .select(
        F.sha2(F.concat_ws("|", F.col("CLI_ID"), F.col("COD_LOJA"), F.col("DATA_VENDA"), F.col("SKU")), 256).alias("pedido_id"),
        F.coalesce(F.col("cuid"), F.lit("DESCONHECIDO")).alias("cliente_unico_id"),
        F.col("pid").alias("produto_id"),
        F.lit("pdv").alias("canal"),
        F.col("DATA_VENDA").cast("date").alias("data_pedido_dt"),
        _to_decimal(F.col("VLR_TOTAL")).alias("valor_total"),
        F.col("QTD").cast("integer").alias("quantidade"),
        F.lit(None).cast(StringType()).alias("status"),
        F.col("COD_LOJA").alias("cod_loja"),
    )
)

# COMMAND ----------

# 2. E-commerce
df_ecom = (
    spark.table(f"`{CATALOG}`.bronze_ecommerce.ecommerce_pedidos")
    .withColumn("nome_norm", _normalize_nome_produto(F.col("product_name")))
    .join(
        df_produto.withColumnRenamed("produto_id", "pid"),
        F.col("nome_norm") == F.col("nome_padrao"),
        "left",
    )
    .select(
        F.sha2(F.concat_ws("|", F.col("order_id"), F.col("product_sku")), 256).alias("pedido_id"),
        F.concat(F.lit("ECOM-"), F.col("customer_id")).alias("cliente_unico_id"),
        F.col("pid").alias("produto_id"),
        F.lit("ecommerce").alias("canal"),
        F.to_date(F.col("order_ts")).alias("data_pedido_dt"),
        (F.col("unit_price").cast(DecimalType(12, 2)) * F.col("quantity").cast("integer")).cast(DecimalType(12, 2)).alias("valor_total"),
        F.col("quantity").cast("integer").alias("quantidade"),
        F.col("status"),
        F.lit(None).cast(StringType()).alias("cod_loja"),
    )
)

# COMMAND ----------

# 3. Rede adquirida
df_rede = (
    spark.table(f"`{CATALOG}`.bronze_rede_adquirida.rede_adquirida")
    .withColumn("cpf_norm", _normalize_cpf(F.col("documento")))
    .join(
        df_clientes.withColumnRenamed("cliente_unico_id", "cuid"),
        F.col("cpf_norm") == F.col("cpf_normalizado"),
        "left",
    )
    .withColumn("nome_norm", _normalize_nome_produto(F.col("produto_comprado")))
    .join(
        df_produto.withColumnRenamed("produto_id", "pid"),
        F.col("nome_norm") == F.col("nome_padrao"),
        "left",
    )
    .select(
        F.sha2(F.concat_ws("|", F.col("cliente"), F.col("data_compra"), F.col("produto_comprado")), 256).alias("pedido_id"),
        F.coalesce(
            F.col("cuid"),
            F.concat(F.lit("REDE-"), F.sha2(F.trim(F.col("cliente")), 256).substr(1, 12)),
        ).alias("cliente_unico_id"),
        F.col("pid").alias("produto_id"),
        F.lit("rede_adquirida").alias("canal"),
        F.coalesce(
            F.expr("try_to_date(data_compra, 'dd/MM/yyyy')"),
            F.expr("try_to_date(data_compra, 'dd-MM-yy')"),
            F.expr("try_to_date(data_compra, 'yyyy-MM-dd')"),
        ).alias("data_pedido_dt"),
        _to_decimal(F.col("valor")).alias("valor_total"),
        F.lit(1).cast("integer").alias("quantidade"),
        F.lit(None).cast(StringType()).alias("status"),
        F.lit(None).cast(StringType()).alias("cod_loja"),
    )
)

# COMMAND ----------

# Union das 3 fontes com schema idêntico (tipos explícitos em todos os branches)
_schema_cols = ["pedido_id", "cliente_unico_id", "produto_id", "canal", "data_pedido_dt",
                "valor_total", "quantidade", "status", "cod_loja"]

df_consolidado = (
    df_pdv.select(*_schema_cols)
    .union(df_ecom.select(*_schema_cols))
    .union(df_rede.select(*_schema_cols))
    .withColumn("ingestion_ts", F.current_timestamp())
    .withColumn("ingestion_dt", F.current_date())
    .dropDuplicates(["pedido_id"])
)

# COMMAND ----------

# overwriteSchema evita conflito de tipos em re-execuções
(
    df_consolidado.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("ingestion_dt")
    .saveAsTable(TARGET_TABLE)
)

count = spark.table(TARGET_TABLE).count()
print(f"Silver pedidos_consolidados: {count} itens em {TARGET_TABLE}")
