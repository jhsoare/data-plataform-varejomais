# Databricks notebook source
"""
Notebook: 07_silver_dim_produto
Origens lidas:
  - data-platform.bronze_pdv.pdv_lojas          (colunas: SKU, PRODUTO)
  - data-platform.bronze_ecommerce.ecommerce_pedidos (colunas: product_sku, product_name)
  - data-platform.bronze_sap.sap_estoque        (colunas: COD_SAP, DESCRICAO, CATEGORIA)
Tabela gerada: data-platform.silver.dim_produto
Granularidade: 1 linha por produto único (produto_id = sha2 do nome normalizado)
Idempotência: MERGE INTO na chave produto_id
"""

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

CATALOG = "data-platform"
TARGET_TABLE = f"`{CATALOG}`.silver.dim_produto"

# COMMAND ----------


def _normalize_nome(col_expr):
    """Remove acentos/especiais, converte para upper, trim — chave de match entre sistemas."""
    return F.upper(F.trim(F.regexp_replace(col_expr, r"[^a-zA-Z0-9 ]", "")))


# COMMAND ----------

# PDV: SKU loja + nome do produto
df_pdv = (
    spark.table(f"`{CATALOG}`.bronze_pdv.pdv_lojas")
    .select(F.col("SKU").alias("sku_loja"), F.col("PRODUTO").alias("nome_raw"))
    .dropDuplicates(["sku_loja"])
    .withColumn("nome_normalizado", _normalize_nome(F.col("nome_raw")))
)

# E-commerce: SKU ecom + nome do produto
df_ecom = (
    spark.table(f"`{CATALOG}`.bronze_ecommerce.ecommerce_pedidos")
    .select(F.col("product_sku").alias("sku_ecom"), F.col("product_name").alias("nome_raw"))
    .dropDuplicates(["sku_ecom"])
    .withColumn("nome_normalizado", _normalize_nome(F.col("nome_raw")))
)

# SAP estoque: COD_SAP + descrição + categoria
df_sap = (
    spark.table(f"`{CATALOG}`.bronze_sap.sap_estoque")
    .select(
        F.col("COD_SAP").cast(StringType()).alias("cod_sap"),
        F.col("DESCRICAO").alias("nome_raw"),
        F.col("CATEGORIA").alias("categoria"),
    )
    .dropDuplicates(["cod_sap"])
    .withColumn("nome_normalizado", _normalize_nome(F.col("nome_raw")))
)

# COMMAND ----------

# Union de nomes únicos dos 3 sistemas
df_nomes = (
    df_pdv.select("nome_normalizado")
    .union(df_ecom.select("nome_normalizado"))
    .union(df_sap.select("nome_normalizado"))
    .dropDuplicates(["nome_normalizado"])
    .filter(F.col("nome_normalizado").isNotNull() & (F.length("nome_normalizado") > 0))
)

# Join para montar a dimensão de produto unificada
df_produto = (
    df_nomes.join(df_pdv.select("nome_normalizado", "sku_loja"), "nome_normalizado", "left")
    .join(df_ecom.select("nome_normalizado", "sku_ecom"), "nome_normalizado", "left")
    .join(df_sap.select("nome_normalizado", "cod_sap", "categoria"), "nome_normalizado", "left")
    .withColumn(
        "produto_id",
        F.sha2(F.col("nome_normalizado"), 256),
    )
    .withColumn("nome_padrao", F.col("nome_normalizado"))
    .select("produto_id", "nome_padrao", "categoria", "sku_loja", "sku_ecom", "cod_sap")
)

# COMMAND ----------

# Cria tabela se não existir
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
        produto_id    STRING NOT NULL,
        nome_padrao   STRING,
        categoria     STRING,
        sku_loja      STRING,
        sku_ecom      STRING,
        cod_sap       STRING
    )
    USING DELTA
    COMMENT '1 linha por produto único. Chave: produto_id (sha2 do nome normalizado).'
""")

# MERGE idempotente
df_produto.createOrReplaceTempView("_silver_dim_produto_stage")

spark.sql(f"""
    MERGE INTO {TARGET_TABLE} AS tgt
    USING _silver_dim_produto_stage AS src
    ON tgt.produto_id = src.produto_id
    WHEN MATCHED THEN UPDATE SET
        tgt.nome_padrao = src.nome_padrao,
        tgt.categoria   = src.categoria,
        tgt.sku_loja    = src.sku_loja,
        tgt.sku_ecom    = src.sku_ecom,
        tgt.cod_sap     = src.cod_sap
    WHEN NOT MATCHED THEN INSERT *
""")

count = spark.table(TARGET_TABLE).count()
print(f"Silver dim_produto: {count} produtos únicos em {TARGET_TABLE}")
