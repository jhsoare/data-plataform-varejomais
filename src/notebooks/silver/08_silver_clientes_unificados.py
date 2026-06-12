# Databricks notebook source
"""
Notebook: 08_silver_clientes_unificados
Origens lidas:
  - data-platform.bronze_pdv.pdv_lojas                   (CPF com máscara, NOME_CLIENTE)
  - data-platform.bronze_sap.sap_clientes                (CPF sem máscara, NOME, CIDADE, UF, DATA_NASC, SEGMENTO)
  - data-platform.bronze_rede_adquirida.rede_adquirida   (documento misto, cliente)
  - data-platform.bronze_ecommerce.ecommerce_pedidos     (customer_id, email — sem CPF)
Tabela gerada: data-platform.silver.clientes_unificados
Granularidade: 1 linha por cliente único (cliente_unico_id)
Idempotência: MERGE INTO na chave cliente_unico_id
"""

# COMMAND ----------

import sys

sys.path.insert(0, "/Workspace/Repos")  # ajuste se necessário para importar módulos locais

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

CATALOG = "data-platform"
TARGET_TABLE = f"`{CATALOG}`.silver.clientes_unificados"

# COMMAND ----------


def normalize_cpf(col_expr):
    """
    Remove máscara e aplica lpad para 11 dígitos.
    Entrada: Column com CPF em qualquer formato.
    Saída: Column StringType com 11 dígitos ou NULL se inválido.
    """
    digits = F.regexp_replace(col_expr, r"\D", "")
    return F.when(
        (F.length(digits) >= 10) & (F.length(digits) <= 11),
        F.lpad(digits, 11, "0"),
    )


# COMMAND ----------

# 1. Clientes do PDV (com CPF mascarado)
df_pdv = (
    spark.table(f"`{CATALOG}`.bronze_pdv.pdv_lojas")
    .select(
        F.col("CLI_ID").alias("id_origem"),
        F.col("NOME_CLIENTE").alias("nome"),
        normalize_cpf(F.col("CPF")).alias("cpf_normalizado"),
    )
    .dropDuplicates(["cpf_normalizado"])
    .filter(F.col("cpf_normalizado").isNotNull())
    .withColumn("cliente_unico_id", F.col("cpf_normalizado"))
    .withColumn("email", F.lit(None).cast(StringType()))
    .withColumn("fonte_origem", F.lit("pdv"))
)

# 2. Clientes SAP (CPF já sem máscara; enriquece com segmento, cidade, uf, data_nasc)
df_sap = (
    spark.table(f"`{CATALOG}`.bronze_sap.sap_clientes")
    .select(
        F.col("BP_ID").alias("id_origem"),
        F.col("NOME").alias("nome"),
        F.lpad(F.col("CPF").cast(StringType()), 11, "0").alias("cpf_normalizado"),
        F.col("SEGMENTO").alias("segmento_sap"),
        F.col("CIDADE").alias("cidade"),
        F.col("UF").alias("uf"),
        # DATA_NASC vem como inteiro YYYYMMDD
        F.to_date(F.col("DATA_NASC").cast(StringType()), "yyyyMMdd").alias("data_nasc_dt"),
    )
    .dropDuplicates(["cpf_normalizado"])
    .filter(F.col("cpf_normalizado").isNotNull())
)

# 3. Clientes da rede adquirida
df_rede = (
    spark.table(f"`{CATALOG}`.bronze_rede_adquirida.rede_adquirida")
    .select(
        F.col("cliente").alias("nome"),
        normalize_cpf(F.col("documento")).alias("cpf_normalizado"),
    )
    .filter(F.trim(F.col("nome")).isNotNull() & (F.length(F.trim(F.col("nome"))) > 0))
    .dropDuplicates(["cpf_normalizado"])
    .withColumn(
        "cliente_unico_id",
        F.when(F.col("cpf_normalizado").isNotNull(), F.col("cpf_normalizado")).otherwise(
            F.concat(F.lit("REDE-"), F.sha2(F.trim(F.col("nome")), 256).substr(1, 12))
        ),
    )
    .withColumn("email", F.lit(None).cast(StringType()))
    .withColumn("fonte_origem", F.lit("rede_adquirida"))
)

# 4. Clientes do e-commerce (sem CPF)
df_ecom = (
    spark.table(f"`{CATALOG}`.bronze_ecommerce.ecommerce_pedidos")
    .select(
        F.col("customer_id").alias("id_origem"),
        F.col("email").alias("email"),
    )
    .dropDuplicates(["id_origem"])
    .withColumn("nome", F.lit(None).cast(StringType()))
    .withColumn("cpf_normalizado", F.lit(None).cast(StringType()))
    .withColumn("cliente_unico_id", F.concat(F.lit("ECOM-"), F.col("id_origem")))
    .withColumn("fonte_origem", F.lit("ecommerce"))
)

# COMMAND ----------

# Union de todas as fontes com schema harmonizado
_cols = ["cliente_unico_id", "nome", "cpf_normalizado", "email", "fonte_origem"]

df_todos = (
    df_pdv.select(*_cols)
    .union(df_rede.select(*_cols))
    .union(df_ecom.select(*_cols))
    .dropDuplicates(["cliente_unico_id"])
)

# Enriquece com dados SAP (segmento, cidade, uf, data_nasc) via join no CPF
df_final = (
    df_todos.join(
        df_sap.select("cpf_normalizado", "segmento_sap", "cidade", "uf", "data_nasc_dt"),
        "cpf_normalizado",
        "left",
    )
    .withColumn("ingestion_ts", F.current_timestamp())
    .select(
        "cliente_unico_id",
        "nome",
        "cpf_normalizado",
        "email",
        "segmento_sap",
        "cidade",
        "uf",
        "data_nasc_dt",
        "fonte_origem",
        "ingestion_ts",
    )
)

# COMMAND ----------

# Cria tabela se não existir
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
        cliente_unico_id  STRING NOT NULL,
        nome              STRING,
        cpf_normalizado   STRING,
        email             STRING,
        segmento_sap      STRING,
        cidade            STRING,
        uf                STRING,
        data_nasc_dt      DATE,
        fonte_origem      STRING,
        ingestion_ts      TIMESTAMP
    )
    USING DELTA
    COMMENT '1 linha por cliente único. Chave: cliente_unico_id (CPF normalizado ou ECOM-/REDE- prefixado).'
""")

df_final.createOrReplaceTempView("_silver_clientes_stage")

spark.sql(f"""
    MERGE INTO {TARGET_TABLE} AS tgt
    USING _silver_clientes_stage AS src
    ON tgt.cliente_unico_id = src.cliente_unico_id
    WHEN MATCHED THEN UPDATE SET
        tgt.nome             = COALESCE(src.nome, tgt.nome),
        tgt.cpf_normalizado  = COALESCE(src.cpf_normalizado, tgt.cpf_normalizado),
        tgt.email            = COALESCE(src.email, tgt.email),
        tgt.segmento_sap     = COALESCE(src.segmento_sap, tgt.segmento_sap),
        tgt.cidade           = COALESCE(src.cidade, tgt.cidade),
        tgt.uf               = COALESCE(src.uf, tgt.uf),
        tgt.data_nasc_dt     = COALESCE(src.data_nasc_dt, tgt.data_nasc_dt),
        tgt.ingestion_ts     = src.ingestion_ts
    WHEN NOT MATCHED THEN INSERT *
""")

count = spark.table(TARGET_TABLE).count()
print(f"Silver clientes_unificados: {count} clientes únicos em {TARGET_TABLE}")
