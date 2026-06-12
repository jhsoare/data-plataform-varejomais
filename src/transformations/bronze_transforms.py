import re

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_date, current_timestamp


def rename_columns_snake_case(df: DataFrame) -> DataFrame:
    """
    Renomeia todas as colunas do DataFrame para snake_case, removendo espaços e
    caracteres especiais, e convertendo para minúsculas.

    Entrada: DataFrame com qualquer schema de colunas.
    Saída: DataFrame com nomes de colunas em snake_case.
    """
    new_names = [
        re.sub(r"[^a-z0-9_]", "", col_name.strip().lower().replace(" ", "_"))
        for col_name in df.columns
    ]
    return df.toDF(*new_names)


def add_bronze_metadata(df: DataFrame, source_file_col: bool = True) -> DataFrame:
    """
    Adiciona colunas de metadados de ingestão ao DataFrame Bronze.

    Entrada: DataFrame lido via Auto Loader (deve ter _metadata disponível).
    Saída: DataFrame com colunas extras: ingestion_ts (timestamp UTC), ingestion_dt (date),
           source_file (caminho do arquivo de origem).
    Granularidade: não altera a granularidade do DataFrame de entrada.
    """
    df = df.withColumn("ingestion_ts", current_timestamp()).withColumn(
        "ingestion_dt", current_date()
    )
    if source_file_col:
        df = df.withColumn("source_file", col("_metadata.file_path"))
    return df
