from pyspark.sql import Column
from pyspark.sql.functions import length, lpad, regexp_replace, when


def normalize_cpf(cpf_col: Column) -> Column:
    """
    Normaliza CPF removendo a máscara e aplicando zero-fill para 11 dígitos.

    Entrada: Column com CPF em qualquer formato ('XXX.XXX.XXX-XX', '12345678901', '1234567890', etc.)
    Saída: Column StringType com exatamente 11 dígitos, ou NULL se a string não for um CPF válido
           (menos de 10 ou mais de 11 dígitos após remover não-numéricos).
    """
    digits = regexp_replace(cpf_col, r"\D", "")
    return when(
        (length(digits) >= 10) & (length(digits) <= 11),
        lpad(digits, 11, "0"),
    )


def validate_cpf(cpf_col: Column) -> Column:
    """
    Retorna True se o CPF normalizado é válido (exatamente 11 dígitos e não todos iguais).

    Entrada: Column com CPF já normalizado (11 dígitos) ou bruto.
    Saída: Column BooleanType.
    """
    from pyspark.sql.functions import col, lit

    normalized = normalize_cpf(cpf_col)
    # Rejeita CPFs com todos os dígitos iguais (ex: 00000000000, 11111111111)
    all_same = regexp_replace(normalized, r"(\d)\1{10}", "").isNull()
    return normalized.isNotNull() & ~all_same
