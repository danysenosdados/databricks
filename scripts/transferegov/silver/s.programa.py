# -------------------------------------------------------------
# SILVER: TRANSFORMAÇÃO DO bronze.transferegov.<endpoint>
# Limpeza, normalização, tipagem e escrita no catálogo Silver
# -------------------------------------------------------------

from pyspark.sql import functions as F
from pyspark.sql import SparkSession
import re

spark = SparkSession.builder.getOrCreate()

# Widget para endpoint
dbutils.widgets.text("endpoint", "programa")
endpoint = dbutils.widgets.get("endpoint")

# -------------------------------------------------------------
# 1. Lê a tabela bronze
# -------------------------------------------------------------
bronze_table = f"bronze.transferegov.{endpoint}"
df = spark.table(bronze_table)

# -------------------------------------------------------------
# 2. Normaliza nomes das colunas
# -------------------------------------------------------------
def normalize_col_name(c):
    return (
        c.lower()
         .replace(" ", "_")
         .replace("-", "_")
         .replace("/", "_")
         .replace(":", "_")
         .replace(".", "_")
         .strip()
    )

df = df.toDF(*[normalize_col_name(c) for c in df.columns])

# -------------------------------------------------------------
# 3. Função para inferir tipo de cada coluna
# -------------------------------------------------------------
def infer_and_cast(df, sample_size=50):
    new_cols = []
    for c in df.columns:
        col = F.col(c)

        sample = df.select(col).na.drop().limit(sample_size).toPandas()[c].astype(str)

        tipo = "string"
        # Se todos são inteiros sem ponto/vírgula
        if sample.str.match(r"^[0-9]+$").all():
            tipo = "bigint"
        # Se todos são números (inteiros ou decimais com ponto/vírgula)
        elif sample.str.match(r"^[0-9]+([.,][0-9]+)?$").all():
            tipo = "double"
        # Se todos são datas
        elif sample.str.match(r"^\d{4}-\d{2}-\d{2}$").all():
            tipo = "date"
        # Se todos são timestamps
        elif sample.str.match(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$").all():
            tipo = "timestamp"

        # aplica cast único na coluna inteira
        if tipo == "bigint":
            new_cols.append(F.expr(f"try_cast({c} as bigint)").alias(c))
        elif tipo == "double":
            new_cols.append(F.regexp_replace(col, ",", ".").cast("double").alias(c))
        elif tipo == "date":
            new_cols.append(F.to_date(col, "yyyy-MM-dd").alias(c))
        elif tipo == "timestamp":
            new_cols.append(F.to_timestamp(col, "yyyy-MM-dd HH:mm:ss").alias(c))
        else:
            new_cols.append(col.alias(c))

    return df.select(new_cols)

df_silver = infer_and_cast(df)

# -------------------------------------------------------------
# 4. Escreve no catálogo silver
# -------------------------------------------------------------
(
    df_silver.write
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .format("delta")
        .saveAsTable(f"silver.transferegov.{endpoint}")
)