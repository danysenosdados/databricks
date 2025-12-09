# -------------------------------------------------------------
# GOLD: TRANSFORMAÇÃO DO silver.transferegov.<endpoint>
# Agregação, métricas de negócio e escrita no catálogo Gold
# -------------------------------------------------------------

from pyspark.sql import functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Widget para endpoint
dbutils.widgets.text("endpoint", "programa_beneficiario")
endpoint = dbutils.widgets.get("endpoint")

# -------------------------------------------------------------
# 1. Lê a tabela silver
# -------------------------------------------------------------
silver_table = f"silver.transferegov.{endpoint}"
df_silver = spark.table(silver_table)

# -------------------------------------------------------------
# 2. Filtro de CNPJs (camada Gold)
# -------------------------------------------------------------
CNPJS_DESEJADOS = [
    "10571982000125",
    "02960040000100",
    "06290858000114",
    "08693255000199"
]

col_cnpj = "cnpj_beneficiario_programa"

df_gold = df_silver.filter(F.col(col_cnpj).isin(CNPJS_DESEJADOS))

# -------------------------------------------------------------
# 3. Escreve no catálogo gold
# -------------------------------------------------------------
(
    df_gold.write
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .format("delta")
        .saveAsTable(f"gold.transferegov.{endpoint}")
)