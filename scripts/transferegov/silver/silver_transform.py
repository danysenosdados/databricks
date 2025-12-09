# -------------------------------------------------------------
# SILVER: TRANSFORMAÇÃO DO bronze.transferegov.<endpoint> EM LOTE
# Limpeza, normalização, tipagem e escrita no catálogo Silver
# -------------------------------------------------------------

from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import traceback

spark = SparkSession.builder.getOrCreate()

# -------------------------------------------------------------
# Lista de Endpoints 
# -------------------------------------------------------------
ENDPOINTS_LIST = [
    "plano_acao",
    "plano_acao_analise",
    "plano_acao_analise_responsavel",
    "plano_acao_dado_bancario",
    "plano_acao_destinacao_recursos",
    "plano_acao_historico",
    "plano_acao_meta",
    "plano_acao_meta_acao",
    "programa",
    "programa_beneficiario",
    "programa_gestao_agil",
    "relatorio_gestao",
    "relatorio_gestao_acoes",
    "relatorio_gestao_analise",
    "relatorio_gestao_analise_responsavel",
    "termo_adesao"
]

# -------------------------------------------------------------
# 1. Função para normalizar nomes de colunas
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

# -------------------------------------------------------------
# 2. Função para inferir e converter tipos 
# -------------------------------------------------------------
def infer_and_cast(df, sample_size=50):
    new_cols = []
    # Usar sample_size maior ou total se df tiver menos registros
    actual_sample_size = min(sample_size, df.count() if df.is_cached else df.limit(sample_size).count())
    
    # Se o DF estiver vazio, retorna ele mesmo (não há tipos para inferir)
    if actual_sample_size == 0:
        return df

    for c in df.columns:
        col = F.col(c)

        # Coleta a amostra para inferência
        sample_df = df.select(col).na.drop().limit(actual_sample_size)
        
        # Verifica se há dados na amostra para Pandas
        if sample_df.count() == 0:
            new_cols.append(col.alias(c)) # Mantém STRING se não houver dados
            continue

        sample = sample_df.toPandas()[c].astype(str)

        tipo = "string"
        # Verifica se todos na amostra são inteiros
        if sample.str.match(r"^[0-9]+$").all():
            tipo = "bigint"
        # Verifica se todos são números (inteiros ou decimais com ponto/vírgula)
        elif sample.str.match(r"^[0-9]+([.,][0-9]+)?$").all():
            tipo = "double"
        # Verifica se todos são datas (formato yyyy-MM-dd)
        elif sample.str.match(r"^\d{4}-\d{2}-\d{2}$").all():
            tipo = "date"
        # Verifica se todos são timestamps 
        elif sample.str.match(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d+)?$").all(): 
            tipo = "timestamp"
        # Verifica se é o formato ISO 8601 com fuso horário (como a data_atualizacao)
        elif sample.str.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+[+\-]\d{2}:\d{2}$").all():
             tipo = "timestamp_iso"


        # Aplica o cast específico
        if tipo == "bigint":
            # try_cast retorna NULL em caso de erro, evitando falhas
            new_cols.append(F.expr(f"try_cast({c} as bigint)").alias(c))
        elif tipo == "double":
            # Substitui vírgula por ponto antes de tentar o cast para double
            new_cols.append(F.regexp_replace(col, ",", ".").cast("double").alias(c))
        elif tipo == "date":
            new_cols.append(F.to_date(col, "yyyy-MM-dd").alias(c))
        elif tipo == "timestamp":
            new_cols.append(F.to_timestamp(col, "yyyy-MM-dd HH:mm:ss").alias(c))
        elif tipo == "timestamp_iso":
            new_cols.append(F.col(c).cast("timestamp").alias(c))
        else:
            new_cols.append(col.alias(c))
            
    return df.select(new_cols)

# -------------------------------------------------------------
# 3. Função Principal para Processar um Único Endpoint
# -------------------------------------------------------------
def process_silver_endpoint(endpoint):
    bronze_table = f"bronze.transferegov.{endpoint}"
    silver_table = f"silver.transferegov.{endpoint}"

    try:
        # 1. Lê a tabela bronze
        df = spark.table(bronze_table)
    except Exception as e:
        print(f"Não foi possível ler a tabela Bronze '{bronze_table}'. Pulando. ---")
        return

    if df.count() == 0:
        print(f"Tabela Bronze '{bronze_table}' está vazia. Pulando.")
        return
        
    # 2. Normaliza nomes das colunas
    df = df.toDF(*[normalize_col_name(c) for c in df.columns])

    # 3. Inferir e converter tipos
    df_silver = infer_and_cast(df)

    # 4. Escreve no catálogo silver
    try:
        (
            df_silver.write
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .format("delta")
                .saveAsTable(silver_table)
        )
        print(f"Sucesso! Tabela Silver '{silver_table}' salva com {df_silver.count()} registros.")
    except Exception as e:
        print(f"--- ERRO ao salvar a tabela Silver para '{endpoint}': {e}")
        print(traceback.format_exc())

# -------------------------------------------------------------
# 4. Executar o Loop Principal
# -------------------------------------------------------------
for endpoint_name in ENDPOINTS_LIST:
    try:
        process_silver_endpoint(endpoint_name)
    except Exception as e:
        print(f"Ocorreu um erro fatal ao processar '{endpoint_name}'. Detalhes: {e}")