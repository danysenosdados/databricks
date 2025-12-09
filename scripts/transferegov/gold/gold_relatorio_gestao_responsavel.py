# -------------------------------------------------------------
# GOLD: Filtra relatorio_gestao_analise_responsavel pelos IDs de relatorio_gestao_analise.
# -------------------------------------------------------------

from pyspark.sql import functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# -------------------------------------------------------------
# Configurações Globais Corretas
# -------------------------------------------------------------
# Tabela base  
PLANO_ACAO_ANALISE_TABLE = "gold.transferegov.relatorio_gestao_analise"
COLUNA_BASE_JOIN = "id_relatorio_gestao_analise"

# Endpoint relacionado 
ENDPOINT_RELACIONADO = "relatorio_gestao_analise_responsavel"
COLUNA_RELATED_JOIN = "relatorio_gestao_analise_fk"

# -------------------------------------------------------------
# 1. Função de Processamento
# -------------------------------------------------------------
def process_nested_join(base_table, base_col, related_endpoint, related_col):
    
    base_source = base_table
    related_source = f"silver.transferegov.{related_endpoint}"
    gold_table = f"gold.transferegov.{related_endpoint}"

    try:
        # 1. Carrega e prepara as chaves da tabela base (Gold)
        df_keys = spark.table(base_source)
        
        # Seleciona APENAS a chave de join e RENOMEIA para que coincida com a coluna do DF relacionado
        df_keys = df_keys.select(
            F.col(base_col).alias(related_col)
        ).distinct()
        
        if df_keys.count() == 0:
            print(f"Tabela de chaves '{base_source}' está vazia. Não é possível fazer o JOIN.")
            return

        # 2. Lê a tabela relacionada (Silver)
        df_related = spark.table(related_source)
        
        # 3. Faz o INNER JOIN
        df_gold = df_related.join(
            df_keys, 
            on=related_col,
            how="inner" 
        )
        
        # 4. Escreve no catálogo gold
        (
            df_gold.write
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .format("delta")
                .saveAsTable(gold_table)
        )

        print(f"Sucesso! Tabela Gold '{gold_table}' salva com {df_gold.count()} registros.")
        
    except Exception as e:
        print(f"--- ERRO ao processar o JOIN: {e}")


# -------------------------------------------------------------
# 2. Executar o Pipeline
# -------------------------------------------------------------
process_nested_join(
    base_table=PLANO_ACAO_ANALISE_TABLE,
    base_col=COLUNA_BASE_JOIN,
    related_endpoint=ENDPOINT_RELACIONADO,
    related_col=COLUNA_RELATED_JOIN
)