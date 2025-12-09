# -------------------------------------------------------------
# GOLD: JUNÇÃO INTERNA (INNER JOIN) com tabela plano_acao
# -------------------------------------------------------------

from pyspark.sql import functions as F
from pyspark.sql import SparkSession
import traceback

spark = SparkSession.builder.getOrCreate()

# -------------------------------------------------------------
# 0. Configurações Globais
# -------------------------------------------------------------
# Tabela base já filtrada 
PLANO_ACAO_TABLE = "gold.transferegov.plano_acao"
COLUNA_JOIN = "id_plano_acao"

# Lista de endpoints que tem a coluna id_plano_acao.
ENDPOINTS_RELACIONADOS = [
    "plano_acao_analise",
    "plano_acao_dado_bancario",
    "plano_acao_destinacao_recursos",
    "plano_acao_historico",
    "plano_acao_meta",
    "relatorio_gestao",
    "termo_adesao"
]

# -------------------------------------------------------------
# 1. Função de Leitura da Tabela Base (SEM MUDANÇAS)
# -------------------------------------------------------------
def get_gold_plano_acao_base():
    try:
        df_base = spark.table(PLANO_ACAO_TABLE)
        
        if COLUNA_JOIN not in df_base.columns:
             raise Exception(f"A coluna de JOIN '{COLUNA_JOIN}' não foi encontrada na tabela base.")
        
        if df_base.count() == 0:
            raise Exception(f"A tabela {PLANO_ACAO_TABLE} está vazia. Não é possível fazer o JOIN.")
            
        # Selecionar APENAS a chave de join da base
        return df_base.select(COLUNA_JOIN).distinct()
        
    except Exception as e:
        print(f"--- ERRO ao carregar a tabela base {PLANO_ACAO_TABLE}: {e}")
        return None

# -------------------------------------------------------------
# 2. Função Principal de Processamento Gold (INNER JOIN e Seleção)
# -------------------------------------------------------------
def process_gold_join(df_base_keys, related_endpoint):
    
    silver_table = f"silver.transferegov.{related_endpoint}"
    gold_table = f"gold.transferegov.{related_endpoint}" 
    
    try:
        # Lê a tabela Silver relacionada
        df_related = spark.table(silver_table)
        
        # Faz o INNER JOIN 
        df_gold = df_related.join(
            df_base_keys, 
            on=COLUNA_JOIN, 
            how="inner" 
        )
        
        # Escreve no catálogo gold
        (
            df_gold.write
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .format("delta")
                .saveAsTable(gold_table)
        )

        print(f"Sucesso! Tabela Gold '{gold_table}' salva com {df_gold.count()} registros.")
        
    except Exception as e:
        print(f"--- ERRO ao processar o INNER JOIN com '{related_endpoint}': {e}")
        print(traceback.format_exc())


# -------------------------------------------------------------
# 3. Executar o Pipeline Principal
# -------------------------------------------------------------
# Carrega APENAS a coluna de chave (id_plano_acao) da base filtrada Gold
df_plano_acao_base_keys = get_gold_plano_acao_base()

if df_plano_acao_base_keys is not None:
    # Processa cada endpoint relacionado
    for endpoint_name in ENDPOINTS_RELACIONADOS:
        process_gold_join(df_plano_acao_base_keys, endpoint_name)