# -------------------------------------------------------------
# BRONZE: EXTRACT API TRANSFEREGOV
# -------------------------------------------------------------

import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import current_timestamp, date_format, col
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
# Função de extração com paginação
# -------------------------------------------------------------
def extract_api_data(endpoint, limit=1000):
    url = f"https://api.transferegov.gestao.gov.br/fundoafundo/{endpoint}"
    headers = {"Accept": "application/json"}

    dados_totais = []
    offset = 0

    while True:
        try:
            params = {"offset": offset, "limit": limit}
            r = requests.get(url, headers=headers, params=params, timeout=60)
            r.raise_for_status()
            dados = r.json()

        except Exception as e:
            if r.status_code == 404:
                 print(f"Endpoint '{endpoint}' não encontrado (404). Pulando.")
            else:
                print(f"Erro ao conectar na API para o endpoint '{endpoint}': {e}")
            dados = None
            break

        if not dados:
            break

        dados_totais.extend(dados)
        offset += limit

    return dados_totais

# -------------------------------------------------------------
# Função Principal para Processar um Único Endpoint
# -------------------------------------------------------------
def process_endpoint(endpoint):
    
    # 1. Buscar dados da API
    dados = extract_api_data(endpoint)

    if not dados:
        print(f"Nenhum dado retornado para '{endpoint}'. Pulando.")
        return

    # 2. Descobrir TODAS as colunas existentes
    todas_colunas = set()
    for item in dados:
        todas_colunas.update(item.keys())

    # 3. Normalizar todos os itens para STRING
    dados_normalizados = []
    for item in dados:
        row = {}
        for col_name in todas_colunas:
            valor = item.get(col_name)
            if isinstance(valor, (dict, list)):
                row[col_name] = json.dumps(valor)
            elif valor is None:
                row[col_name] = None
            else:
                row[col_name] = str(valor)
        dados_normalizados.append(row)

    # 4. Criar schema totalmente STRING
    schema = StructType([StructField(col_name, StringType(), True) for col_name in todas_colunas])

    # 5. Criar DataFrame e adicionar coluna de auditoria (data da atualizacao)
    df = (
        spark.createDataFrame(dados_normalizados, schema)
              .withColumn("data_atualizacao", current_timestamp())
    )

    # 6. Salvar no catálogo = bronze/transferegov/endpoint
    try:
        table_name = f"bronze.transferegov.{endpoint}"
        df.write.mode("overwrite") \
              .option("overwriteSchema", "true") \
              .format("delta") \
              .saveAsTable(table_name)
        print(f"Sucesso! Tabela '{table_name}' salva com {df.count()} registros.")
    except Exception as e:
        print(f"--- ERRO ao salvar a tabela para '{endpoint}': {e}")
        print(traceback.format_exc())

# -------------------------------------------------------------
# Executar o Loop Principal
# -------------------------------------------------------------
for endpoint_name in ENDPOINTS_LIST:
    try:
        process_endpoint(endpoint_name)
    except Exception as e:
        print(f"Ocorreu um erro fatal ao processar '{endpoint_name}'. Detalhes: {e}")