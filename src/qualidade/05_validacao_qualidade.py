# Databricks notebook source
# MAGIC %md
# MAGIC # Qualidade de dados
# MAGIC
# MAGIC As quatro checagens do enunciado, rodando contra a Silver, que e onde a
# MAGIC integridade tem que valer. Cada teste vira uma linha num relatorio com
# MAGIC status ok ou alerta, gravado numa tabela versionada para acompanhar a
# MAGIC saude da base ao longo do tempo.
# MAGIC
# MAGIC Registro alerta em vez de derrubar o job com assert. Em dado publico, uma
# MAGIC chave orfa e motivo para investigar, nao para parar a pipeline. Se fosse
# MAGIC dado financeiro, ai sim eu barraria a carga.

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime

CATALOGO = "workspace"
SCHEMA_SILVER = "alfabetizacao_silver"
SCHEMA_GOLD = "alfabetizacao_gold"

resultados = []


def registrar(teste, tabela, metrica, valor, status, detalhe=""):
    resultados.append((teste, tabela, metrica, float(valor), status, detalhe))


integrada = spark.table(f"{CATALOGO}.{SCHEMA_SILVER}.municipio_ano_integrada")
dim_uf = spark.table(f"{CATALOGO}.{SCHEMA_SILVER}.dim_uf")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Duplicidade
# MAGIC A base integrada deve ser unica por municipio e ano (a extracao ja filtra
# MAGIC uma unica rede por entidade: Municipal para municipio, Publica para uf).

# COMMAND ----------

total = integrada.count()
distintos = integrada.select("ano", "id_municipio").dropDuplicates().count()
dup = total - distintos
registrar("duplicidade", "municipio_ano_integrada", "linhas_duplicadas", dup, "ok" if dup == 0 else "alerta", "chave=(ano, id_municipio)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Valores ausentes
# MAGIC Chave nao pode faltar (tolerancia zero). Indicador pode faltar em algum grau.

# COMMAND ----------

def checar_ausentes(coluna, teto_pct):
    nulos = integrada.filter(F.col(coluna).isNull()).count()
    pct = (nulos / total * 100) if total else 0
    registrar("ausentes", "municipio_ano_integrada", f"pct_nulo_{coluna}", round(pct, 2), "ok" if pct <= teto_pct else "alerta", f"teto={teto_pct}%")


checar_ausentes("id_municipio", 0.0)
checar_ausentes("taxa_alfabetizacao", 10.0)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Chave de relacionamento
# MAGIC Todo municipio tem que resolver para uma UF valida. Sigla nula aqui significa
# MAGIC codigo IBGE que nao caiu no mapa das 27 UFs.

# COMMAND ----------

orfaos = integrada.filter(F.col("sigla_uf").isNull()).count()
registrar("chave_relacionamento", "municipio_ano_integrada", "municipios_sem_uf", orfaos, "ok" if orfaos == 0 else "alerta", "sigla_uf nao resolvida pelo codigo IBGE")

siglas_invalidas = (
    integrada.select("sigla_uf").dropDuplicates()
    .join(dim_uf.select("sigla_uf").dropDuplicates(), "sigla_uf", "left_anti")
    .filter(F.col("sigla_uf").isNotNull())
    .count()
)
registrar("chave_relacionamento", "municipio_ano_integrada", "siglas_fora_da_dim", siglas_invalidas, "ok" if siglas_invalidas == 0 else "alerta", "")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Consistencia entre tabelas
# MAGIC A taxa de alfabetizacao e um percentual, entao tem que ficar entre 0 e 100.

# COMMAND ----------

fora_faixa = integrada.filter((F.col("taxa_alfabetizacao") < 0) | (F.col("taxa_alfabetizacao") > 100)).count()
registrar("consistencia", "municipio_ano_integrada", "taxa_fora_0_100", fora_faixa, "ok" if fora_faixa == 0 else "alerta", "taxa deve ficar em [0,100]")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Relatorio

# COMMAND ----------

colunas = ["teste", "tabela", "metrica", "valor", "status", "detalhe"]
relatorio = spark.createDataFrame(resultados, colunas).withColumn("executado_em", F.lit(datetime.now().isoformat()))

display(relatorio)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{SCHEMA_GOLD}")
(relatorio.write.format("delta").mode("append").saveAsTable(f"{CATALOGO}.{SCHEMA_GOLD}.relatorio_qualidade"))

alertas = relatorio.filter(F.col("status") == "alerta").count()
print(f"Checagens: {len(resultados)} | alertas: {alertas}")
