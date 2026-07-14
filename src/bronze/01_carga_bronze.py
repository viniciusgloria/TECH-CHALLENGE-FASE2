# Databricks notebook source
# MAGIC %md
# MAGIC # Camada Bronze - dados brutos
# MAGIC
# MAGIC A Bronze é o retrato fiel da origem. Leio os CSV que exportei do BigQuery e
# MAGIC subi no Volume, e gravo em Delta sem transformar. Leio tudo como string de
# MAGIC propósito: na Bronze não quero que ninguém (nem o Spark, via inferência de
# MAGIC schema) tome decisão sobre tipo. Tipagem é problema da Silver. A única coisa
# MAGIC que acrescento é metadado de ingestão, para conseguir auditar depois.

# COMMAND ----------

from pyspark.sql import functions as F

VOLUME = "/Volumes/workspace/default/tech_challenge_fase2/raw"
CATALOGO = "workspace"
SCHEMA_BRONZE = "alfabetizacao_bronze"

# nome do CSV no Volume -> nome da tabela Bronze
# (mantenha os arquivos com estes nomes ao subir no Volume)
ARQUIVOS = {
    "uf.csv": "uf",
    "municipio.csv": "municipio",
    "meta_brasil.csv": "meta_alfabetizacao_brasil",
    "meta_uf.csv": "meta_alfabetizacao_uf",
    "meta_municipio.csv": "meta_alfabetizacao_municipio",
    "alunos.csv": "alunos",
}

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{SCHEMA_BRONZE}")

# COMMAND ----------

def carregar_bronze(arquivo: str, tabela: str):
    origem = f"{VOLUME}/{arquivo}"
    destino = f"{CATALOGO}.{SCHEMA_BRONZE}.{tabela}"

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")   # Bronze crua: tudo string
        .csv(origem)
        .withColumn("_arquivo_origem", F.lit(arquivo))
        .withColumn("_ingestao_ts", F.current_timestamp())
    )

    (df.write.format("delta").mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(destino))

    print(f"{tabela}: {spark.table(destino).count():,} linhas")


for arquivo, tabela in ARQUIVOS.items():
    carregar_bronze(arquivo, tabela)

# COMMAND ----------

# MAGIC %md
# MAGIC Conferência rápida. Se alguma vier zerada, quase sempre é nome de arquivo
# MAGIC diferente do esperado no Volume.

# COMMAND ----------

for tabela in ARQUIVOS.values():
    print(f"--- {tabela} ---")
    spark.table(f"{CATALOGO}.{SCHEMA_BRONZE}.{tabela}").show(3, truncate=False)
