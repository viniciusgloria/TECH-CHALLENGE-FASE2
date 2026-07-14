# Databricks notebook source
# MAGIC %md
# MAGIC # Ingestao streaming - atualizacao de indicadores
# MAGIC
# MAGIC Este e o lado streaming da pipeline hibrida. O gerador de eventos
# MAGIC (91_gerador_eventos_streaming.py) escreve um JSON por medicao numa pasta, e
# MAGIC aqui o Auto Loader le em micro-batches conforme os arquivos chegam.
# MAGIC
# MAGIC Escolhi Structured Streaming + Auto Loader no lugar de Kafka de proposito.
# MAGIC Kafka foi o que vimos em ETL Pipelines e resolveria, mas manter um broker de
# MAGIC pe para dado publico de baixa volumetria e custo e operacao que nao se pagam.
# MAGIC O Auto Loader da ingestao incremental com checkpoint e evolucao de schema sem
# MAGIC infra extra. Trade-off documentado no README.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

CATALOGO = "workspace"
SCHEMA_BRONZE = "alfabetizacao_bronze"

ENTRADA = "/Volumes/workspace/default/tech_challenge_fase2/streaming_in"
CHECKPOINT = "/Volumes/workspace/default/tech_challenge_fase2/_chk/indicadores"

# Schema definido na mao. Em streaming nao gosto de inferencia: se o evento vier
# torto, prefiro a coluna cair como null do que o schema mudar sozinho.
schema_evento = StructType([
    StructField("id_municipio", StringType()),
    StructField("ano", IntegerType()),
    StructField("taxa_alfabetizacao", DoubleType()),
    StructField("media_portugues", DoubleType()),
    StructField("evento_ts", StringType()),
])

# COMMAND ----------

stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .schema(schema_evento)
    .load(ENTRADA)
    .withColumn("_ingestao_ts", F.current_timestamp())
    .withColumn("evento_ts", F.to_timestamp("evento_ts"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC Grava numa tabela propria de streaming na Bronze, separada da Bronze batch
# MAGIC para nao misturar as cadencias. `availableNow` processa o que ja chegou e
# MAGIC para, bom para rodar na apresentacao sem deixar cluster ligado (FinOps).

# COMMAND ----------

query = (
    stream.writeStream.format("delta")
    .option("checkpointLocation", CHECKPOINT)
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable(f"{CATALOGO}.{SCHEMA_BRONZE}.indicador_stream")
)

query.awaitTermination()

# COMMAND ----------

display(
    spark.table(f"{CATALOGO}.{SCHEMA_BRONZE}.indicador_stream")
    .groupBy("ano")
    .agg(F.count("*").alias("eventos"), F.avg("taxa_alfabetizacao").alias("taxa_media"))
)
