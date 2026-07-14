# Databricks notebook source
# MAGIC %md
# MAGIC # Camada Gold - datasets analiticos
# MAGIC
# MAGIC O que a analise, o dashboard e o modelo consomem. Tres produtos que
# MAGIC respondem o enunciado, mais um resumo por UF para leitura executiva:
# MAGIC
# MAGIC 1. indicador de alfabetizacao por municipio (foto mais recente);
# MAGIC 2. meta versus resultado (gap para a meta do ano);
# MAGIC 3. evolucao temporal do indicador;
# MAGIC 4. resumo por UF (media do indicador e percentual de municipios na meta).
# MAGIC
# MAGIC Gravo particionado por ano, que e o filtro mais comum das consultas. Menos
# MAGIC varredura, menos custo, resposta mais rapida.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOGO = "workspace"
SCHEMA_SILVER = "alfabetizacao_silver"
SCHEMA_GOLD = "alfabetizacao_gold"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{SCHEMA_GOLD}")

base = spark.table(f"{CATALOGO}.{SCHEMA_SILVER}.municipio_ano_integrada")


def salvar_particionado(df, nome):
    (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("ano").saveAsTable(f"{CATALOGO}.{SCHEMA_GOLD}.{nome}"))
    print(f"{nome}: {spark.table(f'{CATALOGO}.{SCHEMA_GOLD}.{nome}').count():,} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Indicador por municipio (ano mais recente)
# MAGIC Window para pegar a ultima medicao de cada municipio num passo so.

# COMMAND ----------

janela = Window.partitionBy("id_municipio").orderBy(F.col("ano").desc())

gold_indicador = base.withColumn("rn", F.row_number().over(janela)).filter(F.col("rn") == 1).drop("rn")
salvar_particionado(gold_indicador, "indicador_por_municipio")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Meta versus resultado
# MAGIC O que orienta politica publica: o tamanho do buraco entre onde o municipio
# MAGIC esta e a meta pactuada para aquele ano.

# COMMAND ----------

gold_meta_resultado = (
    base.filter(F.col("meta_ano_corrente").isNotNull())
    .select("ano", "id_municipio", "sigla_uf", "regiao", "taxa_alfabetizacao", "meta_ano_corrente", "gap_meta", "atingiu_meta")
)
salvar_particionado(gold_meta_resultado, "meta_versus_resultado")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Evolucao temporal
# MAGIC Serie por municipio com variacao ano a ano. Se a base tiver poucos anos a
# MAGIC serie e curta, mas a estrutura ja fica pronta para novos ciclos.

# COMMAND ----------

janela_tempo = Window.partitionBy("id_municipio").orderBy("ano")

gold_evolucao = (
    base.select("ano", "id_municipio", "sigla_uf", "taxa_alfabetizacao")
    .withColumn("taxa_ano_anterior", F.lag("taxa_alfabetizacao").over(janela_tempo))
    .withColumn("variacao", F.col("taxa_alfabetizacao") - F.col("taxa_ano_anterior"))
)
salvar_particionado(gold_evolucao, "evolucao_temporal")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Resumo por UF
# MAGIC Leitura executiva: media do indicador e proporcao de municipios que bateram
# MAGIC a meta, por estado e ano.

# COMMAND ----------

gold_resumo_uf = (
    base.groupBy("ano", "sigla_uf", "regiao")
    .agg(
        F.count("*").alias("qtd_municipios"),
        F.round(F.avg("taxa_alfabetizacao"), 2).alias("taxa_media"),
        F.round(F.avg("atingiu_meta"), 3).alias("prop_municipios_na_meta"),
    )
)
salvar_particionado(gold_resumo_uf, "resumo_por_uf")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conferencia

# COMMAND ----------

for t in ["indicador_por_municipio", "meta_versus_resultado", "evolucao_temporal", "resumo_por_uf"]:
    print(f"--- {t} ---")
    spark.table(f"{CATALOGO}.{SCHEMA_GOLD}.{t}").show(5, truncate=False)
