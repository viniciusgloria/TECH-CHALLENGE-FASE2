# Databricks notebook source
# MAGIC %md
# MAGIC # Camada Silver - dados tratados e integrados
# MAGIC
# MAGIC Aqui o dado bruto vira confiavel e as bases se conectam. O que faco:
# MAGIC
# MAGIC - tipo as colunas (ano inteiro, taxas e medias como double, chaves como string);
# MAGIC - derivo a sigla da UF pelos dois primeiros digitos do codigo IBGE do
# MAGIC   municipio, com um mapa fixo das 27 UFs (a tabela de municipio so tem o codigo);
# MAGIC - despivoto as metas, que vem largas (uma coluna por ano de 2024 a 2030),
# MAGIC   para o formato longo (ano_meta, meta_taxa);
# MAGIC - integro o indicador municipal com a meta municipal do ano correspondente.
# MAGIC
# MAGIC Nota sobre rede: a extracao (ver consultas_bigquery.sql) ja filtra o indicador
# MAGIC na mesma rede que a respectiva tabela de meta usa: municipio em rede Municipal
# MAGIC (bate com meta_municipio) e uf em rede Publica Estadual e Municipal (bate com
# MAGIC meta_uf). O corte Total (rede 0) foi descartado por ter cobertura quase nula
# MAGIC nas duas tabelas (1 linha em uf, 398 de ~5.570 em municipio). Como cada tabela
# MAGIC de meta guarda rede como texto (nao numerico) e tem uma unica rede por linha,
# MAGIC o join usa apenas (ano, id_municipio), sem cruzar a coluna rede.
# MAGIC
# MAGIC Decisao de grao: agrego o municipio para (ano, id_municipio) antes de cruzar
# MAGIC com a meta, para nao multiplicar linha.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

CATALOGO = "workspace"
SCHEMA_BRONZE = "alfabetizacao_bronze"
SCHEMA_SILVER = "alfabetizacao_silver"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{SCHEMA_SILVER}")


def bronze(tabela):
    return spark.table(f"{CATALOGO}.{SCHEMA_BRONZE}.{tabela}").drop("_arquivo_origem", "_ingestao_ts")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Referencia de UF
# MAGIC Mapa fixo das 27 UFs. O codigo IBGE de dois digitos e a ponte entre o
# MAGIC municipio (que so tem codigo) e a sigla/nome/regiao.

# COMMAND ----------

ufs = [
    ("11", "RO", "Rondonia", "Norte"), ("12", "AC", "Acre", "Norte"),
    ("13", "AM", "Amazonas", "Norte"), ("14", "RR", "Roraima", "Norte"),
    ("15", "PA", "Para", "Norte"), ("16", "AP", "Amapa", "Norte"),
    ("17", "TO", "Tocantins", "Norte"),
    ("21", "MA", "Maranhao", "Nordeste"), ("22", "PI", "Piaui", "Nordeste"),
    ("23", "CE", "Ceara", "Nordeste"), ("24", "RN", "Rio Grande do Norte", "Nordeste"),
    ("25", "PB", "Paraiba", "Nordeste"), ("26", "PE", "Pernambuco", "Nordeste"),
    ("27", "AL", "Alagoas", "Nordeste"), ("28", "SE", "Sergipe", "Nordeste"),
    ("29", "BA", "Bahia", "Nordeste"),
    ("31", "MG", "Minas Gerais", "Sudeste"), ("32", "ES", "Espirito Santo", "Sudeste"),
    ("33", "RJ", "Rio de Janeiro", "Sudeste"), ("35", "SP", "Sao Paulo", "Sudeste"),
    ("41", "PR", "Parana", "Sul"), ("42", "SC", "Santa Catarina", "Sul"),
    ("43", "RS", "Rio Grande do Sul", "Sul"),
    ("50", "MS", "Mato Grosso do Sul", "Centro-Oeste"),
    ("51", "MT", "Mato Grosso", "Centro-Oeste"), ("52", "GO", "Goias", "Centro-Oeste"),
    ("53", "DF", "Distrito Federal", "Centro-Oeste"),
]
dim_uf = spark.createDataFrame(ufs, ["codigo_uf", "sigla_uf", "nome_uf", "regiao"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Indicador por municipio
# MAGIC Tipa, deriva a UF pelo codigo e agrega para (ano, id_municipio).

# COMMAND ----------

fato_municipio = (
    bronze("municipio")
    .withColumn("ano", F.col("ano").cast("int"))
    .withColumn("id_municipio", F.col("id_municipio").cast("string"))
    .withColumn("taxa_alfabetizacao", F.col("taxa_alfabetizacao").cast("double"))
    .withColumn("media_portugues", F.col("media_portugues").cast("double"))
    .withColumn("codigo_uf", F.substring("id_municipio", 1, 2))
    .groupBy("ano", "id_municipio", "codigo_uf")
    .agg(
        F.avg("taxa_alfabetizacao").alias("taxa_alfabetizacao"),
        F.avg("media_portugues").alias("media_portugues"),
    )
    .join(dim_uf, "codigo_uf", "left")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Indicador por UF

# COMMAND ----------

fato_uf = (
    bronze("uf")
    .withColumn("ano", F.col("ano").cast("int"))
    .withColumn("taxa_alfabetizacao", F.col("taxa_alfabetizacao").cast("double"))
    .withColumn("media_portugues", F.col("media_portugues").cast("double"))
    .groupBy("ano", "sigla_uf")
    .agg(
        F.avg("taxa_alfabetizacao").alias("taxa_alfabetizacao"),
        F.avg("media_portugues").alias("media_portugues"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Metas no formato longo
# MAGIC Antes de despivotar, dou um dedupe pegando a versao mais recente por chave
# MAGIC (a tabela pode ter mais de um ano-base). Depois viro o formato longo.

# COMMAND ----------

ANOS_META = list(range(2024, 2031))


def dedupe_ultimo_ano(df, chave):
    w = Window.partitionBy(chave).orderBy(F.col("ano").cast("int").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")


def despivotar_metas(df, chaves):
    pares = ", ".join(f"{ano}, CAST(meta_alfabetizacao_{ano} AS DOUBLE)" for ano in ANOS_META)
    expr = f"stack({len(ANOS_META)}, {pares}) as (ano_meta, meta_taxa)"
    return df.selectExpr(*chaves, expr)


meta_municipio_long = despivotar_metas(
    dedupe_ultimo_ano(bronze("meta_alfabetizacao_municipio").withColumn("id_municipio", F.col("id_municipio").cast("string")), "id_municipio"),
    ["id_municipio"],
)
meta_uf_long = despivotar_metas(dedupe_ultimo_ano(bronze("meta_alfabetizacao_uf"), "sigla_uf"), ["sigla_uf"])
meta_brasil_long = despivotar_metas(
    bronze("meta_alfabetizacao_brasil").orderBy(F.col("ano").cast("int").desc()).limit(1),
    [],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Integracao
# MAGIC Cruzo o indicador municipal com a meta do proprio ano de observacao, por
# MAGIC (ano, id_municipio). Gap positivo quer dizer meta batida.

# COMMAND ----------

municipio_ano_integrada = (
    fato_municipio.alias("f")
    .join(
        meta_municipio_long.alias("m"),
        (F.col("f.id_municipio") == F.col("m.id_municipio")) & (F.col("f.ano") == F.col("m.ano_meta")),
        "left",
    )
    .select(
        "f.ano", "f.id_municipio", "f.sigla_uf", "f.nome_uf", "f.regiao",
        "f.taxa_alfabetizacao", "f.media_portugues",
        F.col("m.meta_taxa").alias("meta_ano_corrente"),
    )
    .withColumn("gap_meta", F.col("taxa_alfabetizacao") - F.col("meta_ano_corrente"))
    .withColumn("atingiu_meta", (F.col("gap_meta") >= 0).cast("int"))
    .dropDuplicates()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checagem no microdado de alunos
# MAGIC Proporcao de alunos marcados como alfabetizados por municipio. A coluna vem
# MAGIC como texto, normalizo antes de contar.

# COMMAND ----------

alunos = (
    bronze("alunos")
    .withColumn("ano", F.col("ano").cast("int"))
    .withColumn("id_municipio", F.col("id_municipio").cast("string"))
    .withColumn("proficiencia", F.col("proficiencia").cast("double"))
    .withColumn("flag_alfabetizado", F.when(F.lower(F.col("alfabetizado")).isin("1", "sim", "true"), 1).otherwise(0))
)

indicador_alunos_municipio = (
    alunos.groupBy("ano", "id_municipio")
    .agg(
        F.count("*").alias("qtd_alunos_amostra"),
        F.avg("proficiencia").alias("proficiencia_media"),
        F.avg("flag_alfabetizado").alias("prop_alfabetizado_amostra"),
    )
)

# COMMAND ----------

def salvar(df, nome):
    (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOGO}.{SCHEMA_SILVER}.{nome}"))
    print(f"{nome}: {spark.table(f'{CATALOGO}.{SCHEMA_SILVER}.{nome}').count():,} linhas")


salvar(dim_uf, "dim_uf")
salvar(fato_municipio, "fato_municipio")
salvar(fato_uf, "fato_uf")
salvar(meta_municipio_long, "meta_municipio_long")
salvar(meta_uf_long, "meta_uf_long")
salvar(meta_brasil_long, "meta_brasil_long")
salvar(municipio_ano_integrada, "municipio_ano_integrada")
salvar(indicador_alunos_municipio, "indicador_alunos_municipio")
