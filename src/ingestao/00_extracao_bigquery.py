"""
Extracao das fontes do Indicador Crianca Alfabetizada (Base dos Dados, BigQuery).

Caminho programatico, alternativa ao passo manual pela console descrito em
consultas_bigquery.sql. Rodo as queries no meu projeto sandbox (sem billing) e
salvo cada entidade como CSV, que depois sobem para o Volume do Databricks.

As queries sao as mesmas do arquivo .sql: seleciono so as colunas usadas. O filtro
de rede nao e '0' (Total): esse codigo esta praticamente vazio nas tabelas de
indicador (uf tem 1 linha na base inteira, municipio so 398 de ~5.570). Uso rede
'5' (Publica Estadual e Municipal) para uf e '3' (Municipal) para municipio, que
sao os codigos com cobertura real e que batem com a rede que as tabelas de meta
ja usam (meta_municipio = "Municipal", meta_uf/meta_brasil = "Publica").
Alunos entra como amostra, porque o objetivo e demonstrar ingestao de microdado,
nao processar a base inteira.

Antes de rodar: gcloud auth application-default login, e defina GCP_PROJECT.
"""

import os
from pathlib import Path

from google.cloud import bigquery

BILLING_PROJECT = os.environ.get("GCP_PROJECT", "tech-challenge-fase2-502418")
DATASET = "basedosdados.br_inep_avaliacao_alfabetizacao"

SAIDA = Path(__file__).resolve().parents[2] / "data" / "raw"
SAIDA.mkdir(parents=True, exist_ok=True)

METAS = ("meta_alfabetizacao_2024, meta_alfabetizacao_2025, meta_alfabetizacao_2026, "
         "meta_alfabetizacao_2027, meta_alfabetizacao_2028, meta_alfabetizacao_2029, "
         "meta_alfabetizacao_2030")

QUERIES = {
    "uf": f"SELECT ano, sigla_uf, serie, rede, taxa_alfabetizacao, media_portugues FROM `{DATASET}.uf` WHERE rede = '5'",
    "municipio": f"SELECT ano, id_municipio, serie, rede, taxa_alfabetizacao, media_portugues FROM `{DATASET}.municipio` WHERE rede = '3'",
    "meta_brasil": f"SELECT ano, rede, taxa_alfabetizacao, {METAS}, percentual_participacao FROM `{DATASET}.meta_alfabetizacao_brasil`",
    "meta_uf": f"SELECT ano, sigla_uf, rede, taxa_alfabetizacao, {METAS}, percentual_participacao FROM `{DATASET}.meta_alfabetizacao_uf`",
    "meta_municipio": f"SELECT ano, id_municipio, rede, taxa_alfabetizacao, {METAS}, nivel_alfabetizacao, percentual_participacao FROM `{DATASET}.meta_alfabetizacao_municipio`",
    "alunos": f"SELECT ano, id_municipio, id_escola, id_aluno, serie, rede, presenca, alfabetizado, proficiencia, peso_aluno FROM `{DATASET}.alunos` WHERE id_municipio LIKE '35%' LIMIT 20000",
}


def main():
    client = bigquery.Client(project=BILLING_PROJECT)
    for entidade, sql in QUERIES.items():
        print(f"[extracao] {entidade}")
        df = client.query(sql).to_dataframe(create_bqstorage_client=True)
        destino = SAIDA / f"{entidade}.csv"
        df.to_csv(destino, index=False)
        print(f"[extracao] {entidade}: {len(df):,} linhas -> {destino.name}")


if __name__ == "__main__":
    main()
