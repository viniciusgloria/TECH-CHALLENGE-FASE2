# Databricks notebook source
# MAGIC %md
# MAGIC # Gerador de eventos para o streaming simulado
# MAGIC
# MAGIC O enunciado pede ingestao em tempo quase real e deixa claro que pode ser uma
# MAGIC simulacao. Em producao isso seria um topico Kafka ou o Pub/Sub recebendo
# MAGIC atualizacao de indicador conforme os municipios revisam os dados. Aqui eu
# MAGIC simulo escrevendo um JSON por evento numa pasta do Volume que o Auto Loader
# MAGIC vigia (`streaming_in`).
# MAGIC
# MAGIC Cada evento e uma nova medicao de indicador para um municipio num ano. Rodo
# MAGIC esta celula e, com o notebook `04_streaming_indicadores` rodando em paralelo
# MAGIC (ou logo em seguida), da para ver os micro-batches consumindo os arquivos.
# MAGIC Mantive simples de proposito: nao e o streaming que da nota, e mostrar que a
# MAGIC arquitetura suporta os dois modos de ingestao.

# COMMAND ----------

import json
import random
import time
from datetime import datetime, timezone
from pathlib import Path

# No Databricks, Volumes do Unity Catalog sao acessiveis como caminho de
# filesystem normal, sem precisar de dbutils para escrever arquivo.
DESTINO = Path("/Volumes/workspace/default/tech_challenge_fase2/streaming_in")
DESTINO.mkdir(parents=True, exist_ok=True)

# uns poucos municipios de exemplo (codigo IBGE). Em uso real viria da dim_municipio.
MUNICIPIOS = ["3550308", "3304557", "2927408", "5300108", "4106902", "1302603"]

# COMMAND ----------


def novo_evento() -> dict:
    # mesmo vocabulario da base: taxa em percentual (0 a 100) e media de portugues
    return {
        "id_municipio": random.choice(MUNICIPIOS),
        "ano": 2024,
        "taxa_alfabetizacao": round(random.uniform(40, 95), 1),
        "media_portugues": round(random.uniform(680, 780), 1),
        "evento_ts": datetime.now(timezone.utc).isoformat(),
    }


def gerar_eventos(qtd: int = 50, intervalo: float = 2.0):
    for i in range(qtd):
        ev = novo_evento()
        arquivo = DESTINO / f"evento_{int(time.time()*1000)}_{i}.json"
        arquivo.write_text(json.dumps(ev, ensure_ascii=False))
        print(f"gerado {arquivo.name}: {ev['id_municipio']} -> {ev['taxa_alfabetizacao']}")
        time.sleep(intervalo)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Execucao
# MAGIC 50 eventos com 2s de intervalo entre cada um (~100s no total). Ajuste `qtd`
# MAGIC e `intervalo` se quiser um lote menor para testar mais rapido.

# COMMAND ----------

gerar_eventos(qtd=50, intervalo=2.0)
