# Amostras

Os dados de verdade não vão para o Git (ver `.gitignore`); ficam no BigQuery e no Volume do Databricks. Esta pasta guarda só o mínimo para entender os formatos e reproduzir sem depender de acesso à nuvem.

- `evento_exemplo.json`: formato de um evento de streaming gerado por `src/ingestao/91_gerador_eventos_streaming.py`.

Quando rodar a extração, os Parquet das seis entidades caem em `data/raw/` (ignorada pelo Git). Se quiser versionar uma amostra pequena para o avaliador reproduzir, gere um recorte com `LIMIT` e salve aqui.
