-- Extração das seis entidades do Indicador Criança Alfabetizada.
-- Dataset público: basedosdados.br_inep_avaliacao_alfabetizacao
--
-- Rodo cada consulta no BigQuery sandbox e baixo o resultado como CSV
-- (Salvar resultados > CSV). Depois subo os CSV para o Volume do Databricks.
--
-- Duas decisões que aparecem nas queries:
-- 1. Filtro de rede. Testamos rede = '0' (Total) primeiro, mas a distribuição real
--    mostrou que esse código está praticamente vazio nas tabelas de indicador: uf
--    tem 1 linha em toda a base e município só 398 de ~5.570 (ano 2024). Contamos
--    por rede e ano (GROUP BY rede, ano) para achar o corte com cobertura de verdade:
--      uf:        rede 5 (Pública Estadual e Municipal) cobre 25 de 27 UFs em 2023 e 2024
--      município: rede 3 (Municipal) cobre 5.448 município em 2023 e 2024 (~98% do total)
--    Isso também casa com o que as tabelas de meta já usam: meta_municipio guarda
--    rede como texto "Municipal" (código 3) e meta_uf/meta_brasil guardam "Pública"
--    (código 5; o código 6, Pública incluindo Federal, nem aparece na base). Com os
--    filtros abaixo, indicador e meta comparam a mesma rede, sem precisar de ressalva.
--    Dicionário completo: 0 Total, 1 Federal, 2 Estadual, 3 Municipal, 4 Privada,
--    5 Pública (Estadual e Municipal), 6 Pública (Federal, Estadual e Municipal).
-- 2. Seleciono só as colunas que uso. O BigQuery cobra por coluna lida e o
--    sandbox tem teto de 1 TB/mês, então nada de SELECT *.


-- ============================================================
-- 1. UF: indicador de alfabetização por estado
-- ============================================================
SELECT ano, sigla_uf, serie, rede, taxa_alfabetizacao, media_portugues
FROM `basedosdados.br_inep_avaliacao_alfabetizacao.uf`
WHERE rede = '5';


-- ============================================================
-- 2. Município: indicador de alfabetização por município
-- ============================================================
SELECT ano, id_municipio, serie, rede, taxa_alfabetizacao, media_portugues
FROM `basedosdados.br_inep_avaliacao_alfabetizacao.municipio`
WHERE rede = '3';


-- ============================================================
-- 3. Meta Alfabetização Brasil (metas 2024 a 2030, formato largo)
-- ============================================================
SELECT ano, rede, taxa_alfabetizacao,
       meta_alfabetizacao_2024, meta_alfabetizacao_2025, meta_alfabetizacao_2026,
       meta_alfabetizacao_2027, meta_alfabetizacao_2028, meta_alfabetizacao_2029,
       meta_alfabetizacao_2030, percentual_participacao
FROM `basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_brasil`;


-- ============================================================
-- 4. Meta Alfabetização por UF
-- ============================================================
SELECT ano, sigla_uf, rede, taxa_alfabetizacao,
       meta_alfabetizacao_2024, meta_alfabetizacao_2025, meta_alfabetizacao_2026,
       meta_alfabetizacao_2027, meta_alfabetizacao_2028, meta_alfabetizacao_2029,
       meta_alfabetizacao_2030, percentual_participacao
FROM `basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_uf`;


-- ============================================================
-- 5. Meta Alfabetização por Município
-- ============================================================
SELECT ano, id_municipio, rede, taxa_alfabetizacao,
       meta_alfabetizacao_2024, meta_alfabetizacao_2025, meta_alfabetizacao_2026,
       meta_alfabetizacao_2027, meta_alfabetizacao_2028, meta_alfabetizacao_2029,
       meta_alfabetizacao_2030, nivel_alfabetizacao, percentual_participacao
FROM `basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_municipio`;


-- ============================================================
-- 6. Alunos: microdado. Amostra para demonstrar ingestão de dado granular.
--    Filtro São Paulo (id_municipio começa com 35) e limito o volume, porque
--    o objetivo aqui é provar que a pipeline lê microdado, não processar milhões
--    de linhas na console. A base analítica usa a taxa já agregada do município.
-- ============================================================
SELECT ano, id_municipio, id_escola, id_aluno, serie, rede,
       presenca, alfabetizado, proficiencia, peso_aluno
FROM `basedosdados.br_inep_avaliacao_alfabetizacao.alunos`
WHERE id_municipio LIKE '35%'
LIMIT 20000;
