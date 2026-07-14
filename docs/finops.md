# FinOps

FinOps aqui não é sobre pagar barato, é sobre desenhar para não pagar caro à toa. O projeto inteiro roda em plano gratuito, então o exercício é mostrar que as decisões continuam válidas se ele fosse para um ambiente pago.

## Decisões que reduzem custo

Formato de armazenamento. Tudo em Parquet e Delta. Colunar e comprimido, o que significa ler menos bytes por consulta. Em warehouse que cobra por byte lido, isso é dinheiro direto.

Particionamento por ano na Gold. Praticamente toda pergunta do negócio filtra por ano (indicador de 2024, evolução até tal ano). Particionar por ano faz a query tocar só a partição relevante em vez de varrer a tabela toda. Menos processamento, menos custo, resposta mais rápida.

Seleção de colunas na origem. Na extração do BigQuery eu evito `SELECT *` nas tabelas grandes e trago só o que uso. O sandbox tem teto de 1 TB de consulta por mês, e o BigQuery cobra por coluna lida, então selecionar coluna é o que mantém o projeto dentro do gratuito.

Compute sob demanda. O Databricks Free usa Serverless, que sobe para processar e não fica ligado ocioso. Não há cluster parado consumindo.

Streaming em rajada. O trigger `availableNow` processa os eventos que chegaram e encerra, em vez de manter um job de streaming rodando 24 por 7 esperando arquivo. Para uma fonte que atualiza pouco, streaming contínuo seria desperdício.

## Estimativa de custo

No desenho atual o custo é zero: BigQuery sandbox e Databricks Free Edition não cobram dentro dos limites usados, e o volume do Indicador Criança Alfabetizada fica muito abaixo do teto de 1 TB.

Numa projeção para ambiente pago, a ordem de grandeza dos custos seria: consulta no BigQuery proporcional aos bytes lidos (contida pela seleção de colunas e pelo particionamento), armazenamento Delta proporcional ao dado comprimido (baixo, por ser Parquet), e compute do Databricks proporcional ao tempo de execução dos jobs (baixo, porque batch roda por ciclo e o streaming roda em rajada). O gasto acompanharia o dado processado, não uma máquina ligada, que é exatamente o comportamento que se busca em FinOps.
