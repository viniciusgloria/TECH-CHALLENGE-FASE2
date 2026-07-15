# Roteiro do vídeo executivo (até 5 minutos)

Resumo de tópicos para guiar a apresentação, em linguagem de liderança/stakeholder. Não é texto para decorar, é o fio condutor: cada bloco deve ser falado com as próprias palavras. Tempo aproximado por bloco entre parênteses; total em torno de 4min30s.

## Abertura (25s)

- Contexto: meta pública do Compromisso Nacional Criança Alfabetizada, toda criança alfabetizada até 2030 (fim do 2º ano do fundamental).
- O que mede o Indicador Criança Alfabetizada (corte de proficiência do Inep).
- Gancho: o indicador hoje vive espalhado em fontes soltas; o projeto junta tudo numa base única e confiável.

## O problema (45s)

- Indicador isolado não conta a história toda: o mesmo percentual pode ser bom ou ruim dependendo do ponto de partida e da meta pactuada.
- Para responder isso, é preciso cruzar indicador com metas (nacional, estadual, municipal), território e perfil dos alunos.
- Fontes com granularidade e chaves diferentes; cruzar na mão não escala.
- Reformular o problema como problema de engenharia: automatizar a integração, com qualidade e custo controlados.

## A arquitetura (95s)

- Nuvem, custo zero: fonte no Base dos Dados via BigQuery (GCP); processamento no Databricks.
- Seis entidades extraídas: UF, município, alunos, metas de alfabetização (Brasil, UF, município).
- Arquitetura Medalhão, três camadas:
  - Bronze: dado bruto, sem transformação, histórico e rastreabilidade preservados.
  - Silver: limpeza, tipagem, normalização de chaves, integração das seis fontes numa visão única por município/ano.
  - Gold: produto final — indicador por município, meta vs resultado, evolução temporal.
- Ingestão híbrida: batch para o histórico, streaming simulado para atualização quase em tempo real (mesma arquitetura absorve os dois ritmos).
- Qualidade automática antes da camada final: duplicidade, ausentes, chaves, consistência (validado com zero alertas).

## O valor (55s)

- Fonte única e confiável: menos tempo juntando planilha, mais tempo interpretando resultado.
- Comparação direta indicador vs meta, por município: orienta onde investir primeiro.
- Custo: armazenamento comprimido, consulta seletiva, processamento sob demanda sem cluster ocioso; desenho continua eficiente se o volume crescer.

## O potencial de IA (40s)

- Base Gold já granular por município/ano (indicador, meta, tendência): pronta para treinar modelo preditivo.
- Uso possível: antecipar municípios com risco de não bater a meta de 2030, antes do resultado sair.
- Cruzando com dados socioeconômicos: mapear vulnerabilidade educacional e priorizar política pública.
- Mensagem-chave: pipeline não é só retrato do passado, é base para agir sobre o futuro.

## Fechamento (20s)

- Recapitular em uma frase: indicador público fragmentado → base integrada, confiável, barata, pronta para análise e IA, a serviço de uma meta que importa para o país.
- Agradecimento.
