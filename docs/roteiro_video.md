# Roteiro do vídeo executivo (até 5 minutos)

Roteiro falado, em linguagem de apresentação para liderança. A ideia não é ler palavra por palavra, é ter o fio da meada. Falar de forma natural vale mais do que decorar. Tempo aproximado por bloco entre parênteses.

## Abertura (30s)

Bom dia. Vou apresentar a pipeline de dados que construímos para analisar a alfabetização infantil no Brasil. O ponto de partida é uma meta pública clara: até 2030, toda criança alfabetizada até o fim do 2º ano do fundamental. Para acompanhar isso existe o Indicador Criança Alfabetizada, que mede o percentual de crianças acima do corte de 743 pontos do Saeb. O nosso trabalho foi transformar esse indicador, que hoje vive espalhado em fontes separadas, numa base única e confiável para tomar decisão.

## O problema (45s)

O indicador sozinho não responde muita coisa. Para saber por que um município vai bem e o vizinho não, a gente precisa cruzar o indicador com as metas de cada nível, com o território e com o perfil dos alunos. Essas fontes vêm com granularidades e chaves diferentes, e juntar isso na mão, toda vez que alguém quer analisar, não escala. O problema que resolvemos é de engenharia: integrar essas fontes de forma automática, com qualidade garantida e custo controlado.

## A arquitetura (90s)

A solução roda em nuvem e com custo zero. A fonte é o Base dos Dados, acessado pelo BigQuery. O processamento acontece no Databricks, organizado em três camadas.

A primeira, Bronze, guarda o dado bruto, do jeito que chegou, para a gente ter rastreabilidade total. A segunda, Silver, é onde limpamos, padronizamos e, principalmente, integramos as seis fontes numa visão única por município e ano. A terceira, Gold, entrega o dado pronto: o indicador por município, a comparação entre meta e resultado, e a evolução ao longo do tempo.

A ingestão é híbrida. O grosso do dado entra em batch, que é histórico e muda pouco. E temos um canal de streaming para absorver atualizações de indicador em tempo quase real, mostrando que a arquitetura aguenta os dois ritmos. Em cima disso rodam validações automáticas de qualidade, que checam duplicidade, dados faltantes e integridade das chaves antes do dado chegar na camada analítica.

## O valor (60s)

O que isso entrega na prática. Primeiro, uma fonte única e confiável: a análise para de gastar tempo juntando planilha e passa a gastar tempo interpretando. Segundo, a comparação direta entre onde cada município está e onde deveria estar, que é o que orienta onde investir. Terceiro, custo. A arquitetura foi desenhada para ser barata: dado comprimido, consulta que lê só o necessário e processamento que sobe, roda e desliga. O mesmo desenho continua eficiente se o volume crescer.

## O potencial de IA (45s)

E isso abre a porta para inteligência artificial. Como a camada final já sai no formato de uma linha por município e ano, com indicador, meta e tendência, dá para treinar um modelo que antecipa quais municípios tendem a ficar longe da meta de 2030, antes do resultado acontecer. Enriquecendo com dados socioeconômicos, conseguimos mapear vulnerabilidade educacional e priorizar política pública onde ela tem maior impacto. Ou seja, a pipeline não é só um relatório do passado, é a base para agir sobre o futuro.

## Fechamento (20s)

Em resumo: pegamos um indicador público fragmentado e entregamos uma base integrada, confiável, barata e pronta para análise e para IA, a serviço de uma meta que importa para o país. Obrigado.
