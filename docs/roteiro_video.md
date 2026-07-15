# Roteiro do vídeo executivo (até 5 minutos)

Texto integral para ser lido/falado do jeito que está. Os trechos entre colchetes, em itálico, são indicação de pausa ou respiração, não são falados. As reticências marcam uma pausa curta dentro da própria frase. Tempo estimado por bloco entre parênteses; total aproximado de 4min30s a 4min50s numa fala em ritmo natural, calmo, sem pressa.

## Abertura (25s)

Bom dia. Meu nome é Vinícius Gloria e hoje eu vou apresentar um projeto de dados construído em torno de um problema real de política pública: a alfabetização das crianças brasileiras.

*[pausa breve]*

O Brasil tem uma meta clara: até 2030, toda criança alfabetizada até o fim do segundo ano do ensino fundamental. Para acompanhar isso existe o Indicador Criança Alfabetizada, que mede o percentual de crianças acima de um corte de desempenho definido pelo Inep.

O que eu vou mostrar aqui é como transformamos esse indicador... que hoje vive espalhado em fontes soltas... numa base única, confiável e pronta para decisão.

## O problema (45s)

*[pausa breve]*

O problema começa simples e fica complicado rápido. Um indicador sozinho não conta a história toda. Setenta por cento de alfabetização pode ser uma vitória para um município e um fracasso para o vizinho, dependendo de onde cada um partiu e da meta que foi pactuada para ele.

Para responder isso direito, é preciso cruzar o indicador com metas nacionais, estaduais e municipais... com o território... e com o perfil dos alunos. E essas fontes vêm com granularidades diferentes, chaves diferentes, e nem sempre a mesma cobertura.

*[pausa breve]*

Fazer esse cruzamento na mão, toda vez que alguém precisa de uma resposta, não escala. Esse foi o problema de engenharia que resolvemos: automatizar essa integração, com qualidade garantida e custo sob controle.

## A arquitetura (95s)

A solução inteira roda em nuvem, com custo zero. A fonte é o Base dos Dados, um repositório público que expõe o Indicador Criança Alfabetizada dentro do BigQuery, do Google Cloud. É de lá que vêm as seis entidades do projeto: UF, município, alunos, e as metas de alfabetização em nível nacional, estadual e municipal.

O processamento acontece no Databricks, organizado em três camadas... o que chamamos de Arquitetura Medalhão.

*[pausa breve]*

A primeira, Bronze, guarda o dado exatamente como ele chegou, sem transformação, para termos rastreabilidade total e histórico preservado.

A segunda, Silver, é onde limpamos, padronizamos os tipos e as chaves e, principalmente, integramos as seis fontes numa visão única por município e por ano.

A terceira, Gold, entrega o produto final: o indicador de alfabetização por município, a comparação entre meta e resultado, e a evolução do indicador ao longo do tempo.

*[pausa breve]*

A ingestão é híbrida. A maior parte do dado entra em lote, porque é histórico e muda pouco. E simulamos também um canal de streaming, absorvendo atualizações de indicador em tempo quase real, para mostrar que a mesma arquitetura aguenta os dois ritmos.

E antes de qualquer dado chegar na camada final, ele passa por uma validação automática de qualidade: duplicidade, dados ausentes, integridade das chaves, consistência entre tabelas. Hoje essa validação roda com zero alertas.

## O valor (55s)

*[pausa breve]*

O que isso entrega, na prática. Primeiro, uma fonte única e confiável: a análise para de gastar tempo juntando planilha solta e passa a gastar tempo interpretando resultado.

Segundo, a comparação direta entre onde cada município está e onde ele deveria estar. Isso é o que orienta a decisão de onde investir primeiro.

*[pausa breve]*

Terceiro... custo. A arquitetura foi pensada para ser barata desde o primeiro dia: armazenamento comprimido, consulta que lê só o necessário, processamento que liga, roda e desliga, sem cluster ocioso. Se o volume de dado crescer amanhã, esse desenho continua eficiente.

## O potencial de IA (40s)

*[pausa breve]*

E isso abre a porta para inteligência artificial. Como a camada final já sai numa linha por município e por ano, com indicador, meta e tendência, dá para treinar um modelo que antecipe quais municípios tendem a ficar longe da meta de 2030... antes do resultado acontecer.

Cruzando com dados socioeconômicos, dá para mapear vulnerabilidade educacional e priorizar política pública onde ela tem mais impacto. Essa pipeline não é só um retrato do passado. Ela é a base para agir sobre o futuro.

## Fechamento (20s)

*[pausa breve]*

Resumindo: pegamos um indicador público que hoje vive fragmentado, e entregamos uma base integrada, confiável, barata, e pronta tanto para análise quanto para inteligência artificial... a serviço de uma meta que importa para o país.

Obrigado.
