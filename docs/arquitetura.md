# Arquitetura da solução

Este documento detalha as decisões de desenho que o README resume. A ideia é deixar registrado o porquê de cada escolha, não só o que foi feito.

## Visão geral

A pipeline é híbrida (batch e streaming) e segue a Arquitetura Medalhão sobre um lakehouse Delta no Databricks Free Edition, com o Base dos Dados no BigQuery sandbox como fonte. O desenho prioriza custo zero e simplicidade operacional, porque o volume de dado público de alfabetização não justifica infraestrutura pesada.

## Camadas

Bronze é o dado cru. A regra aqui é não decidir nada. Qualquer transformação nesta camada é uma decisão que eu não vou conseguer desfazer depois sem voltar na origem, então deixo o dado intacto e só carimbo origem e horário de ingestão. Isso me dá reprocessamento e auditoria de graça.

Silver é onde concentro o trabalho pesado. Padronização de schema, tipagem, normalização de chave e, principalmente, a integração das seis entidades. A granularidade escolhida é município-ano porque é a menor granularidade que responde as perguntas do desafio sem explodir o volume. A chave de município é o código IBGE de 7 dígitos como string; tratar como número é o erro clássico que perde zero à esquerda e quebra o join.

Gold é o dado servido. Modelado por caso de uso, tipado, particionado por ano. Quem consome a Gold não precisa saber que existe uma Bronze.

## Modelo de integração

O centro do modelo é a tabela `municipio_ano_integrada`. Ela parte do indicador agregado por município e ano (derivado dos dados de alunos com o corte de 743 pontos do Saeb) e recebe, por junção, o território (dim_municipio), a meta municipal (por IBGE e ano) e a meta estadual (por sigla de UF e ano). As metas Brasil ficam como referência agregada separada.

## Por que lakehouse e não warehouse puro

Warehouse puro obrigaria a carregar o dado bruto num sistema e o refinado em outro, com custo de mover dado entre eles. O lakehouse Delta deixa Bronze, Silver e Gold no mesmo storage, com transação e versionamento, e ainda entrega performance de warehouse na Gold via particionamento. Para este volume, é o melhor dos dois mundos sem o custo de manter dois sistemas.

## Streaming

O streaming é intencionalmente simples. Auto Loader lendo uma pasta de eventos JSON, com schema fixo e checkpoint. Isso cobre o requisito de ingestão em tempo quase real sem broker. A observação importante é que a arquitetura não muda se o streaming crescer: troca-se a fonte do readStream de arquivo para Kafka ou Pub/Sub e o resto da pipeline continua igual.

## Pontos que eu evoluiria com mais tempo

Colocaria as fontes externas (Censo Escolar, IBGE, FUNDEB) para transformar a Gold numa base de features de verdade. Montaria um painel de observabilidade lendo a tabela de qualidade e os metadados de ingestão. E automatizaria a orquestração com Databricks Workflows, para o batch rodar agendado em vez de manual.
