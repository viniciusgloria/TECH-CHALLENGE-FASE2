# Qualidade de dados

As regras de validação rodam contra a Silver, que é a camada onde a integridade precisa valer. O script está em `src/qualidade/05_validacao_qualidade.py` e grava o resultado numa tabela versionada, uma linha por checagem, com status ok ou alerta.

## As quatro checagens

Duplicidade. A chave município-ano tem que ser única na base integrada. Comparo o total de linhas com o total de chaves distintas; qualquer diferença indica que algum join multiplicou registro, que é o erro mais comum ao integrar tabelas de granularidades diferentes.

Valores ausentes. Separo o que é grave do que é tolerável. Null em chave (id_municipio) tem tolerância zero, porque sem chave a linha não integra. Null em métrica (indicador) tem tolerância de alguns por cento, porque nem todo município tem medição em todo ano e isso é esperado.

Chave de relacionamento. Todo município presente no fato tem que existir na dimensão de municípios. Faço um anti-join para achar código IBGE órfão. Quando aparece órfão, quase sempre é diferença de tipo entre as tabelas (um lado como int, outro como string), que é justamente o motivo de eu padronizar a chave como string na Silver.

Consistência entre tabelas. O indicador é uma proporção, então tem que ficar no intervalo de 0 a 1. Valor fora disso denuncia erro de escala ou de cálculo em alguma etapa anterior.

## Por que relatório e não falha dura

Optei por registrar alerta em vez de derrubar o job com assert. Em dado público de alfabetização, uma chave órfã ou um percentual esquisito é motivo para investigar, não para parar a pipeline inteira. A tabela de qualidade guarda o histórico das execuções, então dá para ver se a base está melhorando ou piorando ao longo do tempo. Se o contexto fosse dado financeiro ou algo com efeito irreversível, aí sim a validação barraria a carga.
