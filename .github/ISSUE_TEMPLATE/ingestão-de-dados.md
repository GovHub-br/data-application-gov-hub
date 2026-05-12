---
name: Ingestão de Dados
about: Criar ou ajustar processo de ingestão de dados
title: ''
labels: ''
assignees: ''

---

## Contexto

Explique a necessidade de ingestão.

## Fonte

- Nome:
- Tipo:
- Localização:
- Formato dos dados:
- Frequência de atualização:

## Destino

- Banco:
- Schema:
- Tabela:
- Camada: bronze / raw / landing
- Ambiente:

## Estratégia de ingestão

- [ ] Full load
- [ ] Incremental
- [ ] Particionada
- [ ] Sob demanda
- [ ] Outra

## Chaves e controle

- Chave primária:
- Campo de data de atualização:
- Estratégia para duplicidade:
- Estratégia para reprocessamento:

## Tarefas

- [ ] Validar fonte
- [ ] Criar estrutura de destino
- [ ] Implementar ingestão
- [ ] Tratar duplicidades
- [ ] Criar controle de execução
- [ ] Validar dados ingeridos
- [ ] Documentar processo

## Critérios de aceite

- [ ] Dados chegam ao destino esperado
- [ ] Ingestão respeita a estratégia definida
- [ ] Duplicidades foram tratadas
- [ ] Processo pode ser reexecutado com segurança
- [ ] Documentação foi criada
