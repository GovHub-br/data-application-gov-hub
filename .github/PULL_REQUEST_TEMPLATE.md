## Descrição

<!-- O que este PR faz e por que a mudança é necessária. Escreva para quem vai
revisar daqui a duas semanas sem lembrar do contexto. -->

## Issue relacionada

Closes #

## Tipo de mudança

- [ ] Ingestão — cliente de API, DAG, carga
- [ ] Modelo dbt — transformação, camada, teste
- [ ] Correção de bug ou de inconsistência de dados
- [ ] Infra / CI
- [ ] Documentação
- [ ] Outro: ___

## Impacto em dados

<!-- Preencha sempre que o PR tocar DAG ou modelo dbt. Se não tocar, escreva
"nenhum" e siga. -->

- **Schemas e tabelas afetados:**
- **Camada:** raw / bronze / silver / gold
- **Precisa reexecutar alguma coisa depois do merge?** (qual DAG, qual `dbt run`, em que ordem)
- **Muda número que já está em uso?** (dashboard, relatório de meta, resposta já entregue)

## Como testar

```bash
# Ajuste conforme o tipo da mudança

# Modelo dbt
cd dbt/minc
dbt run  --select <modelo>
dbt test --select <modelo>

# DAG
airflow dags test <nome_da_dag> <data_execucao>

# Geral
make lint
make test
```

## Evidências

<!-- Print, log, resultado de query, contagem antes e depois. Para mudança que
altera número, a contagem antes e depois não é opcional. -->

## Checklist

- [ ] Título segue Conventional Commits
- [ ] Issue relacionada referenciada acima
- [ ] Rodei `make lint` localmente — ou justifiquei abaixo por que não
- [ ] Rodei `make test` localmente — ou justifiquei abaixo por que não
- [ ] Testes dbt adicionados ou atualizados, se aplicável
- [ ] Descrição das colunas atualizada no `schema.yml`, se criei ou alterei modelo
- [ ] Nenhuma credencial, CPF, CNPJ ou dado pessoal no código, no log ou no print
- [ ] Branch atualizada com `origin/main`
- [ ] Documentação afetada atualizada

<!-- Sobre o lint: a CI roda `make lint-ci` com `|| true`, então ela não reprova
o PR quando o lint falha. O item acima é afirmação sua, não resultado da CI —
é por isso que está escrito na primeira pessoa. -->
