# Acervo — não edite nada aqui

Os `.json` desta pasta são **saída de programa**. Eles são regravados inteiros a
cada `make docs-collect`.

Se você corrigir um valor aqui à mão, a correção funciona até a próxima coleta —
e some sem aviso nenhum. Pior: como alguém já "corrigiu" uma vez, o número passa
a parecer conferido.

**Se um número está errado, o erro está no coletor**, em
[`../../tooling/collectors/`](../../tooling/collectors/):

| Se o errado é… | O coletor é |
|---|---|
| modelo, camada, teste, descrição, linhagem | `dbt_models.py` |
| DAG, fonte de ingestão, cliente de API | `airflow_dags.py` |
| commit, entrega, pull request | `entregas.py` |

## Por que isto é versionado

Estes arquivos entram no git de propósito. O build do site roda **offline** — sem
rede, sem banco, sem dbt — e só consegue porque o acervo já está aqui. Sem ele,
a publicação automática não reproduz o site.

De quebra, o diff de uma coleta é o melhor resumo de período que existe: mostra,
em texto, tudo que mudou no repositório desde a última.
