# data-application-minc

Pipeline de dados do Ministério da Cultura no Gov Hub BR. Ingere dados de sistemas
de governo (TransfereGov, BB Ágil, SALIC, FCU), transforma com dbt em Postgres e
serve o acompanhamento das metas do PNAB e da Lei Paulo Gustavo.

## Stack

Airflow 3.2 orquestra, Cosmos executa o dbt, Postgres é o destino, Docker Compose
sobe tudo local. Dependências via Poetry.

## Mapa do repositório

| Pasta | O que tem | Você edita? |
|---|---|---|
| `dags/data_ingest/` | uma DAG por endpoint de origem — 10 hoje | Sim |
| `dags/dbt/minc_cosmos_dag.py` | a DAG que roda o projeto dbt inteiro | Raramente |
| `dbt/minc/models/` | dois domínios: `cotas_dbt` (24 modelos) e `agentes_dbt` (11), cada um em bronze → silver → gold | Sim |
| `dbt/minc/models/sources.yml` | declaração das tabelas de origem | Sim |
| `dbt/minc/target/` | saída do `dbt run` e do `dbt docs generate` | **Não.** Gerada, e ignorada pelo git |
| `plugins/` | clientes de API (`cliente_*.py`), autenticação e regras de negócio | Sim |
| `helpers/` | utilidades importadas pelas DAGs — Postgres, retry, requisição segura | Sim |
| `infra/` | Docker, Compose, configuração do Airflow e do Superset | Sim |
| `docs-pages/` | o site de documentação publicado no GitHub Pages | Sim — mas só o `src/dominios.yml`. Ver abaixo |
| `docs/adr/` | registro das decisões de arquitetura | Sim, uma por decisão que custou discussão |
| `data/` | extrações brutas geradas por quem roda as DAGs | **Não.** Ignorada pelo git, só a estrutura de pastas é versionada |
| `tests/` | pytest | Sim |

## Comandos

```bash
make setup           # instala dependências e os git hooks
make format          # black + ruff --fix + sqlfmt
make lint            # black --check, ruff, mypy, sqlfmt, sqlfluff
make test            # pytest
make up              # sobe postgres, airflow e airflow-mcp
make down            # derruba
make logs-airflow    # últimas 200 linhas do Airflow

make docs-collect    # relê o repositório e atualiza o acervo do site (usa rede)
make docs-serve      # constrói e serve o site em localhost:8000 (offline)
```

`make lint-ci` é o alvo que a CI roda, e cobre só SQL. A CI **não reprova** o PR
quando ele falha — a chamada tem `|| true`, por decisão da equipe. Rode `make lint`
localmente antes de abrir PR; o checklist do template pede que você afirme isso.

## Convenções

**Branch.** Dois formatos, herdados do protocolo do `data-application-gov-hub`:

```text
<tipo>/<descricao-curta>              feat/ingestao-salic
<numero-da-issue>-<tipo>-<descricao>  24-fix-dag-nota-de-credito
```

O segundo é o que o workflow `issue-para-branch.yml` gera sozinho quando uma issue
recebe a label `Código`.

**Commit.** Conventional Commits. O guia completo está em
[`.github/TEMPLATES/COMMIT_TEMPLATE.md`](.github/TEMPLATES/COMMIT_TEMPLATE.md).

**Issue.** Sempre por formulário — os seis tipos estão em `.github/ISSUE_TEMPLATE/`.
A caixa em branco está desligada de propósito.

## Site de documentação

O site publicado em [`docs-pages/`](docs-pages/) lê os fatos do próprio
repositório a cada coleta — modelos dbt, DAGs, clientes, entregas. **O único
arquivo que se edita à mão é [`docs-pages/src/dominios.yml`](docs-pages/src/dominios.yml)**,
onde mora a narrativa: o que cada conjunto de dados significa e o que é preciso
saber antes de citar seus números.

Se um **número** está errado no site, a correção é no coletor, nunca no texto.
Se uma **explicação** está errada, é no `dominios.yml`.

A publicação é automática; a coleta **não**. Quem altera um modelo ou uma DAG
roda `make docs-collect` e commita o acervo no mesmo pull request — senão o site
continua descrevendo o estado anterior, sem nenhum sinal de que está velho. O
detalhe está em [`docs-pages/README.md`](docs-pages/README.md).

## Skills

As skills deste repositório ficam em [`.claude/skills/`](.claude/skills/) e são
versionadas: quem clona recebe todas, sem instalar nada. O inventário e o que cada
uma faz está em [`.claude/skills/README.md`](.claude/skills/README.md).

Para criar uma skill nova, o critério é: **se o conhecimento dela sobrevive a um
`git mv` neste repositório, ele pertence ao
[GovHub-skills](https://github.com/GovHub-br/GovHub-skills), não aqui.** Uma skill
que cita `dbt/minc` ou `plugins/cliente_*.py` mora aqui, porque quebra junto com um
refactor daqui e o mesmo PR conserta as duas coisas. Uma que fala de Postgres em
geral pertence lá, para outros projetos aproveitarem.

## Segredos

Nenhum valor real de credencial entra em arquivo versionado, nunca — nem em
exemplo, nem em comentário, nem em teste. Os arquivos `.env` de todas as pastas
estão no `.gitignore`; o `local.env` é template sanitizado e **não deve ser
reescrito com valores reais**. Credencial vazada em commit continua no histórico
mesmo depois de removida do working tree.

Ao ler logs, output de DAG ou dump de banco, trate como dado sensível: o repositório
lida com CPF, CNPJ e dados de raça e deficiência de agentes culturais.
