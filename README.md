# Gov Hub BR - Data Application

Este repositório reúne pipelines, modelos dbt, integrações e configurações locais
da aplicação de dados do Gov Hub BR.

O Gov Hub BR é uma iniciativa para enfrentar desafios de fragmentação,
redundância e inconsistências em sistemas estruturantes do governo federal. O
projeto busca transformar dados públicos em ativos estratégicos para apoiar
gestão pública, transparência, interoperabilidade e tomada de decisão baseada em
evidências.

## Stack do Projeto

- **Apache Airflow 2.8.1:** orquestração das DAGs de ingestão, transformação e
  rotinas auxiliares.
- **dbt:** transformação, testes e documentação dos modelos analíticos.
- **PostgreSQL:** persistência relacional para dados ingeridos e transformados.
- **MinIO:** armazenamento de objetos para fluxos que usam landing zone.
- **Apache Superset:** visualização e exploração de dados.
- **Jupyter:** análises exploratórias locais.
- **Docker Compose:** execução do ambiente local.
- **uv + Make:** gerenciamento de dependências e automação de comandos.

## Pré-Requisitos

- Git
- Docker e Docker Compose
- Make
- Python 3.11
- [uv](https://docs.astral.sh/uv/getting-started/installation/)
- Acesso às credenciais necessárias para os sistemas integrados, quando a DAG
  depender de APIs, certificados ou secrets externos

## Setup Local

1. Clone o repositório:

```bash
git clone git@github.com:GovHub-br/data-application-gov-hub.git
cd data-application-gov-hub
```

2. Configure dependências, ambiente local e hooks:

```bash
make setup
```

O comando cria `.env` a partir de `local.env` quando o arquivo ainda não existe,
instala as dependências com `uv`, atualiza o `requirements.txt` e configura os
hooks de Git.

3. Suba o ambiente local:

```bash
make compose
```

Esse comando executa o Docker Compose, configura variáveis/conexões básicas do
Airflow com `make dev` e valida a configuração com `make dev-check`.

## Serviços Locais

Após subir o ambiente, os principais serviços ficam disponíveis em:

| Serviço | URL |
| --- | --- |
| Airflow | <http://localhost:8080> |
| Superset | <http://localhost:8088> |
| Jupyter | <http://localhost:8888> |
| MinIO API | <http://localhost:9000> |
| MinIO Console | <http://localhost:9001> |

As credenciais e variáveis locais vêm de `.env`/`local.env`. Não versione
credenciais reais, tokens, certificados ou secrets de produção.

## Estrutura do Repositório

```text
.
├── .github/
│   ├── workflows/
│   ├── actions/
│   ├── CONTRIBUTING.md
│   ├── MERGE_REQUEST_PROTOCOL.md
│   └── PULL_REQUEST_TEMPLATE.md
├── airflow_lappis/
│   ├── airflow.cfg
│   ├── dags/
│   │   ├── dashboards/
│   │   ├── data_ingest/
│   │   ├── dbt/
│   │   │   └── ipea/
│   │   └── homologation/
│   ├── helpers/
│   ├── plugins/
│   └── templates/
├── docker/
│   └── postgres/
├── superset/
├── tests/
│   ├── integration/
│   ├── test_helpers/
│   ├── test_plugins/
│   └── unit/
├── Dockerfile
├── Dockerfile.superset
├── docker-compose.yml
├── local.env
├── Makefile
├── pyproject.toml
├── requirements.txt
├── uv.lock
└── README.md
```

## Comandos Úteis

| Comando | Uso |
| --- | --- |
| `make setup` | Prepara `.env`, instala dependências, gera `requirements.txt` e configura hooks |
| `make install` | Instala dependências com `uv` |
| `make requirements` | Regenera o `requirements.txt` a partir do `uv` |
| `make compose` | Sobe o ambiente local e configura o Airflow |
| `make dev` | Configura variáveis e conexões locais do Airflow |
| `make dev-check` | Valida variáveis e conexão padrão do Airflow |
| `make format` | Formata código com Black e Ruff |
| `make lint` | Executa verificações de Black, Ruff e Ty |
| `make test` | Executa testes unitários e gera relatórios |
| `make test-integration` | Sobe serviços necessários e executa testes de integração |

## Airflow Local

O ambiente local monta o código do repositório dentro do container do Airflow:

- `airflow_lappis/dags` em `${AIRFLOW_HOME}/dags`
- `airflow_lappis/plugins` em `${AIRFLOW_HOME}/plugins`
- `airflow_lappis/helpers` em `${AIRFLOW_HOME}/helpers`
- `airflow_lappis/dags/dbt/ipea/profiles.yml` em `${AIRFLOW_HOME}/.dbt/profiles.yml`

O `make dev` configura variáveis como `airflow_orgao`,
`airflow_variables`, `dynamic_schedules` e a conexão `postgres_default` para
desenvolvimento local.

## Desenvolvimento

Antes de abrir um Pull Request, execute:

```bash
make lint
make test
```

Para mudanças em DAGs, valide também no Airflow:

```bash
docker compose exec airflow airflow dags list
docker compose exec airflow airflow dags test <dag_id> <data_execucao>
```

Para mudanças em modelos dbt, execute os comandos dentro do projeto alterado:

```bash
cd airflow_lappis/dags/dbt/ipea
dbt run --select <modelo_ou_dominio>
dbt test --select <modelo_ou_dominio>
```

## Contribuição

Antes de contribuir, leia:

- [Guia de Contribuição](.github/CONTRIBUTING.md)
- [Protocolo de Aprovação de Pull Requests](.github/MERGE_REQUEST_PROTOCOL.md)
- [Template de Pull Request](.github/PULL_REQUEST_TEMPLATE.md)

Resumo do fluxo:

1. Crie uma branch seguindo o padrão `<tipo>/<descricao-curta>`.
2. Faça commits seguindo Conventional Commits.
3. Rode lint e testes aplicáveis.
4. Abra um Pull Request usando o template do repositório.
5. Use labels `team:*` quando a revisão por domínio for necessária.

Se o repositório exigir commits assinados, configure sua chave GPG ou SSH no
GitHub e habilite a assinatura no Git antes de commitar.

## Documentação

- [Documentação do GovHub](https://gov-hub.io/govhub/documentacao/)
- [Apache Airflow](https://airflow.apache.org/docs/)
- [dbt](https://docs.getdbt.com/)
- [Apache Superset](https://superset.apache.org/docs/intro)

## Contato

Para dúvidas, sugestões ou contribuições, entre em contato com o Lab Livre:
[lablivreunb@gmail.com](mailto:lablivreunb@gmail.com).
