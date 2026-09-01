# Gov Hub BR - Transformando Dados em Valor para Gestão Pública

O Gov Hub BR é uma iniciativa para enfrentar os desafios da fragmentação,
redundância e inconsistências nos sistemas estruturantes do governo federal. O
projeto busca transformar dados públicos em ativos estratégicos, promovendo
eficiência administrativa, transparência e melhor tomada de decisão. A partir da
integração de dados, gestores públicos terão acesso a informações qualificadas
para subsidiar decisões mais assertivas, reduzir custos operacionais e otimizar
processos internos.

Potencializamos informações de sistemas como TransfereGov, Siape, Siafi,
ComprasGov e Siorg para gerar diagnósticos estratégicos, indicadores confiáveis
e decisões baseadas em evidências.

![Informações do Projeto](https://github.com/GovHub-br/gov-hub/blob/main/docs/land/dist/images/imagem_informacoes.jpg)

- Transparência pública e cultura de dados abertos
- Indicadores confiáveis para acompanhamento e monitoramento
- Decisões baseadas em evidências e diagnósticos estratégicos
- Exploração de inteligência artificial para gerar insights
- Gestão orientada a dados em todos os níveis

## Fluxo/Arquitetura de Dados

A arquitetura do Gov Hub BR é baseada na Arquitetura Medallion, em um fluxo de
dados que permite a coleta, transformação e visualização de dados.

![Fluxo de Dados](https://github.com/GovHub-br/gov-hub/blob/main/fluxo_dados.jpg)

Para mais informações sobre o projeto, veja o nosso
[e-book](https://github.com/GovHub-br/gov-hub/blob/main/docs/land/dist/ebook/GovHub_Livro-digital_0905.pdf).
Também temos alguns slides falando do projeto e como ele pode ajudar a
transformar a gestão pública.

[Slides](https://www.figma.com/slides/PlubQE0gaiBBwFAV5GcVlH/Gov-Hub---F%C3%B3rum-IA---Giga-candanga?node-id=5-131&t=hlLiJiwfyPEPRFys-1)

## Apoio

Esse trabalho é mantido pelo [Lab Livre](https://www.instagram.com/lab.livre/)
e apoiado pelo
[IPEA/Dides](https://www.ipea.gov.br/portal/categorias/72-estrutura-organizacional/210-dides-estrutura-organizacional).

## Data Application

Este repositório reúne pipelines, modelos dbt, integrações e configurações
locais da aplicação de dados do Gov Hub BR.

### Stack do Projeto

- **Apache Airflow 2.8.1:** orquestração de workflows e DAGs.
- **dbt:** transformação, testes e documentação dos modelos analíticos.
- **PostgreSQL:** persistência relacional para dados ingeridos e transformados.
- **MinIO:** armazenamento de objetos para fluxos que usam landing zone.
- **Apache Superset:** visualização e exploração de dados.
- **Jupyter:** análise de dados interativa.
- **Docker Compose:** containerização e desenvolvimento local.
- **uv + Make:** gerenciamento de dependências e automação de comandos.

### Pré-requisitos

- Docker e Docker Compose
- Make
- Python 3.11
- Git
- [uv](https://docs.astral.sh/uv/getting-started/installation/)
- Acesso às credenciais necessárias para os sistemas integrados, quando a DAG
  depender de APIs, certificados ou secrets externos

### Setup

1. Clone o repositório:

```bash
git clone git@github.com:GovHub-br/data-application-gov-hub.git
cd data-application-gov-hub
```

2. Execute a configuração usando Make:

```bash
make setup
```

Esse comando cria `.env` a partir de `local.env` quando necessário, instala as
dependências com `uv`, atualiza o `requirements.txt` e configura os hooks de
Git.

3. Suba o ambiente local:

```bash
make compose
```

O `make compose` sobe os serviços com Docker Compose, configura
variáveis/conexões básicas do Airflow com `make dev` e valida a configuração com
`make dev-check`.

### Serviços Locais

Após subir o ambiente, os principais serviços ficam disponíveis em:

- Airflow: <http://localhost:8080>
- Superset: <http://localhost:8088>
- Jupyter: <http://localhost:8888>
- MinIO API: <http://localhost:9000>
- MinIO Console: <http://localhost:9001>

As credenciais e variáveis locais vêm de `.env`/`local.env`. Não versione
credenciais reais, tokens, certificados ou secrets de produção.

## Desenvolvimento

### Qualidade de Código

Este projeto utiliza hooks de Git, lint e testes automatizados para manter a
qualidade do código.

Execute a verificação de lint:

```bash
make lint
```

Execute os testes unitários:

```bash
make test
```

Execute os testes de integração:

```bash
make test-integration
```

### Estrutura do Projeto

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

### Comandos do Makefile

- `make setup`: configura `.env`, dependências, `requirements.txt` e hooks
- `make install`: instala dependências com `uv`
- `make requirements`: regenera o `requirements.txt`
- `make compose`: sobe o ambiente local e configura o Airflow
- `make dev`: configura variáveis e conexões locais do Airflow
- `make dev-check`: valida variáveis e conexão padrão do Airflow
- `make format`: formata código com Black e Ruff
- `make lint`: executa verificações de Black, Ruff e Ty
- `make test`: executa testes unitários
- `make test-integration`: executa testes de integração

### Airflow Local

O ambiente local monta os principais diretórios do repositório dentro do
container do Airflow:

- `airflow_lappis/dags` em `${AIRFLOW_HOME}/dags`
- `airflow_lappis/plugins` em `${AIRFLOW_HOME}/plugins`
- `airflow_lappis/helpers` em `${AIRFLOW_HOME}/helpers`
- `airflow_lappis/dags/dbt/ipea/profiles.yml` em `${AIRFLOW_HOME}/.dbt/profiles.yml`

O `make dev` configura variáveis como `airflow_orgao`,
`airflow_variables`, `dynamic_schedules` e a conexão `postgres_default` para
desenvolvimento local.

## Fluxo de Trabalho com Git

Se o repositório exigir commits assinados, configure sua chave GPG ou SSH no
GitHub e habilite a assinatura no Git antes de commitar.

## Documentação

- [Documentação do Airflow](https://airflow.apache.org/docs/)
- [Documentação do dbt](https://docs.getdbt.com/)
- [Documentação do Superset](https://superset.apache.org/docs/intro)
- [Documentação do GovHub](https://gov-hub.io/govhub/documentacao/)

## Contribuição

Antes de contribuir, leia o [Guia de Contribuição](.github/CONTRIBUTING.md), o
[Protocolo de Aprovação de Pull Requests](.github/MERGE_REQUEST_PROTOCOL.md) e o
[template de Pull Request](.github/PULL_REQUEST_TEMPLATE.md), que definem o
fluxo de branches, commits, Pull Requests, revisão de código, testes e lint.

Resumo do fluxo:

1. Crie uma branch seguindo o padrão `<tipo>/<descricao-curta>`.
2. Faça commits seguindo Conventional Commits.
3. Garanta que testes e lint passam ou justifique quando não se aplicarem.
4. Abra um Pull Request usando o template do repositório.
5. Use labels `team:*` quando a revisão por domínio for necessária.

## Contato

Para dúvidas, sugestões ou para contribuir com o projeto, entre em contato
conosco: [lablivreunb@gmail.com](mailto:lablivreunb@gmail.com).
