# Gov Hub BR - Transformando Dados em Valor para Gestão Pública

O Gov Hub BR é uma iniciativa para enfrentar os desafios da fragmentação, redundância e inconsistências nos sistemas estruturantes do governo federal. O projeto busca transformar dados públicos em ativos estratégicos, promovendo eficiência administrativa, transparência e melhor tomada de decisão. A partir da integração de dados, gestores públicos terão acesso a informações qualificadas para subsidiar decisões mais assertivas, reduzir custos operacionais e otimizar processos internos. 

Potencializamos informações de sistemas como TransfereGov, Siape, Siafi, ComprasGov e Siorg para gerar diagnósticos estratégicos, indicadores confiáveis e decisões baseadas em evidências.

![Informações do Projeto](https://github.com/GovHub-br/gov-hub/blob/main/docs/land/dist/images/imagem_informacoes.jpg)

- Transparência pública e cultura de dados abertos
- Indicadores confiáveis para acompanhamento e monitoramento
- Decisões baseadas em evidências e diagnósticos estratégicos
- Exploração de inteligência artificial para gerar insights
- Gestão orientada a dados em todos os níveis

## Fluxo/Arquitetura de Dados

A arquitetura do Gov Hub BR é baseada na Arquitetura Medallion,  em um fluxo de dados que permite a coleta, transformação e visualização de dados.

![Fluxo de Dados](https://github.com/GovHub-br/gov-hub/blob/main/fluxo_dados.jpg)

Para mais informações sobre o projeto, veja o nosso [e-book](https://github.com/GovHub-br/gov-hub/blob/main/docs/land/dist/ebook/GovHub_Livro-digital_0905.pdf).
E temos também alguns slides falando do projeto e como ele pode ajudar a transformar a gestão pública.

[Slides](https://www.figma.com/slides/PlubQE0gaiBBwFAV5GcVlH/Gov-Hub---F%C3%B3rum-IA---Giga-candanga?node-id=5-131&t=hlLiJiwfyPEPRFys-1)

## Apoio

Esse trabalho  é mantido pelo [Lab Livre](https://www.instagram.com/lab.livre/) e apoiado pelo [IPEA/Dides](https://www.ipea.gov.br/portal/categorias/72-estrutura-organizacional/210-dides-estrutura-organizacional).

## Contato

Para dúvidas, sugestões ou para contribuir com o projeto, entre em contato conosco: [lablivreunb@gmail.com](mailto:lablivreunb@gmail.com)


# Data Application MinC

Este repositório organiza a aplicação de dados em torno do Airflow e do dbt. A
raiz contém o código executado pelo Airflow; a pasta `infra/` concentra Docker,
Compose e arquivos de suporte para o ambiente local.

## Stack

- **Apache Airflow**: orquestração dos pipelines
- **dbt**: transformação dos dados
- **PostgreSQL**: banco local para desenvolvimento
- **Docker Compose**: execução local dos serviços
- **Make**: automação de comandos de desenvolvimento

## Estrutura

```text
.
├── dags/                 # DAGs carregadas pelo Airflow
│   ├── data_ingest/
│   ├── dashboards/
│   └── dbt/              # DAGs Cosmos que executam os projetos dbt
├── dbt/                  # Projetos dbt fora do parser de DAGs
│   ├── ipea/
│   └── mir/
├── helpers/              # Utilitários importados pelas DAGs
├── plugins/              # Clientes e extensões usados pelo Airflow
├── templates/            # Templates Jinja/XML usados pelos clientes
├── infra/                # Docker, compose, Airflow config e init de banco
├── tests/
├── Makefile
├── pyproject.toml
└── requirements.txt
```

## Setup

```bash
make setup
```

Para usar Docker Compose, mantenha um `.env` na raiz do projeto. Um exemplo de
variáveis esperadas está em `infra/env/.env.example`.

## Rodando Localmente

```bash
make up
```

Serviços principais:

- Airflow: http://localhost:8080
- Airflow MCP: http://localhost:8000
- PostgreSQL: localhost:5432

Comandos úteis:

```bash
make compose-config
make logs-airflow
make down
```

## Desenvolvimento

```bash
make format
make lint
make test
```

## Git Workflow

This project requires signed commits. To set up GPG signing:

1. Generate a GPG key:
```bash
gpg --full-generate-key
```

2. Configure Git to use GPG signing:
```bash
git config --global user.signingkey YOUR_KEY_ID
git config --global commit.gpgsign true
```

3. Add your GPG key to your GitLab account

## Documentation

- [Airflow Documentation](https://airflow.apache.org/docs/)
- [dbt Documentation](https://docs.getdbt.com/)

## Contributing

1. Create a new branch for your feature
2. Make changes and ensure all tests pass
3. Submit a merge request
