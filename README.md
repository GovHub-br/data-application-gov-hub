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


# Data Pipeline Project

O Data Pipeline Project é uma solução moderna que utiliza ferramentas como Airflow, DBT, Jupyter e Superset para orquestração, transformação, análise e visualização de dados. 

## 🚀 Stack do projeto

- **Apache Airflow**: Orquestração de workflows
- **dbt**: Transformação de dados
- **Jupyter**: Análise de dados interativa
- **Apache Superset**: Visualização e exploração de dados
- **Docker**: Containerização e desenvolvimento local
- **Make**: Automação de build e configuração

## 📋 Pré-requisitos

- Docker e Docker Compose
- Make
- Python 3.11.x
- Git

## 🔧 Setup

1. Clone o repositório:
```bash
git clone git@github.com:GovHub-br/data-application-gov-hub.git
cd data-application-gov-hub
```

2. Execute a configuração usando Make:
```bash
make setup
```

- Isso irá:
    - Criar os ambientes virtuais necessários
    - Instalar as dependências
    - Configurar os hooks de pre-commit
    - Preparar o ambiente de desenvolvimento


3. Configuração de ambiente

Este projeto depende de variáveis de ambiente para o desenvolvimento local.

Você pode configurá-las seguindo **[este guia](https://gov-hub.io/govhub/documentacao/instalacao/)**.


## 🏃‍♂️ Executando localmente

Inicie todos os serviços usando Docker Compose:

```bash
docker-compose up -d
```

Acesse os diferentes componentes:
- Airflow: http://localhost:8080
- Jupyter: http://localhost:8888
- Superset: http://localhost:8088

## 💻 Desenvolvimento

### Qualidade de Código

Este projeto utiliza diversas ferramentas para manter a qualidade do código:
- Hooks de pre-commit
- Configurações de lint
- Testes automatizados

Execute a verificação de lint:
```bash
make lint
```

Execute os testes:
```bash
make test
```

### Estrutura do projeto

```
.
├── airflow/
│   ├── dags/
│   └── plugins/
├── dbt/
│   └── models/
├── jupyter/
│   └── notebooks/
├── superset/
│   └── dashboards/
├── docker-compose.yml
├── Makefile
└── README.md
```

### Comandos do Makefile

- `make setup`: Configuração inicial do projeto
- `make lint`: Executa verificações de lint
- `make tests`: Executa a suíte de testes
- `make clean`: Remove arquivos gerados
- `make build`: Constrói as imagens Docker

## 🔐 Fluxo de Trabalho com Git

Este projeto exige commits assinados. Para configurar a assinatura com GPG:

1. Gere uma chave GPG:
```bash
gpg --full-generate-key
```

2. Configure o Git para usar assinatura GPG:
```bash
git config --global user.signingkey SUA_KEY_ID
git config --global commit.gpgsign true
```

3. Adicione sua chave GPG à sua conta do GitLab

## 📚 Documentação

- [Documentação do Airflow](https://airflow.apache.org/docs/)
- [Documentação do dbt](https://docs.getdbt.com/)
- [Documentação do Superset](https://superset.apache.org/docs/intro)
- [Documentação do GovHub](https://gov-hub.io/govhub/documentacao/instalacao/)

## 🤝 Contribuição

Antes de contribuir, leia o [Guia de Contribuição](.github/CONTRIBUTING.md) e o [Protocolo de Aprovação de Pull Requests](.github/MERGE_REQUEST_PROTOCOL.md), que definem o fluxo de branches, commits, Pull Requests, revisão de código, testes e lint.

Resumo do fluxo:

1. Crie uma branch seguindo o padrão `<tipo>/<descricao-curta>`
2. Faça commits seguindo Conventional Commits
3. Garanta que testes e lint passam
4. Abra um Pull Request usando o template do repositório
