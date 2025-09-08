# Gov Hub BR - Transformando Dados em Valor para Gestão Pública

O Gov Hub BR é uma iniciativa para enfrentar os desafios da fragmentação, redundância e inconsistências nos sistemas estruturantes do governo federal. O projeto busca transformar dados públicos em ativos estratégicos, promovendo eficiência administrativa, transparência e melhor tomada de decisão. A partir da integração de dados, gestores públicos terão acesso a informações qualificadas para subsidiar decisões mais assertivas, reduzir custos operacionais e otimizar processos internos. 

Potencializamos informações de sistemas como TransfereGov, Siape, Siafi, ComprasGov e Siorg para gerar diagnósticos estratégicos, indicadores confiáveis e decisões baseadas em evidências.

![Informações do Projeto](docs/land/dist/images/imagem_informacoes.jpg)

- Transparência pública e cultura de dados abertos
- Indicadores confiáveis para acompanhamento e monitoramento
- Decisões baseadas em evidências e diagnósticos estratégicos
- Exploração de inteligência artificial para gerar insights
- Gestão orientada a dados em todos os níveis

## Fluxo/Arquitetura de Dados

A arquitetura do Gov Hub BR é baseada na Arquitetura Medallion,  em um fluxo de dados que permite a coleta, transformação e visualização de dados.

![Fluxo de Dados](fluxo_dados.jpg)

Para mais informações sobre o projeto, veja o nosso [e-book](docs/land/dist/ebook/GovHub_Livro-digital_0905.pdf).
E temos também alguns slides falando do projeto e como ele pode ajudar a transformar a gestão pública.

[Slides](https://www.figma.com/slides/PlubQE0gaiBBwFAV5GcVlH/Gov-Hub---F%C3%B3rum-IA---Giga-candanga?node-id=5-131&t=hlLiJiwfyPEPRFys-1)

## Apoio

Esse trabalho  é mantido pelo [Lab Livre](https://www.instagram.com/lab.livre/) e apoiado pelo [IPEA/Dides](https://www.ipea.gov.br/portal/categorias/72-estrutura-organizacional/210-dides-estrutura-organizacional).

## Contato

Para dúvidas, sugestões ou para contribuir com o projeto, entre em contato conosco: [lablivreunb@gmail.com](mailto:lablivreunb@gmail.com)

# Data Pipeline Project

This project implements a modern data stack using Airflow, dbt, Jupyter, and Superset for data orchestration, transformation, analysis, and visualization.

## 🚀 Stack Components

- **Apache Airflow**: Workflow orchestration
- **dbt**: Data transformation
- **Jupyter**: Interactive data analysis
- **Apache Superset**: Data visualization and exploration
- **Docker**: Containerization and local development
- **Make**: Build automation and setup

## 📋 Prerequisites

- Docker and Docker Compose
- Make
- Python 3.x
- Git

## 🔧 Setup

1. Clone the repository:
```bash
git clone git@gitlab.com:lappis-unb/gest-odadosipea/app-lappis-ipea.git
cd app-lappis-ipea
```

2. Run the setup using Make:
```bash
make setup
```

This will:
- Create necessary virtual environments
- Install dependencies
- Set up pre-commit hooks
- Configure development environment

## 🏃‍♂️ Running Locally

Start all services using Docker Compose:

```bash
docker-compose up -d
```

Access the different components:
- Airflow: http://localhost:8080
- Jupyter: http://localhost:8888
- Superset: http://localhost:8088

## 💻 Development

### Code Quality

This project uses several tools to maintain code quality:
- Pre-commit hooks
- Linting configurations
- Automated testing

Run linting checks:
```bash
make lint
```

Run tests:
```bash
make test
```

### Project Structure

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

### Makefile Commands

- `make setup`: Initial project setup
- `make lint`: Run linting checks
- `make tests`: Run test suite
- `make clean`: Clean up generated files
- `make build`: Build Docker images

## 🔐 Git Workflow

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

## 📚 Documentation

- [Airflow Documentation](https://airflow.apache.org/docs/)
- [dbt Documentation](https://docs.getdbt.com/)
- [Superset Documentation](https://superset.apache.org/docs/intro)

## 🤝 Contributing

1. Create a new branch for your feature
2. Make changes and ensure all tests pass
3. Submit a merge request
