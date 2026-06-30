# Guia de Contribuição — Gov Hub BR

Obrigado por considerar contribuir com o **Gov Hub BR**! O GovHub BR é uma plataforma open-source com o propósito de transformar dados públicos em ativos estratégicos para a administração pública e a sociedade. Toda contribuição — seja código, pipelines de dados, documentação, ideias ou feedback — é bem-vinda.

---

## Índice

- [Código de Conduta](#código-de-conduta)
- [Estrutura do Projeto](#estrutura-do-projeto)
- [Configurando o Ambiente Local](#configurando-o-ambiente-local)
- [Convenções de Branch e Commit](#convenções-de-branch-e-commit)
- [Fluxo de Pull Request](#fluxo-de-pull-request)
- [Protocolo de Aprovação de Pull Requests](#protocolo-de-aprovação-de-pull-requests)
- [Como Pegar ou Atribuir Issues](#como-pegar-ou-atribuir-issues)
- [Processo de Code Review](#processo-de-code-review)
- [Executando os Testes](#executando-os-testes)
- [Padrões de Código e Lint](#padrões-de-código-e-lint)
- [Boas Práticas Gerais](#boas-práticas-gerais)
- [FAQ](#faq)

---

## Código de Conduta

Esperamos que todas as pessoas contribuam com respeito, colaboração e responsabilidade. Comportamentos inadequados, ofensivos ou discriminatórios não serão tolerados.

---

## Estrutura do Projeto

```
.
├── .github/
│   ├── actions/
│   ├── TEMPLATES/
│   ├── workflows/
│   ├── PULL_REQUEST_TEMPLATE.md
│   ├── MERGE_REQUEST_PROTOCOL.md
│   └── CONTRIBUTING.md        # Este arquivo
├── airflow_lappis/
│   ├── dags/
│   │   ├── dashboards/
│   │   ├── data_ingest/
│   │   └── dbt/
│   ├── helpers/
│   ├── plugins/
│   ├── templates/
│   └── airflow.cfg
├── docker/
├── logs/
├── notebooks/
├── superset/
├── tests/
├── docker-compose.yml
├── Makefile
├── pyproject.toml
├── local.env
├── setup-git-hooks.sh
└── README.md
```

> Consulte a [documentação de arquitetura](https://gov-hub.io/govhub/documentacao/arquitetura/) para entender o fluxo completo dos dados.

---

## Configurando o Ambiente Local

### Pré-requisitos

- [Docker](https://docs.docker.com/get-docker/) e [Docker Compose](https://docs.docker.com/compose/)
- [Git](https://git-scm.com/)
- Python = 3.11
- [Poetry](https://python-poetry.org/docs/)
- Make
- Acesso às credenciais dos sistemas estruturantes (quando necessário) — veja o [guia de credenciais](https://gov-hub.io/govhub/documentacao/tutoriais/sistemas-estruturantes/acesso-apis-siafi-siape/)

### Passo a passo

#### 1. Faça o fork pelo GitHub e clone o seu fork
```bash
git clone https://github.com/<seu_usuario>/data-application-gov-hub.git
cd data-application-gov-hub
```

#### 2. Adicione o repositório original como remote "upstream"
```bash
git remote add upstream https://github.com/GovHub-br/data-application-gov-hub.git
```

#### 3. Execute a configuração usando Make
```bash
make setup
```

Isso irá instalar as dependências via Poetry, configurar os git hooks via `setup-git-hooks.sh` e preparar o ambiente de desenvolvimento.

#### 4. Copie as variáveis de ambiente e configure conforme necessário
```bash
cp local.env .env
```

#### 5. Suba o ambiente com Docker Compose
```bash
docker compose up -d
```

Os serviços estarão disponíveis em:
- **Apache Airflow:** http://localhost:8080
- **Apache Superset:** http://localhost:8088
- **Jupyter:** http://localhost:8888

Para instruções detalhadas, consulte a [documentação de instalação](https://gov-hub.io/govhub/documentacao/instalacao/).

---

## Convenções de Branch e Commit

Os mesmos prefixos são usados tanto no nome da branch quanto na mensagem de commit, seguindo o padrão [Conventional Commits](https://www.conventionalcommits.org/pt-br/):

| Tipo | Quando usar |
|------|-------------|
| `feat` | Nova funcionalidade ou pipeline |
| `fix` | Correção de bug ou inconsistência de dados |
| `docs` | Alterações apenas em documentação |
| `refactor` | Refatoração sem mudança de comportamento |
| `perf` | Melhoria de desempenho |
| `test` | Adição ou correção de testes |
| `build` | Mudanças em dependências ou sistema de build |
| `ci` | Mudanças em CI/CD ou infraestrutura |
| `chore` | Ajustes que não afetam código-fonte ou testes |
| `style` | Formatação que não afeta lógica |

**Branch:** `<tipo>/<descricao-curta>`

```bash
git checkout -b feat/integracao-siafi-despesas
git checkout -b fix/corrigir-modelo-silver-servidores
git checkout -b docs/atualizar-dicionario-siape
```

**Commit:** `<tipo>(<escopo opcional>): <descrição clara e objetiva no imperativo>`

```bash
git commit -m "feat(dbt): adicionar modelo gold de execução orçamentária por UG"
git commit -m "fix(dag): corrigir timeout na DAG de ingestão do SIAPE"
git commit -m "docs: adicionar dicionário de dados para domínio de pessoal"
git commit -m "ci: ajustar configuração do Astronomer Cosmos para DBT 1.8"
 
# Referenciando uma issue
git commit -m "feat(dbt): criar snapshot de cargos e funções
 
Closes #42"
```


Quando necessário, use a descrição estendida para detalhar motivações, impactos e decisões técnicas relevantes. Para um guia completo com todos os tipos, exemplos e uso de rodapés, consulte o [`commit_template.md`](TEMPLATES/COMMIT_TEMPLATE.md).

---

## Fluxo de Pull Request

```
fork → branch → commits → push → Pull Request → review → merge
```

```bash
# Mantenha sua branch atualizada com o upstream antes de enviar
git fetch upstream
git rebase upstream/main

# Faça push da sua branch para o seu fork
git push origin feat/integracao-siafi-despesas
```

Em seguida, abra um Pull Request no GitHub apontando para a branch `main` do repositório principal. O modelo de PR está disponível em `.github/PULL_REQUEST_TEMPLATE.md` e será preenchido automaticamente ao abrir um PR.

As regras obrigatórias de abertura, revisão, aprovação e merge estão documentadas no [Protocolo de Aprovação de Pull Requests](MERGE_REQUEST_PROTOCOL.md).

### Checklist antes de abrir o PR

- [ ] As alterações funcionam corretamente no ambiente local
- [ ] Os testes passam (`make test`)
- [ ] O lint não aponta erros (`make lint`)
- [ ] A branch está atualizada com `upstream/main` ou `origin/main`
- [ ] O título segue o padrão Conventional Commits
- [ ] A issue relacionada está referenciada (`Closes #<número>`)
- [ ] Documentação atualizada, se aplicável

---

## Protocolo de Aprovação de Pull Requests

O fluxo formal de revisão e aprovação está definido no [Protocolo de Aprovação de Pull Requests](MERGE_REQUEST_PROTOCOL.md).

Esse protocolo detalha critérios obrigatórios de documentação, testes, lint, revisão por domínio, quantidade mínima de aprovações, regras para merges urgentes e responsabilidades de autores e revisores.

---

## Como Pegar ou Atribuir Issues

1. Navegue até a aba [**Issues**](https://github.com/GovHub-br/data-application-gov-hub/issues) do repositório.
2. Filtre por `OSS` para começar.
3. Verifique se a issue já tem alguém atribuído.
4. Comente manifestando interesse: **"Posso trabalhar nessa issue?"**.
5. Aguarde a confirmação de um mantenedor antes de iniciar.

> Não abra PRs para issues já atribuídas a outro contribuidor sem combinar antes.

Toda solicitação de mudança ou sugestão deve ser registrada como issue.

---

## Processo de Code Review

Consulte o [Protocolo de Aprovação de Pull Requests](MERGE_REQUEST_PROTOCOL.md) para os critérios obrigatórios de revisão, aprovação e merge.

O repositório usa domínios de revisão (`IPEA`, `MIR`, `MCid`, `MinC` e `OSS`) e a workflow `Request team review` para solicitar revisões conforme as labels `team:*` aplicadas manualmente no PR. PRs originados na disciplina GCES devem usar a label `team:gces`, que solicita revisão do time `OSS`.

**Para quem submete o PR:** responda a todos os comentários de revisão, implemente as mudanças em novos commits e aguarde nova aprovação antes do merge.

**Para quem faz o review:** seja construtivo e específico, explique o *porquê* das sugestões, diferencie bloqueadores de sugestões opcionais, e verifique qualidade dos modelos DBT, linhagem de dados, testes e impacto em pipelines existentes. O merge é responsabilidade dos mantenedores.

---

## Executando os Testes

```bash
# Executar todos os testes
make test

# Testes DBT por modelo específico
cd airflow_lappis/dags/dbt/<projeto>
dbt test --select <nome_do_modelo>

# Compilar modelos sem executar (validação de SQL)
dbt compile

# Testar execução de uma DAG específica
airflow dags test <nome_da_dag> <data_execucao>
```

Consulte a documentação de [testes DBT](https://gov-hub.io/govhub/documentacao/tutoriais/dbt/testes/) para mais detalhes.

> Toda contribuição que adiciona ou altera modelos DBT deve incluir testes no `schema.yml` correspondente (ex.: `not_null`, `unique`, `accepted_values`).

---

## Padrões de Código e Lint

### DBT / SQL

- Siga a [Arquitetura Medallion](https://gov-hub.io/govhub/documentacao/tutoriais/dbt/arquitetura-medallion/) (raw → bronze → silver → gold).
- Faça a separação clara por pastas: `models/bronze`, `models/silver`, `models/gold`.
- Use sufixos ou prefixos que indiquem a camada (ex: `contratos_bronze`, `contratos_silver`).
- Documente cada modelo e coluna no `schema.yml` correspondente.
- Use [macros DBT](https://gov-hub.io/govhub/documentacao/tutoriais/dbt/macros/) para lógica reutilizável.
- As regras de lint SQL estão definidas em `.sqlfluff` e `.sqlfluffignore` na raiz do projeto.

```bash
make lint      # Executa sqlfluff para SQL e ruff para Python
make format    # Aplica formatação automática
```

---

## Boas Práticas Gerais

- **PRs pequenos e focados:** um PR por funcionalidade ou correção facilita o review.
- **Sem credenciais no código:** use variáveis de ambiente. Veja o [guia de credenciais](https://gov-hub.io/govhub/documentacao/tutoriais/sistemas-estruturantes/acesso-apis-siafi-siape/).
- **Sem dados sensíveis:** não inclua dados reais de servidores públicos ou cidadãos em testes ou exemplos.
- **Documente junto ao código:** atualize `schema.yml` e a documentação no mesmo PR da mudança.
- **Novas dependências:** discuta em uma issue antes de adicionar novos pacotes ou serviços.
- **Idioma:** código e comentários em **inglês**; issues, PRs e documentação em **português**.

---

## FAQ

**Posso contribuir sem ter sido atribuído a uma issue?**
> Não. Toda contribuição deve ter uma issue registrada antes. Abra uma issue usando o modelo disponível em `.github/TEMPLATES/`, preencha o contexto e aguarde a atribuição antes de começar.

**Não tenho acesso ao SIAFI/SIAPE. Posso contribuir?**
> Sim! Documentação, testes DBT com dados sintéticos, melhorias de infraestrutura e dashboards no Superset não exigem acesso a APIs governamentais.

**Onde posso tirar dúvidas técnicas?**
> Consulte os [tutoriais da documentação](https://gov-hub.io/govhub/documentacao/instalacao/) ou entre em contato com a equipe mantenedora.

**Meu PR foi fechado sem merge. O que faço?**
> Leia o motivo no comentário de fechamento, corrija os pontos indicados e abra um novo PR referenciando o anterior.

---

<div align="center">
  Feito com 💜 pela comunidade GovHub BR &nbsp;·&nbsp;
  <a href="https://github.com/GovHub-br/data-application-gov-hub/issues">Reportar problema</a> &nbsp;·&nbsp;
  <a href="https://gov-hub.io/govhub/">Documentação</a>
</div>
