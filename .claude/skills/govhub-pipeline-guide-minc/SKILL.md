---
name: govhub-pipeline-guide-minc
description: >-
  Guia de ponta a ponta para trabalhar no pipeline deste repositório — criar
  cliente de API, DAG de ingestão e modelos dbt de bronze a gold, com testes e
  documentação. Use sempre que o usuário pedir para ingerir uma fonte nova,
  criar ou corrigir uma DAG, escrever ou consertar modelo dbt, resolver uma issue
  de ingestão ou de dbt, ou perguntar onde se mexe em alguma coisa do pipeline
  do MinC. Também quando disser "resolve a issue #N" e a issue for de dados.
allowed-tools: Bash, Read, Grep, Glob, Edit, Write
---

# Pipeline do data-application-minc

Este repositório ingere dados de sistemas de governo, transforma com dbt em
Postgres e serve o acompanhamento das metas do PNAB e da Lei Paulo Gustavo.

Adaptado da `govhub-pipeline-guide` do
[GovHub-skills](https://github.com/GovHub-br/GovHub-skills), que descreve o
`data-application-gov-hub` — outro repositório, com outros projetos dbt. **Nada
daquela versão vale aqui sem conferir**: lá são os projetos `mir` e `ipea`, aqui
é `dbt/minc`.

## O caminho de um dado

```
API de origem  →  plugins/cliente_<sistema>.py   (fala HTTP, devolve dict)
               →  dags/data_ingest/<fonte>/*.py  (orquestra e grava no Postgres)
               →  schema raw / bronze            (o dado como veio)
               →  dbt/minc/models/<dominio>/     (bronze → silver → gold)
               →  gold                            (é o que dashboard e relatório leem)
```

Regra que atravessa tudo: **o dado bruto não é corrigido na ingestão.** Ele entra
como veio, e a limpeza acontece no dbt, onde fica versionada, testada e visível
no diff. Consertar na ingestão esconde o problema — no dia em que o número estiver
errado, ninguém acha onde foi mexido.

## Fase 0 — Ler a issue

Se o usuário deu um número:

```bash
gh issue view <numero> --comments
```

As issues aqui nascem de formulário, então os campos que você precisa já estão
preenchidos: fonte, tipo, destino, camada, estratégia de carga, granularidade.
Extraia deles o escopo antes de escrever qualquer linha. Se algum campo
obrigatório estiver vazio — issue antiga, criada antes dos formulários —
pergunte **só o que falta**.

Se não houver issue, pergunte se deve criar uma: veja a skill
[`to-issues-minc`](../to-issues-minc/SKILL.md).

## Fase 1 — Cliente da API

Um arquivo por sistema de origem, em `plugins/cliente_<sistema>.py`. Herde de
`ClienteBase` (`plugins/cliente_base.py`), que já resolve retry, timeout e
tratamento de status:

```python
from cliente_base import ClienteBase

class ClienteNovoSistema(ClienteBase):
    BASE_URL = "https://api.exemplo.gov.br"

    def __init__(self) -> None:
        super().__init__(base_url=self.BASE_URL)

    def get_programas(self) -> list[dict] | None:
        status, data = self.request("GET", "/programas")
        ...
```

O `request` devolve `(HTTPStatus, dict | list | None)` — sempre confira o status
antes de usar o dado. Autenticação com certificado ou SCA fica em módulo próprio;
veja `plugins/csa_auth.py` como referência.

**O cliente não conhece o banco.** Ele fala HTTP e devolve estrutura Python. Quem
grava é a DAG.

## Fase 2 — DAG de ingestão

Uma pasta por fonte em `dags/data_ingest/`, um arquivo por endpoint. Airflow 3,
com a API `airflow.sdk`:

```python
from airflow.sdk import dag, task
from cliente_postgres import ClientPostgresDB
from postgres_helpers import get_postgres_conn
from schedule_loader import get_dynamic_schedule
import schemas_minc as schemas

default_args = {"owner": "<seu nome>", "retries": 3, "retry_delay": timedelta(minutes=5)}

@dag(
    schedule=get_dynamic_schedule("nome_da_dag"),
    default_args=default_args,
    catchup=False,
    tags=["minc", "<sistema>", "raw"],
)
def nome_da_dag() -> None:
    ...
```

O que não se improvisa aqui:

- **Schedule sai do `schedule_loader`**, não fica escrito na DAG. Ele lê a Variable
  `dynamic_schedules` do Airflow, e é isso que permite mudar periodicidade sem PR.
- **Nome de schema e tabela sai de `plugins/schemas_minc.py`.** String solta na DAG
  vira divergência silenciosa no dia em que o schema for renomeado.
- **A carga tem que poder repetir.** Reexecutar a DAG não pode duplicar registro —
  use chave e upsert, não append cego. É a falha mais cara de descobrir tarde,
  porque o número só fica errado depois da segunda execução.
- **Log com prefixo do arquivo**, no padrão `[nome_do_arquivo.py] mensagem`. É como
  se acha a origem no log do Airflow, que mistura todas as DAGs.

## Fase 3 — Modelos dbt

Projeto em `dbt/minc/`, perfil `minc`. Dois domínios, cada um com schema próprio:

| Domínio | Schema | Assunto |
|---|---|---|
| `cotas_dbt` | `minc_cotas` | Meta 3 — diversidade, cotas e territórios |
| `agentes_dbt` | `minc_agentes` | Meta 5 — perfil e primeiro acesso |
| `metadata` | `metadata` | metadados do próprio pipeline |

Camadas, em ordem: `bronze/` (o bruto tipado e limpo do lixo de parsing),
`silver/` (regra de negócio aplicada), `gold/` (o que vai a consumo).
`agentes_dbt` tem também `views/`.

Materialização já vem definida por camada no `dbt_project.yml` — **não repita
`+materialized` no modelo** a menos que ele seja exceção, e nesse caso comente por
quê. O `cotas_dbt/bronze` é materializado como `table`, e não `incremental`, porque
filtra lixo de parsing e cai de milhões para ~20 mil linhas; está escrito lá.

Macros reaproveitáveis em `dbt/minc/macros/` antes de escrever SQL novo:
`sem_acento`, `normaliza_documento`, `parse_valor`, `parse_financial_value`,
`ano_edital`, `coalesce_por_nome`. Normalizar documento à mão quando já existe
macro é como o mesmo CPF vira duas pessoas em dois modelos diferentes.

Cada camada tem seu `schema.yml`, ao lado dos modelos. Fonte nova entra em
`dbt/minc/models/sources.yml`.

```bash
cd dbt/minc
dbt run  --select <modelo>
dbt test --select <modelo>
dbt run  --select +<modelo>   # com as dependências acima dele
```

## Fase 4 — Testes

Teste de dbt no `schema.yml` da camada: `unique` e `not_null` na chave, no mínimo.
Modelo gold sem teste de unicidade na chave é como duplicata chega ao dashboard.

Teste de Python em `tests/`, rodado por `make test`.

Quando o modelo produz número que alguém vai citar, confira a contagem contra a
origem e registre o resultado na issue ou no PR. Número que ninguém conferiu não
é entrega, é rascunho.

## Fase 5 — Abrir o PR

Use a skill [`abrir-pr`](../abrir-pr/SKILL.md). Ela preenche o impacto em dados,
que é a seção que o revisor não consegue tirar do diff.

---

## Onde mexer, por sintoma

| O que você quer | Onde |
|---|---|
| Número errado numa tabela gold | Suba a linhagem: gold → silver → bronze → tabela raw. O erro quase nunca está no gold |
| Mudar periodicidade de uma DAG | Variable `dynamic_schedules` do Airflow — não na DAG |
| Nome de schema ou tabela | `plugins/schemas_minc.py` |
| Adicionar fonte nova ao dbt | `dbt/minc/models/sources.yml` |
| Descrição de modelo que aparece no `dbt docs` | `schema.yml` da camada |
| Regra de normalização de CPF/CNPJ | `dbt/minc/macros/normaliza_documento.sql` |
| Ano do edital a partir do nome do arquivo | `dbt/minc/macros/ano_edital.sql` |
| Materialização de uma camada inteira | `dbt/minc/dbt_project.yml` |
| Ordem em que os modelos rodam | Não se define à mão — sai do `ref()` entre modelos |
| Autenticação com certificado | `plugins/csa_auth.py` |
| Retry e timeout de chamada HTTP | `plugins/cliente_base.py` — vale para todos os clientes |
| Subir o ambiente local | `make up` |

## O que é gerado e o que é seu

| Caminho | Natureza | Você edita? |
|---|---|---|
| `dbt/minc/models/**/*.sql` | os modelos | **Sim** |
| `dbt/minc/models/**/schema.yml` | testes e descrições | **Sim** |
| `dbt/minc/models/sources.yml` | declaração das origens | **Sim** |
| `dbt/minc/target/` | saída do `dbt run` e do `dbt docs` | **Não.** Regenerada, e ignorada pelo git |
| `dbt/minc/logs/`, `dbt_packages/` | execução e dependências | **Não** |
| `data/**` | extrações brutas de quem roda as DAGs | **Não.** Ignorada pelo git; só a estrutura de pastas é versionada |
| `requirements.generated.txt` | exportado do Poetry por `make setup` | **Não.** Edite o `pyproject.toml` |

## Quando falha

| Mensagem | Causa mais comum |
|---|---|
| `Compilation Error ... depends on a node named X which was not found` | `ref()` para modelo que não existe, ou erro de digitação no nome |
| `Database Error ... relation "..." does not exist` | A fonte não foi ingerida ainda, ou está em outro schema. Confira `sources.yml` |
| `dbt test` falha em `unique` | A granularidade do modelo não é a que você acha que é. Conte antes de mudar o teste |
| DAG não aparece no Airflow | Erro de import. `make logs-airflow` mostra o traceback |
| `ModuleNotFoundError` num import de `plugins/` | O `PYTHONPATH` sai do Makefile — rode pelos alvos `make`, não com `python` direto |
| Segunda execução da DAG duplicou linha | Carga sem chave/upsert. Conserte a carga, não apague no dbt |
| `make format` falha no commit, ou `make lint` no push | Erros de lint pré-existentes no Python, não seus. Veja a armadilha em [`commit-smart`](../commit-smart/SKILL.md) |

## Regras

1. **Não conserte dado bruto na ingestão.** A limpeza é no dbt, onde fica no diff.
   Correção na DAG some do histórico e ninguém acha depois.
2. **Nunca escreva schema ou tabela como string solta.** Use `schemas_minc.py`. O
   dia em que o schema for renomeado, a string solta continua apontando para o
   lugar antigo e falha em silêncio.
3. **Toda carga precisa poder repetir.** Se reexecutar duplica, está errado —
   mesmo que o número de hoje esteja certo.
4. **Nenhuma credencial em arquivo versionado**, nem de exemplo, nem em comentário,
   nem em teste. O `local.env` é template sanitizado e não deve ser reescrito com
   valores reais.
5. **Cuidado com o dado pessoal.** O repositório lida com CPF, CNPJ e dados de raça
   e deficiência de agentes culturais. Isso não entra em log, em print de PR, nem
   em issue.
6. **Número que ninguém conferiu não é entrega.** Ao produzir gold, confira a
   contagem contra a origem e registre onde alguém possa achar.

## Antes de considerar pronto

- [ ] `dbt run` e `dbt test` passam no modelo alterado e nos que dependem dele
- [ ] A DAG importa sem erro e aparece no Airflow
- [ ] Reexecutar a carga não duplica registro
- [ ] A chave do modelo gold tem teste de `unique` e `not_null`
- [ ] As colunas novas estão descritas no `schema.yml`
- [ ] A contagem foi conferida contra a origem, se o modelo produz número em uso
- [ ] Nenhuma credencial ou dado pessoal no código, no log ou no PR
