---
name: govhub-pipeline-guide
description: >
  Guia completo para implementar uma nova fonte de dados no Gov Hub BR — desde a criação do
  DAG Airflow e cliente de API até as transformações dbt (bronze → silver → gold), testes e PR.
  Use este skill sempre que o usuário mencionar: novo DAG, nova fonte de dados, ingestão de
  dados, pipeline para qualquer sistema governamental (SIAPE, SIAFI, SICONV, TransfereGov,
  IBGE, PNCP ou qualquer outro), ou quando quiser construir/corrigir modelos dbt neste
  repositório. Ative também quando o usuário abrir uma issue, mencionar uma task ou ticket
  relacionado a dados governamentais, ou pedir para "resolver uma dag", "criar uma pipeline"
  ou "adicionar uma fonte".
---

# Gov Hub Pipeline Guide

Este skill guia a implementação completa de uma nova fonte de dados no repositório
`data-application-gov-hub`, do DAG de ingestão até os modelos dbt prontos para análise.

## Fase 0 — Ler a Issue

O ponto de entrada é sempre uma issue do GitHub. Se o usuário não forneceu o número,
pergunte: **"Qual é o número da issue?"**

Com o número em mãos, busque o conteúdo completo:

```bash
gh issue view {numero} --repo GovHub-br/data-application-gov-hub
```

Leia atentamente o título, corpo, labels e comentários da issue para extrair:

- **Sistema/fonte:** qual API ou dataset precisa ser ingerido
- **Tabelas/endpoints:** quais entidades específicas
- **Tipo de API:** REST/JSON, SOAP/XML, arquivo, etc.
- **Frequência:** se mencionada (diária, mensal, manual)
- **Escopo dbt:** quais camadas são necessárias (bronze, silver, gold)

Após ler a issue, **sempre pergunte explicitamente ao usuário:**

> "Para qual projeto dbt os modelos devem ir? As opções disponíveis são:"
>
> 1. `mir` — transferências, emendas, convênios, TransfereGov
> 2. `ipea` — contratos, servidores, orçamento, BACEN, indicadores econômicos
> 3. Outro — informe o nome do diretório

**Não infira o projeto dbt automaticamente.** Mesmo que pareça óbvio pela issue, confirme — a escolha errada gera retrabalho nas referências de `source()` e na estrutura de pastas.

Se alguma outra informação estiver ausente ou ambígua na issue, pergunte ao usuário
**apenas o que falta** — não repita o que já está claro.

Somente após a confirmação do projeto dbt, gere o **checklist personalizado** e comece a executar.

---

## Princípios de TDD neste projeto

**Todo código novo deve seguir Red → Green → Refactor:**

1. **Red** — escreva o teste que falha *antes* de qualquer implementação
2. **Green** — implemente o mínimo necessário para o teste passar
3. **Refactor** — limpe sem quebrar os testes

Regras não-negociáveis:
- Não crie `cliente_{sistema}.py` sem antes ter `tests/plugins/test_cliente_{sistema}.py` com pelo menos um teste falhando
- Não crie um modelo dbt sem antes ter o `schema.yml` com os testes (`not_null`, `unique`, genéricos) declarados
- `make test` deve passar ao final de cada passo — nunca só no Passo 7

---

## Checklist Padrão

Adapte para a fonte específica e use para acompanhar o progresso. **A ordem importa — testes vêm antes do código.**

```
[ ] Branch criada: feat/{sistema}-{entidade}
[ ] Issue no GitHub referenciada

# Passo 1 — Cliente (TDD)
[ ] Teste do cliente: tests/plugins/test_cliente_{sistema}.py (Red)
[ ] Cliente implementado: airflow_lappis/plugins/cliente_{sistema}.py (Green)
[ ] make test passando

# Passo 2 — DAG (TDD)
[ ] Teste do DAG: tests/dags/test_{sistema}_{entidade}_ingest_dag.py (Red)
[ ] DAG implementado: airflow_lappis/dags/data_ingest/{sistema}/{entidade}_ingest_dag.py (Green)
[ ] make test passando

# Passos 3-6 — dbt (schema-first TDD)
[ ] Fonte declarada em models/sources.yml (projeto dbt correto)
[ ] schema.yml bronze escrito com testes declarados (Red)
[ ] Modelo bronze: models/{sistema}_dbt/bronze/{entidade}.sql (Green)
[ ] dbt test --select {entidade} passando
[ ] schema.yml silver escrito com testes declarados (se necessário)
[ ] Modelo silver: models/{sistema}_dbt/silver/{entidade}.sql (se necessário)
[ ] schema.yml gold escrito com testes declarados (se necessário)
[ ] Modelo gold: models/{sistema}_dbt/gold/{entidade}.sql (se necessário)
[ ] dbt test --select {entidade}+ passando

[ ] PR criado seguindo o template
```

---

## Passo 1 — Cliente de API (`plugins/cliente_{sistema}.py`)

### 1a — Escreva o teste primeiro (Red)

**Caminho:** `tests/plugins/test_cliente_{sistema}.py`

Crie o arquivo de teste antes de qualquer implementação. Ele **deve falhar** agora (o módulo ainda não existe — isso é esperado).

```python
import pytest
from unittest.mock import patch, MagicMock


class TestCliente{Sistema}:
    def test_get_{entidade}_retorna_lista(self):
        """Retorna lista de dicts quando a API responde com sucesso."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"data": [{"id": 1, "descricao": "Teste"}]}
        mock_response.raise_for_status.return_value = None

        with patch("requests.get", return_value=mock_response):
            from cliente_{sistema} import Cliente{Sistema}
            cliente = Cliente{Sistema}()
            resultado = cliente.get_{entidade}()

        assert isinstance(resultado, list)
        assert len(resultado) == 1
        assert resultado[0]["id"] == 1

    def test_get_{entidade}_retorna_lista_vazia_quando_sem_dados(self):
        """Retorna lista vazia quando a API não devolve registros."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"data": []}
        mock_response.raise_for_status.return_value = None

        with patch("requests.get", return_value=mock_response):
            from cliente_{sistema} import Cliente{Sistema}
            cliente = Cliente{Sistema}()
            resultado = cliente.get_{entidade}()

        assert resultado == []

    def test_get_{entidade}_levanta_excecao_em_erro_http(self):
        """Propaga HTTPError quando a API retorna status de erro."""
        import requests

        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.HTTPError("500")

        with patch("requests.get", return_value=mock_response):
            from cliente_{sistema} import Cliente{Sistema}
            cliente = Cliente{Sistema}()
            with pytest.raises(requests.HTTPError):
                cliente.get_{entidade}()
```

Confirme que os testes falham:
```bash
make test
# ou: pytest tests/plugins/test_cliente_{sistema}.py -v
```

### 1b — Implemente o cliente (Green)

Somente após os testes existirem e falharem, implemente o cliente.

Antes de criar, verifique se já existe algo em `airflow_lappis/plugins/` que possa ser
reaproveitado (ex: `cliente_base.py` para HTTP com retry).

O cliente deve:
- Encapsular toda a comunicação com a fonte (autenticação, paginação, retry)
- Retornar dados como `list[dict]` para o DAG consumir diretamente
- Ler credenciais de variáveis de ambiente (`os.getenv(...)`)

**Estrutura para API REST:**
```python
import os
import logging
import requests
from typing import Any
from retry_helpers import retry_on_exception

logger = logging.getLogger(__name__)

class Cliente{Sistema}:
    def __init__(self) -> None:
        self.base_url = os.getenv("{SISTEMA}_BASE_URL", "https://api.exemplo.gov.br")
        self.token = os.getenv("{SISTEMA}_TOKEN")

    @retry_on_exception(max_retries=3, delay=5)
    def get_{entidade}(self, **params: Any) -> list[dict]:
        headers = {"Authorization": f"Bearer {self.token}"}
        response = requests.get(
            f"{self.base_url}/{endpoint}",
            headers=headers,
            params=params,
            timeout=30,
        )
        response.raise_for_status()
        return response.json().get("data", [])
```

Para SOAP (como SIAFI), use `zeep` — veja `cliente_siafi.py` como referência.

### 1c — Confirme Green

```bash
make test
# Todos os testes do cliente devem passar antes de ir para o Passo 2
```

---

## Passo 2 — DAG de Ingestão

### 2a — Escreva o teste primeiro (Red)

**Caminho:** `tests/dags/test_{sistema}_{entidade}_ingest_dag.py`

```python
import pytest
from unittest.mock import patch, MagicMock


class TestIngestDAG:
    def test_dag_carrega_sem_erros(self):
        """DAG deve ser importável e parseável pelo Airflow sem erros."""
        from airflow_lappis.dags.data_ingest.{sistema}.{entidade}_ingest_dag import dag_instance
        assert dag_instance is not None

    def test_dag_tem_task_de_ingestao(self):
        """DAG deve conter a task de fetch_and_store."""
        from airflow_lappis.dags.data_ingest.{sistema}.{entidade}_ingest_dag import dag_instance
        task_ids = [t.task_id for t in dag_instance.tasks]
        assert any("{entidade}" in tid for tid in task_ids)

    def test_fetch_insere_dados_no_postgres(self):
        """Task deve chamar insert_data com os dados retornados pelo cliente."""
        mock_cliente = MagicMock()
        mock_cliente.get_{entidade}.return_value = [{"id": 1}]
        mock_db = MagicMock()

        with patch("cliente_{sistema}.Cliente{Sistema}", return_value=mock_cliente), \
             patch("cliente_postgres.ClientPostgresDB", return_value=mock_db):
            from airflow_lappis.dags.data_ingest.{sistema}.{entidade}_ingest_dag import fetch_and_store_{entidade}
            fetch_and_store_{entidade}()

        mock_db.insert_data.assert_called_once()
        args = mock_db.insert_data.call_args
        assert args[1]["schema"] == "{sistema}"
```

Confirme falha:
```bash
make test
```

### 2b — Implemente o DAG (Green)

**Caminho:** `airflow_lappis/dags/data_ingest/{sistema}/{entidade}_ingest_dag.py`

Use sempre o padrão TaskFlow API (`@dag` + `@task`). Padrão do projeto:

```python
import logging
from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime, timedelta
from typing import Any
from schedule_loader import get_dynamic_schedule
from cliente_{sistema} import Cliente{Sistema}
from cliente_postgres import ClientPostgresDB
from postgres_helpers import get_postgres_conn


@dag(
    schedule_interval=get_dynamic_schedule("{sistema}_{entidade}_ingest_dag"),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "owner": "{seu_nome}",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["{sistema}", "{entidade}"],
)
def {sistema}_{entidade}_ingest_dag() -> None:
    @task
    def fetch_and_store_{entidade}(**context: Any) -> None:
        orgao_alvo = Variable.get("airflow_orgao", default_var=None)
        cliente = Cliente{Sistema}()
        db = ClientPostgresDB(get_postgres_conn())

        dados = cliente.get_{entidade}(orgao=orgao_alvo)
        for item in dados:
            item["dt_ingest"] = datetime.now().isoformat()

        db.insert_data(
            dados,
            table_name="{entidade}",
            conflict_fields=["{chave_primaria}"],
            primary_key=["{chave_primaria}"],
            schema="{sistema}",
        )
        logging.info(f"{len(dados)} registros inseridos em {sistema}.{entidade}")

    fetch_and_store_{entidade}()


dag_instance = {sistema}_{entidade}_ingest_dag()
```

**Pontos importantes:**
- `schema` no `insert_data` deve ser o nome do sistema em minúsculas (ex: `siafi`, `siconv`) — será o mesmo schema referenciado no `sources.yml`
- `conflict_fields` define o comportamento de upsert — identifique a chave natural dos dados
- O nome do DAG passado para `get_dynamic_schedule` deve ser único e descritivo
- Se a ingestão precisar de parâmetros de backfill (ex: ano), use `Param` — veja `nota_empenho_siafi_ingest_dag.py` como referência
- Se filtrar por UG/órgão, use `Variable.get("airflow_variables")` + yaml.safe_load — veja o padrão SIAFI

---

## Passo 3 — Declarar a Fonte no dbt (`models/sources.yml`)

No projeto dbt correto (`mir` ou `ipea`), adicione ao arquivo `models/sources.yml`:

```yaml
- name: {sistema}          # deve bater com o schema do Postgres usado no DAG
  schema: {sistema}
  tables:
    - name: {entidade}
    # adicione outras tabelas do mesmo sistema aqui se necessário
```

Se o sistema já está declarado, apenas adicione a nova tabela sob ele.

---

## Passo 4 — Modelo Bronze

### 4a — Escreva o `schema.yml` primeiro (Red)

**Caminho:** `models/{sistema}_dbt/bronze/schema.yml`

Declare os testes **antes** de criar o `.sql`. A ideia: `dbt test` falhará com "relation does not exist" — isso é o Red esperado.

```yaml
version: 2

models:
  - name: {entidade}
    description: >
      Bronze layer de {entidade} — dados brutos do {sistema} com tipagem correta.
    meta:
      tags:
        - bronze
    columns:
      - name: id_{entidade}
        description: Identificador único do registro.
        tests:
          - unique
          - not_null
      - name: codigo
        description: Código do registro.
        tests:
          - not_null
      - name: valor
        description: Valor monetário em R$.
        tests:
          - not_null
```

Confirme falha (Red):
```bash
dbt test --select {entidade}
# Esperado: "Relation ... does not exist" — isso é correto, ainda não há model
```

### 4b — Implemente o modelo (Green)

**Caminho:** `models/{sistema}_dbt/bronze/{entidade}.sql`

Bronze = dados brutos com tipagem correta. Regras:
- `{{ source('{sistema}', '{entidade}') }}` — nunca `ref()` no bronze
- Selecione todas as colunas relevantes da fonte
- Apenas conversões de tipo: `::integer`, `::date`, `::numeric(15, 2)`, `::text`
- Para datas em texto: `to_date(nullif(campo, ''), 'DD/MM/YYYY')`
- Para valores monetários com vírgula: `replace(nullif(campo, ''), ',', '.')::numeric(15, 2)`
- Nenhum filtro, nenhuma regra de negócio

```sql
{{ config(materialized="table") }}

with
    {entidade}_raw as (
        select
            id_{entidade}::integer as id_{entidade},
            codigo::text as codigo,
            descricao::text as descricao,
            to_date(nullif(data_inicio, ''), 'DD/MM/YYYY') as data_inicio,
            replace(nullif(valor, ''), ',', '.')::numeric(15, 2) as valor
        from {{ source('{sistema}', '{entidade}') }}
    )

select *
from {entidade}_raw
```

### 4c — Confirme Green

```bash
dbt run --select {entidade}
dbt test --select {entidade}
# Todos os testes do schema.yml devem passar
```

---

## Passo 5 — Modelo Silver (quando aplicável)

**Caminho:** `models/{sistema}_dbt/silver/{entidade}.sql`

Silver = dados confiáveis com regras de negócio aplicadas. Use quando precisar de:
- Joins entre tabelas bronze
- Filtros de qualidade (remover registros inválidos)
- Deduplicação
- Padronização de valores categóricos

```sql
{{ config(materialized="table") }}

with
    bronze_{entidade_a} as (select * from {{ ref('{entidade_a}') }}),
    bronze_{entidade_b} as (select * from {{ ref('{entidade_b}') }})

select
    a.id_{entidade_a},
    a.campo_chave,
    b.descricao,
    a.valor
from bronze_{entidade_a} a
inner join bronze_{entidade_b} b on a.id_fk = b.id_{entidade_b}
where a.situacao = 'ATIVO'
```

---

## Passo 6 — Modelo Gold (quando aplicável)

**Caminho:** `models/{sistema}_dbt/gold/{entidade}.sql`

Gold = dados agregados, prontos para consumo em dashboards (Superset). Use `{{ ref() }}`
apontando para modelos silver. Nomes de colunas descritivos e em português.

---

## Passo 7 — Verificação Final (não é aqui que os testes começam)

Se o TDD foi seguido corretamente, todos os testes já passam desde cada passo.
Este passo é apenas a confirmação final antes do PR.

```bash
# Suite Python completa
make test

# Suite dbt completa para tudo que foi criado
dbt run --select {entidade}+
dbt test --select {entidade}+
```

Se qualquer teste falhar aqui e não falhou antes, houve regressão — investigue antes de abrir o PR.

Use o macro `verificacao_tipagem` para garantir que os tipos estão corretos após a carga — veja exemplos no projeto para a sintaxe.

---

## Passo 8 — PR

Ao final do trabalho, exiba a descrição completa do PR para o usuário copiar e colar no
GitHub. Preencha cada campo com base no que foi efetivamente implementado — não deixe
comentários vagos, não diga "inclui X" sem escrever X.

A saída deve ser um bloco de markdown que começa com o título e termina no checklist,
exatamente assim (substitua tudo entre `{}`):

---
**Título do PR:**
`feat({sistema}): {resumo em uma linha do que foi feito}`

**Corpo:**

```markdown
## Descrição
{2-4 frases: o que foi implementado, de qual sistema, quais camadas dbt, e por que
essa mudança é necessária. Ex: "Implementa 15 modelos silver para cruzamentos do SICONV
no projeto MIR. Cada modelo une a tabela de convênio/proposta com uma entidade de detalhe
(empenho, licitação, pagamento, etc.), aplicando deduplicação e tipagem correta. Resolve
a demanda de análise de execução financeira por convênio."}

## Tipo de mudança
- {[x] ou [ ]} Nova funcionalidade / pipeline
- {[x] ou [ ]} Correção de bug ou inconsistência de dados
- {[x] ou [ ]} Refatoração de modelo DBT
- {[x] ou [ ]} Documentação
- {[x] ou [ ]} Infraestrutura / CI

## Issues relacionadas
Closes #{numero}

## Como testar / validar

```bash
{comandos exatos para rodar — dbt run, dbt test, make test conforme o que foi feito}
```

## Evidências
{instrução clara do que o usuário deve rodar e colar aqui antes de submeter.
Ex: "Cole aqui o output de `dbt run --select siconv_dbt.silver.*` e a contagem de
linhas de ao menos 2 modelos: `select count(*) from analytics.siconv_dbt.convenio_empenho`"}

## Checklist
- {[x] se testes foram adicionados} Testes DBT adicionados/atualizados
- {[x] se schema.yml foi atualizado} Documentação atualizada
- [x] Sem dados sensíveis ou credenciais no código
- [ ] Branch atualizada com `upstream/main`
```
---

- Commits atômicos: um commit por etapa (cliente, DAG, bronze, silver, gold)

---

## Referências Rápidas

| O que | Onde |
|---|---|
| Clientes de API existentes | `airflow_lappis/plugins/` |
| DAGs de ingestão de exemplo | `airflow_lappis/dags/data_ingest/siafi/` |
| Bronze com datas/valores | `models/siconv_dbt/bronze/proposta.sql` |
| Silver com joins | `models/siconv_dbt/silver/proposta_convenio.sql` |
| schema.yml completo | `models/emendas_dbt/bronze/schema.yml` |
| Backfill com Param | `dags/data_ingest/siafi/nota_empenho_siafi_ingest_dag.py` |
| CONTRIBUTING | `.github/CONTRIBUTING.md` |
