# OpenMetadata DAG

Esta pasta concentra a DAG de ingestão do OpenMetadata e o código de suporte para renderizar recipes, preparar artefatos do dbt e executar cada workflow.

## Estrutura

- `openmetadata_ingestion_dag.py`: define a DAG e o encadeamento entre tasks.
- `config.py`: catálogo de recipes, replacements e requirements do virtualenv.
- `execution.py`: renderização e execução dos workflows.
- `recipes/`: recipes YAML usadas pelo OpenMetadata.
- `airflow_log_config.py`: logging custom usado apenas para a recipe de Airflow.

## Caso especial: `airflow_metadata`

A recipe `airflow_metadata` roda em `@task.virtualenv`, mas o source `airflow` do OpenMetadata importa o pacote `airflow` de verdade durante a execução. No nosso ambiente, isso exigiu alguns cuidados:

1. A recipe foi configurada para ler o metadata DB do Airflow via `Postgres`, e não via `Backend`.
2. A execução dessa recipe acontece em-process com `MetadataWorkflow.create(...)`, em vez de `metadata ingest -c ...`.
3. O virtualenv precisa incluir `asyncpg`, porque o Airflow inicializa uma sessão assíncrona do SQLAlchemy ao subir o ORM.
4. O Airflow também precisa de uma config de logging simplificada para conseguir inicializar dentro do virtualenv isolado.

Sem isso, os erros observados foram genéricos, como "missing plugin [airflow]", mas a causa real era falha ao inicializar o próprio pacote `airflow` dentro do venv.

## Dependências importantes do virtualenv

Em `config.py`, manter para a task de OpenMetadata:

- `openmetadata-ingestion[dbt,postgres,superset,airflow,pii-processor]==1.12.1`
- `asyncpg`

Se `asyncpg` sair da lista, a ingestão de Airflow volta a falhar ao importar o source `airflow`.

## Logs esperados

Quando `airflow_metadata` estiver saudável, o log tende a mostrar:

- `Executing workflow em-process via MetadataWorkflow.create(...)`
- `Running CheckAccess...`
- `Running PipelineDetailsAccess...`
- `Running TaskDetailAccess...`
- `Workflow Success %: 100.0`

## Plugins locais do Airflow

O Airflow pode registrar erros ao importar plugins de `/opt/airflow/plugins`, por exemplo por falta de dependências como `imap_tools` ou `zeep`. No cenário atual isso não bloqueou a ingestão do OpenMetadata, então esses erros devem ser tratados separadamente dos problemas da recipe `airflow_metadata`.

## Shim de lineage para APIs

Algumas DAGs precisam anotar lineage de uma API externa para uma tabela, por exemplo:

```text
compras_gov_api -> api_contratos_dag -> IPEA.analytics.compras_gov.contratos
```

O formato completo de lineage do OpenMetadata usa imports como:

```python
from metadata.generated.schema.entity.services.apiService import ApiService
from metadata.ingestion.source.pipeline.airflow.lineage_parser import OMEntity
```

Esses imports normalmente vêm do pacote `openmetadata-ingestion`, mas esse pacote não deve ser instalado no runtime principal do Airflow porque pode conflitar com as dependências do scheduler/webserver. Para evitar isso, criamos um shim mínimo em `airflow_lappis/helpers/metadata`.

O `docker-compose.yml` já inclui `/opt/airflow/helpers` no `PYTHONPATH`, então esses módulos ficam disponíveis para o parse das DAGs sem instalar a lib completa:

```text
airflow_lappis/helpers/metadata/generated/schema/entity/services/apiService.py
airflow_lappis/helpers/metadata/generated/schema/entity/data/apiEndpoint.py
airflow_lappis/helpers/metadata/ingestion/source/pipeline/airflow/lineage_parser.py
```

Esse shim não conversa com o OpenMetadata. Ele só cria classes mínimas para que a DAG serialize as anotações no formato esperado pela ingestão de Airflow. Exemplo:

```python
inlets=[
    OMEntity(entity=ApiService, fqn="compras_gov_api", key="apiService")
]
outlets=[
    {
        "entity": "table",
        "fqn": "IPEA.analytics.compras_gov.contratos",
        "key": "apiService",
    }
]
```

A `key` precisa ser a mesma no inlet e no outlet para agrupar as duas pontas na mesma relação de lineage. Se a key divergir, o OpenMetadata pode criar arestas incompletas ou self-loops.

Antes de usar uma entidade não-table, confirme o FQN e o tipo real dela no OpenMetadata. No caso testado, `compras_gov_api` era um `ApiService`, não um `APIEndpoint`.

## Quando atualizar

Atualize esta pasta quando houver:

- nova recipe de metadata/profiler/classifier;
- mudança no banco do Airflow ou nas variáveis de conexão;
- mudança de versão do OpenMetadata;
- novos schemas/tabelas relevantes para as recipes de Postgres metadata e profiler.
