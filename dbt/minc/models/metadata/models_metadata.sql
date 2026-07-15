{{
    config(
        materialized='incremental',
        unique_key=['schema_name', 'table_name'],
        on_schema_change='sync_all_columns'
    )
}}

{#
    Tabela de Metadados dos Modelos dbt
    ===================================
    
    Esta tabela armazena metadados de todos os modelos executados no dbt.
    
    Campos principais:
    - schema_name: Schema do modelo
    - table_name: Nome da tabela/modelo
    - dt_transform: Data da última transformação (quando o modelo foi executado)
    - run_id: ID único da execução do dbt
    
    A tabela é atualizada de forma incremental, mantendo apenas o registro
    mais recente para cada combinação de schema + table_name.
#}

WITH dbt_models AS (
    {# 
        Usando a função graph do dbt para iterar sobre todos os modelos do projeto.
        Isso garante que capturamos metadados de todos os modelos definidos.
    #}
    {% set models_data = [] %}
    
    {% for node in graph.nodes.values() %}
        {% if node.resource_type == 'model' %}
            {% do models_data.append({
                'schema_name': node.schema,
                'table_name': node.name,
                'database_name': node.database,
                'materialization': node.config.materialized,
                'description': node.description | default('') | replace("'", "''")
            }) %}
        {% endif %}
    {% endfor %}

    {% for model in models_data %}
        SELECT
            '{{ model.schema_name }}' AS schema_name,
            '{{ model.table_name }}' AS table_name,
            '{{ model.database_name }}' AS database_name,
            '{{ model.materialization }}' AS materialization,
            '{{ model.description[:500] }}' AS description,
            ('{{ run_started_at }}'::TIMESTAMP AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS dt_transform,
            '{{ invocation_id }}' AS run_id
        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
)

SELECT
    schema_name,
    table_name,
    database_name,
    materialization,
    description,
    dt_transform,
    run_id
FROM dbt_models
