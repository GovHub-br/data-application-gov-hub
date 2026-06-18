{{
    config(
        materialized="incremental",
        unique_key=["schema_name", "table_name"],
        on_schema_change="sync_all_columns",
    )
}}

with dbt_models as (
    {% set models_data = [] %}

    {% for node in graph.nodes.values() %}
        {% if node.resource_type == "model" %}
            {% do models_data.append({
                "schema_name": node.schema,
                "table_name": node.name,
                "database_name": node.database,
                "materialization": node.config.materialized,
                "description": node.description | default("") | replace("'", "''"),
            }) %}
        {% endif %}
    {% endfor %}

    {% for model in models_data %}
        select
            '{{ model.schema_name }}' as schema_name,
            '{{ model.table_name }}' as table_name,
            '{{ model.database_name }}' as database_name,
            '{{ model.materialization }}' as materialization,
            '{{ model.description[:500] }}' as description,
            (
                '{{ run_started_at }}'::timestamp
                at time zone 'UTC'
                at time zone 'America/Sao_Paulo'
            ) as dt_transform,
            '{{ invocation_id }}' as run_id
        {% if not loop.last %}
        union all
        {% endif %}
    {% endfor %}
)

select
    schema_name,
    table_name,
    database_name,
    materialization,
    description,
    dt_transform,
    run_id
from dbt_models
