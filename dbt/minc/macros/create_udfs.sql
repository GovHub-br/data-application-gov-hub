{% macro create_udfs() %}
{#
  Cria/atualiza as UDFs do projeto via run_query, isolada do on-run-start.

  Antes, esta macro era chamada em todo on-run-start do dbt — como o Cosmos
  dispara um processo dbt por task/modelo em paralelo, várias conexões
  executavam CREATE OR REPLACE FUNCTION ao mesmo tempo, causando
  "tuple concurrently updated" no catálogo pg_proc do Postgres.

  Agora ela só deve ser chamada explicitamente, uma única vez, via:
    dbt run-operation create_udfs
  em uma task isolada do Airflow, executada antes das tasks de modelo
  geradas pelo Cosmos — eliminando a concorrência na origem.
#}

{% set create_schema_sql %}
  CREATE SCHEMA IF NOT EXISTS {{ target.schema }};
{% endset %}
{% do run_query(create_schema_sql) %}

{% set parse_date_sql %}
  {{ create_f_parse_dates() }}
{% endset %}
{% do run_query(parse_date_sql) %}

{% set format_nc_sql %}
  {{ create_f_format_nc() }}
{% endset %}
{% do run_query(format_nc_sql) %}

{% do log("UDFs criadas/atualizadas no schema " ~ target.schema, info=True) %}

{% endmacro %}
