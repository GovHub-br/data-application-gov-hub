{% macro create_udfs() %}

CREATE SCHEMA IF NOT EXISTS {{ target.schema }};

DO $do$
BEGIN
  -- Serializa a criação das UDFs entre threads paralelas do dbt/Cosmos.
  -- Sem isso, múltiplas conexões tentam CREATE OR REPLACE FUNCTION ao mesmo
  -- tempo, causando "tuple concurrently updated" no catálogo pg_proc.
  PERFORM pg_advisory_lock(hashtext('{{ target.schema }}_create_udfs'));

  {{ create_f_parse_dates() }}
  {{ create_f_format_nc() }}

  PERFORM pg_advisory_unlock(hashtext('{{ target.schema }}_create_udfs'));
EXCEPTION WHEN OTHERS THEN
  PERFORM pg_advisory_unlock(hashtext('{{ target.schema }}_create_udfs'));
  RAISE;
END;
$do$;

{% endmacro %}
