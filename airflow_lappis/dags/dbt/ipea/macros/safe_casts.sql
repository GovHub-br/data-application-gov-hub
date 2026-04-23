{% macro safe_bigint(column_name) -%}
    case
        when {{ column_name }} is null then null
        when nullif(trim({{ column_name }}::text), '') is null then null
        when upper(trim({{ column_name }}::text)) = 'NAN' then null
        when trim({{ column_name }}::text) ~ '^[+-]?[0-9]+$' then trim({{ column_name }}::text)::bigint
        else null
    end
{%- endmacro %}

{% macro safe_numeric(column_name, precision=18, scale=2) -%}
    case
        when {{ column_name }} is null then null
        when nullif(trim({{ column_name }}::text), '') is null then null
        when upper(trim({{ column_name }}::text)) = 'NAN' then null
        when trim({{ column_name }}::text) ~ '^[+-]?([0-9]+([.][0-9]+)?|[.][0-9]+)$'
            then trim({{ column_name }}::text)::numeric({{ precision }}, {{ scale }})
        else null
    end
{%- endmacro %}

{% macro safe_boolean(column_name) -%}
    case
        when {{ column_name }} is null then null
        when nullif(trim({{ column_name }}::text), '') is null then null
        when lower(trim({{ column_name }}::text)) in ('true', 't', '1', 'sim', 's') then true
        when lower(trim({{ column_name }}::text)) in ('false', 'f', '0', 'nao', 'não', 'n') then false
        else null
    end
{%- endmacro %}

{% macro safe_date(column_name) -%}
    case
        when {{ column_name }} is null then null
        when trim({{ column_name }}::text) in ('', '0001-01-01', '1900-01-01') then null
        when upper(trim({{ column_name }}::text)) = 'NAN' then null
        when trim({{ column_name }}::text) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
            then trim({{ column_name }}::text)::date
        else null
    end
{%- endmacro %}

{% macro safe_timestamp(column_name) -%}
    case
        when {{ column_name }} is null then null
        when nullif(trim({{ column_name }}::text), '') is null then null
        when upper(trim({{ column_name }}::text)) = 'NAN' then null
        else {{ column_name }}::timestamp
    end
{%- endmacro %}
