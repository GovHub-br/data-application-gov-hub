{% macro create_udfs() %}
    {{ adapter.dispatch('create_udfs', 'ipea')() }}
{% endmacro %}

{% macro postgres__create_udfs() %}

create schema if not exists {{ target.schema }};

    {{ create_f_parse_dates() }}
    ;
    {{ create_f_format_nc() }}
    ;

{% endmacro %}

{% macro default__create_udfs() %}
    select 1 as udfs_skipped;
{% endmacro %}
