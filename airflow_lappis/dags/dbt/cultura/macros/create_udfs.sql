{% macro create_udfs() %}

create schema if not exists {{ target.schema }};

    {{ create_f_parse_dates() }}
    ;
    {{ create_f_format_nc() }}
    ;

{% endmacro %}
