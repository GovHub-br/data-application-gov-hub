-- built-in schema generator
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is not none -%}
        {{ custom_schema_name | as_text }}
    {%- else -%}
        {{ target.schema }}
    {%- endif -%}
{%- endmacro %}
