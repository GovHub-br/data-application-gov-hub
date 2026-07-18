{% macro coalesce_por_nome(relation, nomes) %}
{#-
  COALESCE robusto sobre dados sujos: dado um `relation` (source/ref) e uma lista
  de nomes-alvo, descobre as colunas REAIS via information_schema e coalesce todas
  cujo nome (trim+lower) casa com algum alvo. Resolve o schema-drift do ingest,
  onde a mesma coluna aparece com/sem espaço (ex.: "valor pago" e "valor pago ").
  Em `dbt parse` (execute=false) get_columns retorna [] -> vira NULL (inócuo).
-#}
    {%- set cols = adapter.get_columns_in_relation(relation) -%}
    {%- set matches = [] -%}
    {%- for c in cols -%}
        {%- set cn = c.name | trim | lower -%}
        {%- for alvo in nomes -%}
            {%- if cn == (alvo | trim | lower) and ('"' ~ c.name ~ '"') not in matches -%}
                {%- do matches.append('"' ~ c.name ~ '"') -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endfor -%}
    {%- if matches | length > 0 -%}
        coalesce({{ matches | join(', ') }})
    {%- else -%}
        cast(null as text)
    {%- endif -%}
{% endmacro %}
