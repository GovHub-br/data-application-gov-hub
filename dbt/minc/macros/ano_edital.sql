{% macro ano_edital(col) %}
{#-
  Extrai o ANO do edital de um texto: prioriza o padrão "NN/AAAA" (número do
  edital, ex.: "03/2024"), senão qualquer "20xx" solto. Valida faixa [2013,2026]
  (corta lixo tipo 2057). Retorna NULL se não achar ano válido. Sempre edital-year.
-#}
(
    case
        when substring({{ col }} from '[0-9]{1,2}\s*/\s*(20[0-9]{2})') ~ '^20[0-9]{2}$'
             and substring({{ col }} from '[0-9]{1,2}\s*/\s*(20[0-9]{2})')::int between 2013 and 2026
            then substring({{ col }} from '[0-9]{1,2}\s*/\s*(20[0-9]{2})')
        when substring({{ col }} from '(20[0-9]{2})') ~ '^20[0-9]{2}$'
             and substring({{ col }} from '(20[0-9]{2})')::int between 2013 and 2026
            then substring({{ col }} from '(20[0-9]{2})')
    end
)
{% endmacro %}
