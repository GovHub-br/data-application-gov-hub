{% macro parse_valor(col, teto=10000000) %}
{#-
  Converte valor monetário sujo em numeric; NULL quando não casar (nunca derruba o run).
  1) se fórmula ('=A+B'), pega trecho após o último '='.
  2) v = mantém só dígitos, ',', '.', '-'.
  3) n = normaliza p/ decimal-ponto (BR ',dd' / milhar multi-ponto / senão remove ',').
  4) só casta se n casar '^-?[0-9]+(\.[0-9]+)?$' E estiver em [0, teto].
     teto default = R$10M (corta lixo astronômico); teto=none desativa o teto.
-#}
{%- set v -%}
    regexp_replace(
        case
            when {{ col }} like '%=%'
                then reverse(split_part(reverse({{ col }}), '=', 1))
            else {{ col }}
        end,
        '[^0-9,.\-]', '', 'g'
    )
{%- endset -%}
{%- set n -%}
    case
        when ({{ v }}) ~ ',[0-9]{1,2}$'
            then replace(replace(({{ v }}), '.', ''), ',', '.')
        when (length(({{ v }})) - length(replace(({{ v }}), '.', ''))) > 1
            then regexp_replace(({{ v }}), '\.', '', 'g')
        else replace(({{ v }}), ',', '')
    end
{%- endset -%}
(
    case
        when {{ col }} is null then null
        when ({{ n }}) ~ '^-?[0-9]+(\.[0-9]+)?$'
             and ({{ n }})::numeric >= 0
             {% if teto is not none %}and ({{ n }})::numeric <= {{ teto }}{% endif %}
            then ({{ n }})::numeric
    end
)
{% endmacro %}
