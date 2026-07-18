{% macro sem_acento(col) %}
{#- lower + remoção de acentos sem depender da extensão unaccent. -#}
    translate(
        lower(coalesce({{ col }}, '')),
        'áàâãäéèêëíìîïóòôõöúùûüçñ',
        'aaaaaeeeeiiiiooooouuuucn'
    )
{% endmacro %}
