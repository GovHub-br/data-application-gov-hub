{% macro name_formater(column_name) %}
    trim(
        translate(
            upper({{ column_name }}),
            'ÁÀÂÃÄÅÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇÑ',
            'AAAAAAEEEEIIIIOOOOOUUUUCN'
        )
    )
{% endmacro %}
