{% macro name_formater(column_name) %}
    TRIM(TRANSLATE(UPPER({{ column_name }}), 'ÁÀÂÃÄÅÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇÑ', 'AAAAAAEEEEIIIIOOOOOUUUUCN'))
{% endmacro %}
