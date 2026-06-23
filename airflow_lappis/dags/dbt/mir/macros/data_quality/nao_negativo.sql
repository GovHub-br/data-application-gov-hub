{% macro test_nao_negativo(model, column_name) %}
    -- Teste generico de valor nao-negativo: falha se houver valores < 0.
    -- Valores nulos sao ignorados (null < 0 nao e verdadeiro).
    select {{ column_name }}
    from {{ model }}
    where {{ column_name }} < 0
{% endmacro %}
