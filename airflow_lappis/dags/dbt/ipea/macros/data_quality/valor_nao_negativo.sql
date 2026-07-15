{% macro test_valor_nao_negativo(model, column_name) %}
    -- Garante que valores orçamentários não possuem inconsistências de sinal,
    -- ou seja, devem ser sempre maiores ou iguais a zero.
    select {{ column_name }} as valor
    from {{ model }}
    where {{ column_name }} < 0
{% endmacro %}
