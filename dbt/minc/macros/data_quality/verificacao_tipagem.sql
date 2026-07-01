{% macro test_verificacao_tipagem(model, nome_tabela, nome_coluna, tipo_esperado) %}
    with
        column_info as (
            select
                table_schema,
                table_name,  -- Nome real da coluna no information_schema
                column_name,  -- Nome real da coluna no information_schema
                data_type
            from information_schema.columns
            where
                table_schema || '.' || table_name = '{{ nome_tabela }}'
                and column_name = '{{ nome_coluna }}'
        ),
        comparison as (
            select
                '{{ nome_tabela }}' as nome_tabela,
                '{{ nome_coluna }}' as nome_coluna,
                '{{ tipo_esperado }}' as tipo_esperado,
                data_type as actual_type
            from column_info
        )
    select *
    from comparison
    where actual_type != tipo_esperado
{% endmacro %}
