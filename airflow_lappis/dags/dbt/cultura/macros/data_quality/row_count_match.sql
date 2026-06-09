{% macro test_row_count_match(model, source_table, target_table) %}
    with
        source_count as (select count(*) as row_count from {{ source_table }}),
        target_count as (select count(*) as row_count from {{ target_table }}),
        comparison as (
            select
                source_count.row_count as source_row_count,
                target_count.row_count as target_row_count
            from source_count, target_count
        )
    select *
    from comparison
    where source_row_count != target_row_count
{% endmacro %}
