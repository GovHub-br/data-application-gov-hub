{% macro test_unique_combination_of_columns(model, combination_of_columns) %}
    with
        validation_errors as (
            select
                {{ combination_of_columns | join(', ') }},
                count(*) as row_count
            from {{ model }}
            group by {{ combination_of_columns | join(', ') }}
            having count(*) > 1
        )

    select *
    from validation_errors
{% endmacro %}
