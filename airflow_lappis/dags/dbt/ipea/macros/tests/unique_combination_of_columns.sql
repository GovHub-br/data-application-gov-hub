{% test unique_combination_of_columns(model, combination_of_columns) %}

with validation as (
    select
        {% for column_name in combination_of_columns %}
            {{ column_name }}{% if not loop.last %},{% endif %}
        {% endfor %}
    from {{ model }}
    group by
        {% for column_name in combination_of_columns %}
            {{ column_name }}{% if not loop.last %},{% endif %}
        {% endfor %}
    having count(*) > 1
)

select *
from validation

{% endtest %}
