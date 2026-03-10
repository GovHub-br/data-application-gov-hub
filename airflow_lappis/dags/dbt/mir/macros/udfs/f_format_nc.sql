{% macro create_f_format_nc() %}
    create or replace function {{ target.schema }}.format_nc(in_text text)
    returns text
    as $$ 
    
    with 

    pre_process as (
        select left(in_text, 7) as prefix,
            right(in_text, 4)::numeric as posfix
    )
    
    select concat(prefix, to_char(posfix, 'FM00000')) as result
    from pre_process 
    
    $$
    language sql
    ;
{% endmacro %}
