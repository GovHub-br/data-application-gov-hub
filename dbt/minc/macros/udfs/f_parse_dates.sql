-- Essa fun
{% macro create_f_parse_dates() %}

    create or replace function {{ target.schema }}.parse_date(in_text text)
    returns date
    as
        $$

        with

        split_column as (
        select
            split_part(in_text, '/', 1) as mes,
            split_part(in_text, '/', 2) as ano
        ),
        
        fixed_month as (
            select
                ano,
                case
                    when mes = 'JAN' then '01'
                    when mes = 'FEV' then '02'
                    when mes = 'MAR' then '03'
                    when mes = 'ABR' then '04'
                    when mes = 'MAI' then '05'
                    when mes = 'JUN' then '06'
                    when mes = 'JUL' then '07'
                    when mes = 'AGO' then '08'
                    when mes = 'SET' then '09'
                    when mes = 'OUT' then '10'
                    when mes = 'NOV' then '11'
                    when mes = 'DEZ' then '12'
                    else mes end as mes_num
            from split_column
        )
                
        select
            (to_date(ano::numeric - 1 || '-' || '12', 'YYYY-MM') + (mes_num || ' months')::interval)::date as result
        from fixed_month
    $$
    language sql
    ;

{% endmacro %}
