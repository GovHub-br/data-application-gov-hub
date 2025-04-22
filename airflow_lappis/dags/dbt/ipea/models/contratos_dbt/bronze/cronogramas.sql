{{ config(materialized="table") }}

with
    cronogramas as (
        select
            id::integer as id,
            contrato_id::integer as contrato_id,
            tipo::text as tipo,
            numero::text as numero,
            receita_despesa::text as receita_despesa,
            observacao::text as observacao,
            mesref::integer as mesref,
            anoref::integer as anoref,
            retroativo::text as retroativo,
            replace(replace(valor::text, '.', ''), ',', '.')::numeric(15, 2) as valor,
            vencimento::date as vencimento
        from {{ source("compras_gov", "cronograma") }}
    ),

    distinct_cronogramas as (select distinct * from cronogramas)

select *
from distinct_cronogramas
