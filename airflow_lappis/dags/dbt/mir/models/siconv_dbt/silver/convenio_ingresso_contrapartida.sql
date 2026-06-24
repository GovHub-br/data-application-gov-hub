{{ config(materialized="table") }}

with
    convenio as (
        select *
        from {{ ref("convenios_consolidados") }}
    ),
    ingresso_contrapartida as (
        select *
        from {{ ref("ingresso_contrapartida") }}
    )

select
    c.*,
    ic.dt_ingresso_contrapartida,
    ic.vl_ingresso_contrapartida as vl_ingresso_contrapartida_real
from convenio c
left join ingresso_contrapartida ic on c.nr_convenio = ic.nr_convenio