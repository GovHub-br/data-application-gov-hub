{{ config(materialized="table") }}

with
    convenio as (
        select *
        from {{ ref("convenios_consolidados") }}
    ),
    termo_aditivo as (
        select *
        from {{ ref("termo_aditivo") }}
    )

select
    c.*,
    t.numero_ta,
    t.tipo_ta,
    t.vl_global_ta,
    t.vl_repasse_ta,
    t.vl_contrapartida_ta,
    t.dt_assinatura_ta,
    t.dt_inicio_ta,
    t.dt_fim_ta,
    t.justificativa_ta
from convenio c
left join termo_aditivo t on c.nr_convenio = t.nr_convenio