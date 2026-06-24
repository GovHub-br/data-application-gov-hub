{{ config(materialized="table") }}

with
    convenio as (
        select *
        from {{ ref("convenios_consolidados") }}
    ),
    prorroga_oficio as (
        select *
        from {{ ref("prorroga_oficio") }}
    )

select
    c.*,
    p.nr_prorroga,
    p.dt_inicio_prorroga,
    p.dt_fim_prorroga,
    p.dias_prorroga,
    p.dt_assinatura_prorroga,
    p.sit_prorroga
from convenio c
left join prorroga_oficio p on c.nr_convenio = p.nr_convenio