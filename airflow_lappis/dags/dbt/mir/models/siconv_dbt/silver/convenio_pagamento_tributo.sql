{{ config(materialized="table") }}

with
    convenio as (
        select *
        from {{ ref("convenio") }}
    ),
    pagamento_tributo as (
        select *
        from {{ ref("pagamento_tributo") }}
    )

select
    c.*,
    pt.data_tributo,
    pt.vl_pag_tributos
from convenio c
left join pagamento_tributo pt on c.nr_convenio = pt.nr_convenio