{{ config(materialized="table") }}

with
    convenio as (
        select *
        from {{ ref("convenios_consolidados") }}
    ),
    desbloqueio as (
        select *
        from {{ ref("desbloqueio") }}
    )

select
    c.*,
    d.nr_ob,
    d.data_cadastro,
    d.data_envio,
    d.tipo_recurso_desbloqueio,
    d.vl_total_desbloqueio,
    d.vl_desbloqueado,
    d.vl_bloqueado
from convenio c
left join desbloqueio d on c.nr_convenio = d.nr_convenio