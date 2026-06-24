{{ config(materialized="table") }}

with
    convenio as (
        select *
        from {{ ref("convenios_consolidados") }}
    ),
    desembolso as (
        select *
        from {{ ref("desembolso") }}
    )

select
    c.*,
    d.id_desembolso,
    d.dt_ult_desembolso,
    d.qtd_dias_sem_desembolso,
    d.data_desembolso,
    d.ano_desembolso,
    d.mes_desembolso,
    d.nr_siafi,
    d.ug_emitente_dh,
    d.observacao_dh,
    d.vl_desembolsado
from convenio c
left join desembolso d on c.nr_convenio = d.nr_convenio