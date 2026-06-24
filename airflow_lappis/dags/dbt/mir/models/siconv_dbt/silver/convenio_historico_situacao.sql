{{ config(materialized="table") }}

with
    convenio as (
        select *
        from {{ ref("convenios_consolidados") }}
    ),
    historico_situacao as (
        select *
        from {{ ref("historico_situacao") }}
    )

select
    c.*,
    h.dia_historico_sit,
    h.historico_sit,
    h.dias_historico_sit,
    h.cod_historico_sit
from convenio c
left join historico_situacao h on c.nr_convenio = h.nr_convenio