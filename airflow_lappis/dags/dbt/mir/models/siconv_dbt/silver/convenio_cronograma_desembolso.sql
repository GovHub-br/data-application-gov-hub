{{ config(materialized="table") }}

with
    convenio as (
        select *
        from {{ ref("convenios_consolidados") }}
    ),
    cronograma_desembolso as (
        select *
        from {{ ref("cronograma_desembolso") }}
    )

select
    c.*,
    cd.nr_parcela_crono_desembolso,
    cd.mes_crono_desembolso,
    cd.ano_crono_desembolso,
    cd.tipo_resp_crono_desembolso,
    cd.valor_parcela_crono_desembolso
from convenio c
left join cronograma_desembolso cd on c.nr_convenio = cd.nr_convenio