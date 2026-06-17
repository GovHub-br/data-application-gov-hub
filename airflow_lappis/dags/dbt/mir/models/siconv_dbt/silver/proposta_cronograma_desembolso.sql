{{ config(materialized="table") }}

with
    proposta as (
        select *
        from {{ ref("proposta") }}
    ),
    cronograma_desembolso as (
        select *
        from {{ ref("cronograma_desembolso") }}
    )

select
    p.*,
    c.nr_convenio as nr_convenio_cronograma,
    c.nr_parcela_crono_desembolso,
    c.mes_crono_desembolso,
    c.ano_crono_desembolso,
    c.tipo_resp_crono_desembolso,
    c.valor_parcela_crono_desembolso
from proposta p
left join cronograma_desembolso c on p.id_proposta = c.id_proposta