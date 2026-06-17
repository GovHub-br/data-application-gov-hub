{{ config(materialized="table") }}

with
    meta_crono_fisico as (
        select *
        from {{ ref("meta_crono_fisico") }}
    ),
    cronograma_desembolso as (
        select *
        from {{ ref("cronograma_desembolso") }}
    )

select
    m.*,
    c.nr_parcela_crono_desembolso,
    c.mes_crono_desembolso,
    c.ano_crono_desembolso,
    c.tipo_resp_crono_desembolso,
    c.valor_parcela_crono_desembolso
from meta_crono_fisico m
left join cronograma_desembolso c on m.id_proposta = c.id_proposta