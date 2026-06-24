{{ config(materialized="table") }}

with
    convenio as (
        select *
        from {{ ref("convenios_consolidados") }}
    ),
    solicitacao_alteracao as (
        select *
        from {{ ref("solicitacao_alteracao") }}
    )

select
    c.*,
    s.id_solicitacao,
    s.nr_solicitacao,
    s.situacao_solicitacao,
    s.objeto_solicitacao,
    s.data_solicitacao
from convenio c
left join solicitacao_alteracao s on c.nr_convenio = s.nr_convenio