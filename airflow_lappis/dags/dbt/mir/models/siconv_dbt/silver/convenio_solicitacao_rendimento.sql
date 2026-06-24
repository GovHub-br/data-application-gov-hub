{{ config(materialized="table") }}

with
    convenio as (
        select *
        from {{ ref("convenios_consolidados") }}
    ),
    solicitacao_rendimento as (
        select *
        from {{ ref("solicitacao_rendimento_aplicacao") }}
    )

select
    c.*,
    r.id_solicitacao_rend_aplicacao,
    r.nr_solicitacao_rend_aplicacao,
    r.status_solicitacao_rend_aplicacao,
    r.data_solicitacao_rend_aplicacao,
    r.valor_solicitacao_rend_aplicacao,
    r.valor_aprovado_solicitacao_rend_aplicacao
from convenio c
left join solicitacao_rendimento r on c.nr_convenio = r.nr_convenio