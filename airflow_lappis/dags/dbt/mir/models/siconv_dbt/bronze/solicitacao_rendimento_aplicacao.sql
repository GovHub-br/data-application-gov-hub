{{ config(materialized="table") }}

with
    solicitacao_rendimento_aplicacao_raw as (
        select
            nullif(id_solicitacao_rend_aplicacao, '')::integer as id_solicitacao_rend_aplicacao,
            nullif(nr_convenio, '')::integer as nr_convenio,
            nr_solicitacao_rend_aplicacao::text as nr_solicitacao_rend_aplicacao,
            status_solicitacao_rend_aplicacao::text as status_solicitacao_rend_aplicacao,
            case
                when nullif(data_solicitacao_rend_aplicacao, '') is null then null
                when data_solicitacao_rend_aplicacao ~ '^\d{2}/\d{2}/\d{4}$' then to_date(data_solicitacao_rend_aplicacao, 'DD/MM/YYYY')
                else to_date(data_solicitacao_rend_aplicacao, 'YYYY-MM-DD')
            end as data_solicitacao_rend_aplicacao,
            replace(nullif(valor_solicitacao_rend_aplicacao, ''), ',', '.')::numeric(15, 2) as valor_solicitacao_rend_aplicacao,
            replace(nullif(valor_aprovado_solicitacao_rend_aplicacao, ''), ',', '.')::numeric(15, 2) as valor_aprovado_solicitacao_rend_aplicacao
        from {{ source("siconv", "solicitacao_rendimento_aplicacao") }}
    )

select *
from solicitacao_rendimento_aplicacao_raw