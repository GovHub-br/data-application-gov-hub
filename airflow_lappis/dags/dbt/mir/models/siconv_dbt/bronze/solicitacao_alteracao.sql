{{ config(materialized="table") }}

with
    solicitacao_alteracao_raw as (
        select
            nullif(id_solicitacao, '')::integer as id_solicitacao,
            nullif(nr_convenio, '')::integer as nr_convenio,
            nr_solicitacao::text as nr_solicitacao,
            situacao_solicitacao::text as situacao_solicitacao,
            objeto_solicitacao::text as objeto_solicitacao,
            case
                when nullif(data_solicitacao, '') is null then null
                when data_solicitacao ~ '^\d{2}/\d{2}/\d{4}$' then to_date(data_solicitacao, 'DD/MM/YYYY')
                else to_date(data_solicitacao, 'YYYY-MM-DD')
            end as data_solicitacao
        from {{ source("siconv", "solicitacao_alteracao") }}
    )

select *
from solicitacao_alteracao_raw