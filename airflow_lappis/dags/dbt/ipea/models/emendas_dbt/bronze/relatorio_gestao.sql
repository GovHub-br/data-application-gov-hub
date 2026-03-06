{{ config(materialized="table") }}

with
    relatorio_gestao_raw as (
        select
            id_relatorio_gestao::integer as id_relatorio_gestao,
            situacao_relatorio_gestao::text as situacao_relatorio_gestao,
            parecer_relatorio_gestao::text as parecer_relatorio_gestao,
            id_plano_acao::integer as id_plano_acao,
            (dt_ingest || '-03:00')::timestamptz as dt_ingest
        from {{ source("transferegov_emendas", "relatorio_gestao_especial") }}
    )

select *
from relatorio_gestao_raw