{{ config(materialized="table") }}

with
    relatorio_gestao_novo_raw as (
        select
            id_relatorio_gestao_novo::integer as id_relatorio_gestao_novo,
            data_e_hora_relatorio_gestao_novo::timestamp as data_e_hora_relatorio_gestao_novo,
            tipo_relatorio_gestao_novo::text as tipo_relatorio_gestao_novo,
            valor_executado_relatorio_gestao_novo::numeric(15, 2) as valor_executado_relatorio_gestao_novo,
            valor_pendente_relatorio_gestao_novo::numeric(15, 2) as valor_pendente_relatorio_gestao_novo,
            situacao_relatorio_gestao_novo::text as situacao_relatorio_gestao_novo,
            id_plano_acao::integer as id_plano_acao,
            (dt_ingest || '-03:00')::timestamptz as dt_ingest
        from {{ source("transferegov_emendas", "relatorios_gestao_novo_especial") }}
    )

select *
from relatorio_gestao_novo_raw