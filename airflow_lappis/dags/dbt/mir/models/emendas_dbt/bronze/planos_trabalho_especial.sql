{{ config(materialized="table") }}

with
    planos_trabalho_raw as (
        select
            id_plano_trabalho::integer as id_plano_trabalho,
            situacao_plano_trabalho::text as situacao_plano_trabalho,
            ind_orcamento_proprio_plano_trabalho::text as ind_orcamento_proprio_plano_trabalho,
            data_inicio_execucao_plano_trabalho::timestamp as data_inicio_execucao_plano_trabalho,
            data_fim_execucao_plano_trabalho::timestamp as data_fim_execucao_plano_trabalho,
            prazo_execucao_meses_plano_trabalho::integer as prazo_execucao_meses_plano_trabalho,
            id_plano_acao::integer as id_plano_acao,
            classificacao_orcamentaria_pt::text as classificacao_orcamentaria_pt,
            ind_justificativa_prorrogacao_atraso_pt::boolean as ind_justificativa_prorrogacao_atraso_pt,
            ind_justificativa_prorrogacao_paralizacao_pt::boolean as ind_justificativa_prorrogacao_paralizacao_pt,
            justificativa_prorrogacao_pt::text as justificativa_prorrogacao_pt,
            (dt_ingest || '-03:00')::timestamptz as dt_ingest
        from {{ source("transferegov_emendas", "plano_trabalho_especial") }}
    )  --

select *
from planos_trabalho_raw
