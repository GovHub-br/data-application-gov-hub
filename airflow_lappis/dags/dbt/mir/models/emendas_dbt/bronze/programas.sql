{{ config(materialized="table") }}

with
    programas_raw as (
        select
            id_programa::integer as id_programa,
            ano_programa::integer as ano_programa,
            modalidade_programa::text as modalidade_programa,
            codigo_programa::text as codigo_programa,
            id_orgao_superior_programa:: integer as id_orgao_superior_programa,
            sigla_orgao_programa::text as sigla_orgao_programa,
            nome_orgao_programa::text as nome_orgao_programa,
            id_unidade_gestora_programa::integer as id_unidade_gestora_programa,
            documentos_origem_programa::text as documentos_origem_programa,
            id_unidade_orcamentaria_responsavel_programa::integer as id_unidade_orcamentaria_responsavel_programa,
            data_inicio_ciencia_programa::date as data_inicio_ciencia_programa,
            data_fim_ciencia_programa::date as data_fim_ciencia_programa,
            valor_necessidade_financeira_programa::numeric(15, 2) as valor_necessidade_financeira_programa,
            valor_total_disponibilizado_programa::numeric(15, 2) as valor_total_disponibilizado_programa,
            valor_impedido_programa::numeric(15, 2) as valor_impedido_programa,
            valor_a_disponibilizar_programa::numeric(15, 2) as valor_a_disponibilizar_programa,
            valor_documentos_habeis_gerados_programa::numeric(15, 2) as valor_documentos_habeis_gerados_programa,
            valor_obs_geradas_programa::numeric(15, 2) as valor_obs_geradas_programa,
            valor_disponibilidade_atual_programa::numeric(15, 2) as valor_disponibilidade_atual_programa,
            (dt_ingest || '-03:00')::timestamptz as dt_ingest
        from {{ source("transferegov_emendas", "programas_especiais") }}
    )  --

select *
from programas_raw
