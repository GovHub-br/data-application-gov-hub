{{ config(materialized="table") }}

with
    planos_raw as (
        select
            id_plano_acao::integer as id_plano_acao,
            codigo_plano_acao::text as codigo_plano_acao,
            ano_plano_acao::integer as ano_plano_acao,
            modalidade_plano_acao::text as modalidade_plano_acao,
            situacao_plano_acao::text as situacao_plano_acao,
            cnpj_beneficiario_plano_acao::text as cnpj_beneficiario_plano_acao,
            nome_beneficiario_plano_acao::text as nome_beneficiario_plano_acao,
            uf_beneficiario_plano_acao::text as uf_beneficiario_plano_acao,
            codigo_banco_plano_acao::text as codigo_banco_plano_acao,
            NULLIF(codigo_situacao_dado_bancario_plano_acao::numeric, 'NaN'::numeric)::integer as codigo_situacao_dado_bancario_plano_acao,
            nome_banco_plano_acao::text as nome_banco_plano_acao,
            NULLIF(numero_agencia_plano_acao::numeric, 'NaN'::numeric)::integer as numero_agencia_plano_acao,
            dv_agencia_plano_acao::text as dv_agencia_plano_acao,
            NULLIF(numero_conta_plano_acao::numeric, 'NaN'::numeric)::integer as numero_conta_plano_acao,
            dv_conta_plano_acao::text as dv_conta_plano_acao,
            nome_parlamentar_emenda_plano_acao::text as nome_parlamentar_emenda_plano_acao,
            ano_emenda_parlamentar_plano_acao::text as ano_emenda_parlamentar_plano_acao,
            codigo_parlamentar_emenda_plano_acao::text as codigo_parlamentar_emenda_plano_acao,
            sequencial_emenda_parlamentar_plano_acao::integer as sequencial_emenda_parlamentar_plano_acao,
            numero_emenda_parlamentar_plano_acao::text as numero_emenda_parlamentar_plano_acao,
            codigo_emenda_parlamentar_formatado_plano_acao::text as codigo_emenda_parlamentar_formatado_plano_acao,
            codigo_descricao_areas_politicas_publicas_plano_acao::text as codigo_descricao_areas_politicas_publicas_plano_acao,
            descricao_programacao_orcamentaria_plano_acao::text as descricao_programacao_orcamentaria_plano_acao,
            motivo_impedimento_plano_acao::text as motivo_impedimento_plano_acao,
            valor_custeio_plano_acao::numeric(15, 2) as valor_custeio_plano_acao,
            valor_investimento_plano_acao::numeric(15, 2) as valor_investimento_plano_acao,
            id_programa::integer as id_programa,
            (dt_ingest || '-03:00')::timestamptz as dt_ingest
        from {{ source("transferegov_emendas", "planos_acao_especiais") }}
    )  --

select *
from planos_raw
