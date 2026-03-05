{{ config(materialized="table") }}

with
    empenhos_raw as (
        select
            id_empenho::integer as id_empenho,
            id_minuta_empenho::text as id_minuta_empenho,
            numero_empenho::text as numero_empenho,
            situacao_empenho::integer as situacao_empenho,
            descricao_situacao_empenho::text as descricao_situacao_empenho,
            tipo_documento_empenho::integer as tipo_documento_empenho,
            descricao_tipo_documento_empenho::text as descricao_tipo_documento_empenho,
            status_processamento_empenho::text as status_processamento_empenho,
            ug_responsavel_empenho::integer as ug_responsavel_empenho,
            ug_emitente_empenho::integer as ug_emitente_empenho,
            descricao_ug_emitente_empenho::text as descricao_ug_emitente_empenho,
            fonte_recurso_empenho::text as fonte_recurso_empenho,
            plano_interno_empenho::text as plano_interno_empenho,
            ptres_empenho::numeric(15, 2) as ptres_empenho, -- verificar possibilidade de .0 
            grupo_natureza_despesa_empenho::text as grupo_natureza_despesa_empenho,
            natureza_despesa_empenho::text as natureza_despesa_empenho,
            subitem_empenho::text as subitem_empenho,
            categoria_despesa_empenho::text as categoria_despesa_empenho,
            modalidade_despesa_empenho::integer as modalidade_despesa_empenho,
            cnpj_beneficiario_empenho::text as cnpj_beneficiario_empenho,
            nome_beneficiario_empenho::text as nome_beneficiario_empenho,
            uf_beneficiario_empenho::text as uf_beneficiario_empenho,
            numero_ro_empenho::text as numero_ro_empenho,
            data_emissao_empenho::date as data_emissao_empenho,
            prioridade_desbloqueio_empenho::integer as prioridade_desbloqueio_empenho,
            valor_empenho::numeric(15, 2) as valor_empenho,
            id_plano_acao::integer as id_plano_acao,
            (dt_ingest || '-03:00')::timestamptz as dt_ingest
        from {{ source("transferegov_emendas", "empenhos_especiais") }}
    )  --

select *
from empenhos_raw
