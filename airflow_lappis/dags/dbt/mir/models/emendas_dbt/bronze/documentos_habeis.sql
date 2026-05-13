{{ config(materialized="table") }}

with
    documentos_habeis_raw as (
        select
            id_dh::integer as id_dh,
            id_empenho::integer as id_empenho,
            numero_documento_habil::text as numero_documento_habil,
            situacao_dh::integer as situacao_dh,
            descricao_situacao_dh::text as descricao_situacao_dh,
            tipo_documento_dh::text as tipo_documento_dh,
            ug_emitente_dh::integer as ug_emitente_dh,
            descricao_ug_emitente_dh::text as descricao_ug_emitente_dh,
            data_vencimento_dh::date as data_vencimento_dh,
            data_emissao_dh::date as data_emissao_dh,
            ug_pagadora_dh::integer as ug_pagadora_dh,
            descricao_ug_pagadora_dh::text as descricao_ug_pagadora_dh,
            variacao_patrimonial_diminuta_dh::text as variacao_patrimonial_diminuta_dh,
            passivo_transferencia_constitucional_legal_dh::text as passivo_transferencia_constitucional_legal_dh,
            centro_custo_empenho::text as centro_custo_empenho,
            codigo_siorg_empenho::integer as codigo_siorg_empenho,
            mes_referencia_empenho::text as mes_referencia_empenho,
            ano_referencia_empenho::integer as ano_referencia_empenho,
            ug_beneficiada_dh::integer as ug_beneficiada_dh,
            descricao_ug_beneficiada_dh::text as descricao_ug_beneficiada_dh,
            valor_dh::numeric(15, 2) as valor_dh,
            valor_rateio_dh::numeric(15, 2) as valor_rateio_dh,
            (dt_ingest || '-03:00')::timestamptz as dt_ingest
        from {{ source("transferegov_emendas", "documentos_habeis_especiais") }}
    )  --

select *
from documentos_habeis_raw
