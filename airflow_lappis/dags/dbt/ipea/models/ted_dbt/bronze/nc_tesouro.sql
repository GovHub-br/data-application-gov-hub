{{ config(materialized="table") }}

with

    notas_credito_pre_2026 as (
        select
            {{ target.schema }}.parse_date(emissao_mes) as emissao_mes,
            to_date(emissao_dia, 'DD/MM/YYYY') as emissao_dia,
            nc,
            nc_transferencia,
            nc_fonte_recursos,
            nc_fonte_recursos_descricao,
            ptres,
            nc_evento,
            nc_evento_descricao as nc_evento_descr,
            ug_responsavel,
            ug_responsavel_descricao,
            natureza_despesa as nc_natureza_despesa,
            natureza_despesa_detalhada as nc_natureza_despesa_descricao,
            plano_interno,
            plano_detalhado_descricao1,
            plano_detalhado_descricao2,
            favorecido_doc,
            favorecido_doc_descricao,
            {{ parse_financial_value("nc_valor_linha") }} as nc_valor_linha,
            {{ parse_financial_value("movimento_liquido") }} as movimento_liquido,
            (dt_ingest || '-03:00')::timestamptz as dt_ingest,

            cast(null as varchar) as descricao,
            cast(null as varchar) as nc_item_detalhamento,
            cast(null as varchar) as ro,
            cast(null as varchar) as dc,
            cast(null as numeric(15, 2)) as item_total,
            cast(null as numeric(15, 2)) as total_lista,
            cast(null as varchar) as esfera_orcamentaria_codigo,
            cast(null as varchar) as esfera_orcamentaria_nome,
            cast(null as varchar) as emissao_ano,
            cast(null as varchar) as tipo_nc
        from {{ source("siafi", "nc_tesouro") }}
    ),

    notas_credito_pos_2026 as (
        select
            {{ target.schema }}.parse_date(emissao_mes) as emissao_mes,
            to_date(nullif(emissao_dia, ''), 'DD/MM/YYYY') as emissao_dia,
            nc,
            nc_transferencia,
            fonte_codigo as nc_fonte_recursos,
            fonte_nome as nc_fonte_recursos_descricao,
            ptres,
            cast(null as varchar) as nc_evento,
            tipo_nc as nc_evento_descr,
            emitente_codigo as ug_responsavel,
            emitente_nome as ug_responsavel_descricao,
            gnd_codigo as nc_natureza_despesa,
            gnd_nome as nc_natureza_despesa_descricao,
            pi_codigo as plano_interno,
            pi_nome as plano_detalhado_descricao1,
            cast(null as varchar) as plano_detalhado_descricao2,
            favorecido_codigo as favorecido_doc,
            favorecido_nome as favorecido_doc_descricao,
            {{ parse_financial_value("valor_celula") }} as nc_valor_linha,
            {{ parse_financial_value("total_lista") }} as movimento_liquido,
            (dt_ingest || '-03:00')::timestamptz as dt_ingest,

            descricao,
            nc_item_detalhamento,
            ro,
            dc,
            {{ parse_financial_value("item_total") }} as item_total,
            {{ parse_financial_value("total_lista") }} as total_lista,
            esfera_orcamentaria_codigo,
            esfera_orcamentaria_nome,
            emissao_ano,
            tipo_nc
        from {{ source("siafi", "nc_tesouro_pos_2026") }}
    )

select * from notas_credito_pre_2026
union all
select * from notas_credito_pos_2026
