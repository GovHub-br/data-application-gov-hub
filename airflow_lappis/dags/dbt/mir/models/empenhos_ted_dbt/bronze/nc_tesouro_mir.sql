{{ config(materialized='table')}}

with

    notas_credito_pre as (
        select
            programa_governo,
            programa_governo_descricao,
            acao_governo,
            acao_governo_descricao,

            nc,
            nc_transferencia,
            nc_fonte_recursos,
            nc_fonte_recursos_descricao,
            ptres,
            nc_evento_descricao,
            nc_ug_responsavel,
            nc_ug_responsavel_descricao,
            nc_natureza_despesa,
            nc_natureza_despesa_descricao,
            nc_plano_interno,
            nc_plano_interno_descricao1,
            favorecido_doc,
            favorecido_doc_descricao,

            favorecido_municipio,
            favorecido_municipio_descricao,

            {{parse_financial_value("nc_valor_linha")}} as valor_celula, 
            {{parse_financial_value("movimento_liquido_moeda_origem")}} as movimento_liquido_moeda_origem,

            (dt_ingest || '-03:00')::timestamptz as dt_ingest,

            cast(null as varchar) as  descricao,
            nc_plano_interno_descricao2,
            nc_evento,
            cast(null as varchar) as nc_item_detalhamento,
            cast(null as date) as emissao_dia,
            cast(null as varchar) as emissao_mes,
            cast(null as varchar) as emissao_ano,
            cast(null as varchar) as ro,
            cast(null as varchar) as dc,
            cast(null as numeric) as total_lista,
            cast(null as varchar) as esfera_orcamentaria_codigo,
            cast(null as varchar) as esfera_orcamentaria_nome
        from {{ source("siafi", "nc_tesouro_pre_2026") }}
    ),
    notas_credito_pos as (
        select
        -- campos nulos:
            cast(null as varchar) as programa_governo,
            cast(null as varchar) as programa_governo_descricao,
            cast(null as varchar) as acao_governo,
            cast(null as varchar) as acao_governo_descricao,

            nc,
            nc_transferencia,
            fonte_codigo as nc_fonte_recursos,
            fonte_nome as nc_fonte_recursos_descricao,
            ptres,
            tipo_nc as nc_evento_descricao,
            emitente_codigo as nc_ug_responsavel,
            emitente_nome as nc_ug_responsavel_descricao,
            gnd_codigo as nc_natureza_despesa,
            gnd_nome as nc_natureza_despesa_descricao,
            pi_codigo as nc_plano_interno,
            pi_nome as nc_plano_interno_descricao1,
            favorecido_codigo as nc_favorecido_doc,
            favorecido_nome as nc_favorecido_doc_descricao,

            cast(null as varchar) as favorecido_municipio,
            cast(null as varchar) as favorecido_municipio_descricao,

            {{parse_financial_value("valor_celula")}} as nc_valor_linha,
            {{parse_financial_value("total_lista")}} as movimento_liquido_moeda_origem,
            (dt_ingest || '-03:00')::timestamptz as dt_ingest,

            descricao,
            cast(null as varchar)as nc_plano_interno_descricao2,
            cast(null as varchar)as nc_evento,
            nc_item_detalhamento,
            to_date(emissao_dia, 'DD/MM/YYYY') as emissao_dia,
            emissao_mes,
            emissao_ano,
            ro,
            dc,
            {{parse_financial_value("total_lista")}} as total_lista,
            esfera_orcamentaria_codigo,
            esfera_orcamentaria_nome
        from {{ source("siafi", "nc_tesouro_pos__2026") }}
    )

select * from notas_credito_pre
union all
select * from notas_credito_pos
