with

    notas_credito as (
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
            replace(nc_valor_linha, ',', '.')::numeric(15, 2) as nc_valor_linha,
            {{ parse_financial_value("movimento_liquido") }} as movimento_liquido
        from {{ source("siafi", "nc_tesouro") }}
    )

--
select *
from notas_credito
