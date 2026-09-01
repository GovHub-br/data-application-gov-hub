with

    bolsas_pagas as (
        select
            credor_codigo,
            credor_nome,
            to_date(nullif(nullif(dia_emissao, ''), 'NaN'), 'DD/MM/YYYY') as dia_emissao,
            -- mes_emissao chega como YYYYMM float-ificado (ex.: "201905.0")
            to_date(
                nullif(nullif(split_part(mes_emissao, '.', 1), ''), 'NaN'), 'YYYYMM'
            ) as mes_emissao,
            split_part(ano_emissao, '.', 1) as ano_emissao,
            split_part(emissao_ano, '.', 1) as emissao_ano,
            -- mes_lancamento chega como MMM/YYYY (ex.: "MAI/2019")
            {{ target.schema }}.parse_date(
                nullif(nullif(mes_lancamento, ''), 'NaN')
            ) as mes_lancamento,
            fonte_recursos_codigo,
            fonte_recursos_descricao,
            pi_codigo,
            pi_descricao,
            -- ptres e natureza chegam float-ificados (ex.: "84848.0"); remove o sufixo ".0"
            split_part(ptres, '.', 1) as ptres,
            split_part(natureza_codigo, '.', 1) as natureza_codigo,
            natureza_descricao,
            processo,
            {{ parse_financial_value("valor") }} as valor,
            observacao,
            ne_ccor,
            documento_habil,
            item_informacao,
            {{ parse_financial_value("despesa_paga") }} as despesa_paga,
            {{ parse_financial_value("rp_processados") }} as rp_processados,
            {{ parse_financial_value("rp_nao_processados") }} as rp_nao_processados,
            {{ parse_financial_value("pagamentos_totais") }} as pagamentos_totais,
            (dt_ingest || '-03:00')::timestamptz as dt_ingest
        from {{ source("siafi", "bolsas_pagas") }}
    )

--
select *
from bolsas_pagas
