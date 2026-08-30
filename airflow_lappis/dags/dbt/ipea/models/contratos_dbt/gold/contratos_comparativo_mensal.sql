with

    siafi_data as (
        select *, mes_lancamento as mes_ref from {{ ref("contratos_estagios") }}
    ),

    compras_gov_data as (select * from {{ ref("cronogramas_faturas_mensal") }}),

    partial_result as (
        select
            contrato_id,
            mes_ref,
            c.valor_cronograma as comprasgov_valor_cronograma,
            (
                c.valor_faturas_pagas + c.valor_faturas_pendentes
            ) as comprasgov_valor_faturas,
            c.saldo_contratual_disponivel as comprasgov_saldo_contratual_disponivel,
            s.valor_empenhado as siafi_valor_empenhado,
            s.valor_liquidado as siafi_valor_liquidado,
            s.valor_pago as siafi_valor_pago,
            s.restos_a_pagar as siafi_restos_a_pagar,
            s.restos_a_pagar_pago as siafi_restos_a_pagar_pago,
            greatest(c.dt_ingest::timestamptz, s.dt_ingest::timestamptz) as dt_ingest
        from compras_gov_data as c
        full join siafi_data as s using (contrato_id, mes_ref)

    ),

    preenchimento as (select contrato_id, mes_ref from {{ ref("preenchimento_meses") }}),

    contratos as (
        select
            id,
            numero,
            fornecedor_cnpj_cpf_idgener,
            fornecedor_tipo,
            fornecedor_nome,
            objeto,
            vigencia_inicio,
            vigencia_fim,
            dt_ingest as dt_ingest_contratos
        from {{ ref("contratos") }}
    ),

    naturezas_por_contrato as (
        select
            contrato_id,
            array_agg(distinct natureza_despesa_detalhada order by natureza_despesa_detalhada)
                filter (where natureza_despesa_detalhada is not null) as naturezas_despesa_detalhada
        from {{ ref("contratos_empenhos") }}
        where contrato_id is not null
        group by contrato_id
    ),

    comparativo_mensal as (
        select
            contrato_id,
            mes_ref,
            comprasgov_valor_cronograma,
            comprasgov_valor_faturas,
            comprasgov_saldo_contratual_disponivel,
            siafi_valor_empenhado,
            siafi_valor_liquidado,
            siafi_valor_pago,
            siafi_restos_a_pagar,
            siafi_restos_a_pagar_pago,
            dt_ingest
        from partial_result
        full join preenchimento using (contrato_id, mes_ref)
    )

--
select
    ccm.contrato_id,
    ccm.mes_ref,
    ccm.comprasgov_valor_cronograma,
    ccm.comprasgov_valor_faturas,
    ccm.comprasgov_saldo_contratual_disponivel,
    ccm.siafi_valor_empenhado,
    ccm.siafi_valor_liquidado,
    ccm.siafi_valor_pago,
    ccm.siafi_restos_a_pagar,
    ccm.siafi_restos_a_pagar_pago,
    c.numero,
    c.fornecedor_cnpj_cpf_idgener,
    c.fornecedor_tipo,
    c.fornecedor_nome,
    c.objeto,
    c.vigencia_inicio,
    c.vigencia_fim,
    n.naturezas_despesa_detalhada,
    greatest(ccm.dt_ingest::timestamptz, c.dt_ingest_contratos::timestamptz) as dt_ingest
from comparativo_mensal as ccm
left join contratos as c on ccm.contrato_id = c.id
left join naturezas_por_contrato as n on ccm.contrato_id = n.contrato_id
