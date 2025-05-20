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
            s.restos_a_pagar_pago as siafi_restos_a_pagar_pago
        from compras_gov_data as c
        full join siafi_data as s using (contrato_id, mes_ref)

    ),

    preenchimento as (select contrato_id, mes_ref from {{ ref("preenchimento_meses") }})

--
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
    siafi_restos_a_pagar_pago
from partial_result
full join preenchimento using (contrato_id, mes_ref)
