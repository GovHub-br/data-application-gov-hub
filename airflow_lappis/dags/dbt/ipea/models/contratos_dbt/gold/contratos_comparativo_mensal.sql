with

    siafi_data as (
        select *, mes_lancamento as mes_ref from {{ ref("contratos_estagios") }}
    ),

    compras_gov_data as (select * from {{ ref("cronogramas_faturas_mensal") }}),

    partial_result as (
        select
            c.contrato_id,
            c.mes_ref,
            c.valor_cronograma as comprasgov_valor_cronograma,
            c.saldo_contratual_disponivel as comprasgov_saldo_contratual_disponivel,
            s.valor_empenhado as siafi_valor_empenhado,
            s.valor_liquidado as siafi_valor_liquidado,
            s.valor_pago as siafi_valor_pago,
            s.restos_a_pagar as siafi_restos_a_pagar,
            s.restos_a_pagar_pago as siafi_restos_a_pagar_pago,
            (
                c.valor_faturas_pagas + c.valor_faturas_pendentes
            ) as comprasgov_valor_faturas
        from compras_gov_data as c
        full join
            siafi_data as s on c.contrato_id = s.contrato_id and c.mes_ref = s.mes_ref

    ),

    preenchimento as (select contrato_id, mes_ref from {{ ref("preenchimento_meses") }}),

    comparativo_final as (
        select
            p.contrato_id,
            p.mes_ref,
            comprasgov_valor_cronograma,
            comprasgov_valor_faturas,
            comprasgov_saldo_contratual_disponivel,
            siafi_valor_empenhado,
            siafi_valor_liquidado,
            siafi_valor_pago,
            siafi_restos_a_pagar,
            siafi_restos_a_pagar_pago
        from partial_result as p
        full join
            preenchimento as pm
            on p.contrato_id = pm.contrato_id
            and p.mes_ref = pm.mes_ref
    )

select cf.*, c.numero, c.fornecedor_cnpj_cpf_idgener
from comparativo_final as cf
left join {{ ref("contratos") }} as c on cf.contrato_id = c.id
