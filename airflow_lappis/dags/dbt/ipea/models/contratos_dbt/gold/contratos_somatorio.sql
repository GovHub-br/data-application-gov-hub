select
    contrato_id,
    sum(comprasgov_valor_cronograma) as total_cronograma,
    sum(comprasgov_valor_faturas) as total_faturas,
    sum(comprasgov_saldo_contratual_disponivel) as total_saldo_disponivel,

    -- Indicador de Orçamento a Executar:
    sum(
        case when comprasgov_valor_faturas = 0 then comprasgov_valor_cronograma else 0 end
    ) as orcamento_a_executar,

    sum(siafi_valor_empenhado) as total_empenhado,
    sum(siafi_valor_liquidado) as total_liquidado,
    sum(siafi_valor_pago) as total_pago,
    max(dt_ingest) as dt_ingest

from {{ ref("contratos_comparativo_mensal") }}
group by contrato_id
