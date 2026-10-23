with

    siafi_por_natureza as (
        select
            contrato_id,
            natureza_despesa_detalhada,
            natureza_despesa_detalhada_descricao,
            mes_lancamento,
            sum(valor_empenhado) as siafi_valor_empenhado,
            sum(valor_liquidado) as siafi_valor_liquidado,
            sum(valor_pago) as siafi_valor_pago,
            sum(restos_a_pagar) as siafi_restos_a_pagar,
            sum(restos_a_pagar_pago) as siafi_restos_a_pagar_pago,
            max(dt_ingest) as dt_ingest
        from {{ ref("contratos_estagios") }}
        where contrato_id is not null
        group by 1, 2, 3, 4
    ),

    contratos as (
        select
            id,
            numero,
            fornecedor_cnpj_cpf_idgener,
            fornecedor_tipo,
            fornecedor_nome,
            objeto,
            situacao,
            vigencia_inicio,
            vigencia_fim,
            dt_ingest as dt_ingest_contratos
        from {{ ref("contratos") }}
        where situacao = 'Ativo'
    )

--
select
    s.contrato_id,
    s.mes_lancamento,
    s.natureza_despesa_detalhada,
    s.natureza_despesa_detalhada_descricao,
    s.siafi_valor_empenhado,
    s.siafi_valor_liquidado,
    s.siafi_valor_pago,
    s.siafi_restos_a_pagar,
    s.siafi_restos_a_pagar_pago,
    c.numero,
    c.fornecedor_cnpj_cpf_idgener,
    c.fornecedor_tipo,
    c.fornecedor_nome,
    c.objeto,
    c.vigencia_inicio,
    c.vigencia_fim,
    greatest(s.dt_ingest, c.dt_ingest_contratos) as dt_ingest
from siafi_por_natureza as s
left join contratos as c on s.contrato_id = c.id
