-- Armazenando apenas os valores independentes das tabelas
-- valores calculados serão computados no dashboard
with

    -- Valor firmado
    valor_firmado_tb as (
        select
            id_plano_acao as plano_acao,
            vl_total_plano_acao as valor_firmado,
            sigla_unidade_descentralizada,
            ted_beneficiario_emitente,
            dt_ingest as dt_ingest_vf
        from {{ ref("planos_acao") }}
    ),

    -- Orçamento recebido
    -- Orçamento devolvido
    valores_orcamentos_tb as (
        select
            plano_acao,
            num_transf,
            sum(
                case when nc_evento not in ('300301', '300307') then nc_valor else 0 end
            ) as orcamento_recebido,
            sum(
                case when nc_evento in ('300301', '300307') then nc_valor else 0 end
            ) as orcamento_devolvido,
            max(dt_ingest) as dt_ingest_vo
        from {{ ref("nc_plano_acao") }}
        where ptres not in ('-9')
        group by plano_acao, num_transf
    ),
    -- Destaque orçamentario = Orçamento recebido - Orçamento devolvido
    -- Destaque a receber = Valor firmado - Destaque orçamentario
    -- Empenhado
    -- Empenho anulado
    -- Utilizado/pago
    valores_empenhados_tb as (
        select
            plano_acao,
            num_transf,
            sum(
                case when despesas_empenhadas > 0 then despesas_empenhadas else 0 end
            ) as empenhado,
            sum(
                case when despesas_empenhadas < 0 then - despesas_empenhadas else 0 end
            ) as empenho_anulado,
            sum(despesas_pagas) as despesas_pagas_exercicio,
            sum(restos_a_pagar_pagos) as despesas_pagas_rap,
            sum(restos_a_pagar_inscritos) as restos_a_pagar,
            sum(despesas_liquidadas) as despesas_liquidada,
            max(dt_ingest) as dt_ingest_ve
        from {{ ref("empenhos_plano_acao") }}
        group by plano_acao, num_transf
    ),

    -- Saldo empenho = Empenhado - Empenho anulado - Utilizado/pago
    -- Financeiro recebido
    -- Financeiro devolvido
    -- Utilizado/pago
    valores_financeiro_tb as (
        select
            plano_acao,
            num_transf,
            sum(
                case when pf_acao = 'TRANSFERENCIA' then pf_valor_linha else 0 end
            ) as financeiro_recebido,
            sum(
                case when pf_acao = 'DEVOLUCAO' then pf_valor_linha else 0 end
            ) as financeiro_devolvido,
            sum(
                case when pf_acao = 'CANCELAMENTO' then pf_valor_linha else 0 end
            ) as financeiro_cancelado,
            max(dt_ingest) as dt_ingest_vfin
        from {{ ref("pf_plano_acao") }}
        group by plano_acao, num_transf
    ),
    -- Saldo financeiro = Financeiro recebido - Financeiro devolvido - Utilizado/pago
    -- Financeiro a receber = Valor firmado - Financeiro recebido + Financeiro devolvido
    join_parcial as (
        select 
            *,
            greatest(vo.dt_ingest_vo, ve.dt_ingest_ve, vfin.dt_ingest_vfin) as dt_ingest_jp
        from valores_orcamentos_tb vo
        full join valores_empenhados_tb ve using (plano_acao, num_transf)
        full join valores_financeiro_tb vfin using (plano_acao, num_transf)

    )
-- Final
select
    plano_acao,
    num_transf,
    sigla_unidade_descentralizada,
    case
        when ted_beneficiario_emitente = 'beneficiario'
        then 'beneficiario'
        when ted_beneficiario_emitente = 'emitente'
        then 'emitente'
        else 'nao_indicado'
    end as ted_beneficiario_emitente,
    valor_firmado,
    orcamento_recebido,
    orcamento_devolvido,
    empenhado,
    empenho_anulado,
    despesas_pagas_exercicio,
    despesas_pagas_rap,
    restos_a_pagar,
    despesas_liquidada,
    financeiro_recebido,
    financeiro_devolvido,
    financeiro_cancelado,
    greatest(vf.dt_ingest_vf, jp.dt_ingest_jp) as dt_ingest
from valor_firmado_tb vf
full join join_parcial jp using (plano_acao)
where (plano_acao is not null) or (num_transf is not null)
