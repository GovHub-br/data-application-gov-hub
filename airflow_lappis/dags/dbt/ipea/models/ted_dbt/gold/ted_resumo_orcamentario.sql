-- Armazenando apenas os valores independentes das tabelas
-- valores calculados serão computados no dashboard
with

    -- Valor firmado
    valor_firmado_tb as (
        select
            id_plano_acao as plano_acao,
            vl_total_plano_acao as valor_firmado,
            sigla_unidade_descentralizada,
            ted_beneficiario_emitente
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
            ) as orcamento_devolvido
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
                case when despesas_empenhadas < 0 then -despesas_empenhadas else 0 end
            ) as empenho_anulado,
            sum(despesas_pagas) as despesas_pagas_exercicio,
            sum(restos_a_pagar_pagos) as despesas_pagas_rap,
            sum(restos_a_pagar_inscritos) as restos_a_pagar,
            sum(despesas_liquidadas) as despesas_liquidada
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
            ) as financeiro_cancelado
        from {{ ref("pf_plano_acao") }}
        group by plano_acao, num_transf
    ),

    -- Saldo financeiro = Financeiro recebido - Financeiro devolvido - Utilizado/pago
    -- Financeiro a receber = Valor firmado - Financeiro recebido + Financeiro devolvido
    join_parcial as (
        select
            valores_orcamentos_tb.orcamento_recebido,
            valores_orcamentos_tb.orcamento_devolvido,
            valores_empenhados_tb.empenhado,
            valores_empenhados_tb.empenho_anulado,
            valores_empenhados_tb.despesas_pagas_exercicio,
            valores_empenhados_tb.despesas_pagas_rap,
            valores_empenhados_tb.restos_a_pagar,
            valores_empenhados_tb.despesas_liquidada,
            valores_financeiro_tb.financeiro_recebido,
            valores_financeiro_tb.financeiro_devolvido,
            valores_financeiro_tb.financeiro_cancelado,
            coalesce(
                valores_orcamentos_tb.plano_acao,
                valores_empenhados_tb.plano_acao,
                valores_financeiro_tb.plano_acao
            ) as plano_acao,
            coalesce(
                valores_orcamentos_tb.num_transf,
                valores_empenhados_tb.num_transf,
                valores_financeiro_tb.num_transf
            ) as num_transf
        from valores_orcamentos_tb
        full join
            valores_empenhados_tb
            on valores_orcamentos_tb.plano_acao = valores_empenhados_tb.plano_acao
            and valores_orcamentos_tb.num_transf = valores_empenhados_tb.num_transf
        full join
            valores_financeiro_tb
            on coalesce(
                valores_orcamentos_tb.plano_acao, valores_empenhados_tb.plano_acao
            )
            = valores_financeiro_tb.plano_acao
            and coalesce(
                valores_orcamentos_tb.num_transf, valores_empenhados_tb.num_transf
            )
            = valores_financeiro_tb.num_transf

    )

-- Final
select
    join_parcial.num_transf,
    valor_firmado_tb.sigla_unidade_descentralizada,
    valor_firmado_tb.valor_firmado,
    join_parcial.orcamento_recebido,
    join_parcial.orcamento_devolvido,
    join_parcial.empenhado,
    join_parcial.empenho_anulado,
    join_parcial.despesas_pagas_exercicio,
    join_parcial.despesas_pagas_rap,
    join_parcial.restos_a_pagar,
    join_parcial.despesas_liquidada,
    join_parcial.financeiro_recebido,
    join_parcial.financeiro_devolvido,
    join_parcial.financeiro_cancelado,
    coalesce(valor_firmado_tb.plano_acao, join_parcial.plano_acao) as plano_acao,
    case
        when valor_firmado_tb.ted_beneficiario_emitente = 'beneficiario'
        then 'beneficiario'
        when valor_firmado_tb.ted_beneficiario_emitente = 'emitente'
        then 'emitente'
        else 'nao_indicado'
    end as ted_beneficiario_emitente
from valor_firmado_tb
full join join_parcial on valor_firmado_tb.plano_acao = join_parcial.plano_acao
where
    (coalesce(valor_firmado_tb.plano_acao, join_parcial.plano_acao) is not null)
    or (join_parcial.num_transf is not null)
