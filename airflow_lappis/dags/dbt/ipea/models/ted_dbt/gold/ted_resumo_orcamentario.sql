-- Armazenando apenas os valores independentes das tabelas
-- valores calculados serão computados no dashboard
with

    -- Valor firmado
    valor_firmado_tb as (
        select id_plano_acao as plano_acao, vl_total_plano_acao as valor_firmado
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
                case when despesas_empenhadas < 0 then - despesas_empenhadas else 0 end
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
        select *
        from valores_orcamentos_tb
        full join valores_empenhados_tb using (plano_acao, num_transf)
        full join valores_financeiro_tb using (plano_acao, num_transf)

    )
-- Final
select
    plano_acao,
    num_transf,
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
    financeiro_cancelado
from valor_firmado_tb
full join join_parcial using (plano_acao)
where (plano_acao is not null) or (num_transf is not null)
