-- Armazenando apenas os valores independentes das tabelas
-- valores calculados serão computados no dashboard
with

    -- Valor firmado
    valor_firmado_tb as (
        select id_plano_acao, vl_total_plano_acao as valor_firmado_tb
        from {{ ref("planos_acao") }}
    ),

    -- Orçamento recebido
    -- Orçamento devolvido
    valores_orcamentos_tb as (
        select
            plano_acao as id_plano_acao,
            sum(
                case when nc_evento not in ('300301', '300307') then nc_valor else 0 end
            ) as orcamento_recebido,
            sum(
                case when nc_evento in ('300301', '300307') then nc_valor else 0 end
            ) as orcamento_devolvido
        from {{ ref("nc_plano_acao") }}
        where ptres not in ('-9')
        group by id_plano_acao
    ),
    -- Destaque orçamentario = Orçamento recebido - Orçamento devolvido
    -- Destaque a receber = Valor firmado - Destaque orçamentario
    -- Empenhado
    -- Empenho anulado
    -- Utilizado/pago
    valores_empenhados_tb as (
        select
            plano_acao as id_plano_acao,
            sum(
                case when despesas_empenhadas > 0 then despesas_empenhadas else 0 end
            ) as empenhado,
            sum(
                case when despesas_empenhadas < 0 then - despesas_empenhadas else 0 end
            ) as empenho_anulado,
            sum(despesas_pagas) as despesas_pagas
        from {{ ref("empenhos_plano_acao") }}
        where abs(despesas_empenhadas) > 0
        group by id_plano_acao
    ),

    -- Saldo empenho = Empenhado - Empenho anulado - Utilizado/pago
    -- Financeiro recebido
    -- Financeiro devolvido
    -- Utilizado/pago
    valores_financeiro_tb as (
        select
            plano_acao as id_plano_acao,
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
        group by plano_acao
    )
-- Saldo financeiro = Financeiro recebido - Financeiro devolvido - Utilizado/pago
-- Financeiro a receber = Valor firmado - Financeiro recebido + Financeiro devolvido
-- Final
select *
from valor_firmado_tb
left join valores_orcamentos_tb using (id_plano_acao)
left join valores_empenhados_tb using (id_plano_acao)
left join valores_financeiro_tb using (id_plano_acao)
where id_plano_acao is not null
