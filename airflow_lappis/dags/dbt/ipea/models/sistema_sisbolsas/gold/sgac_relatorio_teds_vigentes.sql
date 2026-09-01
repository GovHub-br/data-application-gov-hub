with
    projetos as (
        select *
        from {{ ref("sgac_projetos_sgac") }}
        where data_vencimento >= current_date
          and coalesce(status, '') ilike '%execu%'
          and (
            instrumento ilike '%TED%'
            or instrumento ilike '%execução descentralizada%'
            or instrumento ilike '%Dispensa de TED%'
          )
    ),

    teds_siafi as (
        select *
        from {{ ref("sgac_teds_siafi") }}
    ),

    relatorio as (
        select
            p.entidades_externas,
            p.instrumento,
            p.diretoria_responsavel as diretoria,
            p.data_inicio as inicio_execucao,
            p.data_vencimento as data_de_conclusao,
            p.numero_do_proc as numero_do_processo,
            p.recursos_orcamentarios as recursos_externos,
            p.recursos_nao_orcamentarios as recursos_ipea,
            coalesce(p.recursos_orcamentarios, 0)
            + coalesce(p.recursos_nao_orcamentarios, 0) as recursos_totais_sgac,
            p.numero_siafi,
            p.termos_aditivos,
            null::text as apostilamentos,
            null::text as prorrogacao_oficio,
            null::text as planejamento_diarias_passagens,
            s.plano_acao,
            s.num_transf,
            s.sq_instrumento,
            s.sigla_unidade_descentralizada,
            s.unidade_descentralizada,
            s.sigla_unidade_descentralizadora,
            s.sigla_unidade_responsavel_acompanhamento,
            s.programa,
            s.nome_institucional_programa,
            s.percentual_vigencia,
            s.dt_inicio_vigencia as dt_inicio_vigencia_transferegov,
            s.dt_fim_vigencia as dt_fim_vigencia_transferegov,
            s.tx_situacao_plano_acao as situacao_plano_acao,
            s.vl_total_plano_acao,
            s.valor_firmado,
            s.numeros_empenho,
            s.quantidade_empenhos,
            s.numeros_nc,
            s.quantidade_ncs,
            s.orcamento_recebido,
            s.orcamento_devolvido,
            case
                when s.sgac_id is not null
                then coalesce(s.orcamento_recebido, 0)
                    - coalesce(s.orcamento_devolvido, 0)
            end as orcamento_liquido,
            s.empenhado,
            s.empenho_anulado,
            case
                when s.sgac_id is not null
                then coalesce(s.empenhado, 0) - coalesce(s.empenho_anulado, 0)
            end as empenhado_liquido,
            s.despesas_liquidada,
            case
                when s.sgac_id is not null
                then coalesce(s.despesas_pagas_exercicio, 0)
                    + coalesce(s.despesas_pagas_rap, 0)
            end as pago,
            s.despesas_pagas_exercicio,
            s.despesas_pagas_rap,
            s.restos_a_pagar,
            s.financeiro_recebido,
            s.financeiro_devolvido,
            s.financeiro_cancelado,
            case
                when s.sgac_id is not null
                then coalesce(s.financeiro_recebido, 0)
                    - coalesce(s.financeiro_devolvido, 0)
                    - coalesce(s.financeiro_cancelado, 0)
            end as financeiro_liquido,
            case
                when s.sgac_id is not null
                then greatest(
                    coalesce(s.valor_firmado, p.total_de_recursos, 0)
                    - (
                        coalesce(s.despesas_pagas_exercicio, 0)
                        + coalesce(s.despesas_pagas_rap, 0)
                    ),
                    0
                )
            end as pendente_de_pagamento,
            coalesce(
                s.valor_firmado,
                coalesce(p.recursos_orcamentarios, 0)
                + coalesce(p.recursos_nao_orcamentarios, 0)
            ) as valor_referencia_ted,
            case
                when s.valor_firmado is not null
                then (
                    coalesce(p.recursos_orcamentarios, 0)
                    + coalesce(p.recursos_nao_orcamentarios, 0)
                ) - s.valor_firmado
            end as diferenca_valor_sgac_siafi,
            p.status as situacao,
            p.id as sgac_id,
            (
                p.instrumento ilike '%TED%'
                or p.instrumento ilike '%execução descentralizada%'
                or p.instrumento ilike '%Dispensa de TED%'
            ) as eh_ted,
            p.data_vencimento - current_date as dias_para_conclusao,
            date_trunc('month', p.data_vencimento)::date as mes_conclusao,
            extract(year from p.data_vencimento) as ano_conclusao,
            case
                when p.data_vencimento is null then 'Sem data de conclusão'
                when p.data_vencimento < current_date then 'Vencido'
                when p.data_vencimento <= current_date + interval '30 days'
                then 'Vence em até 30 dias'
                when p.data_vencimento <= current_date + interval '60 days'
                then 'Vence entre 31 e 60 dias'
                when p.data_vencimento <= current_date + interval '90 days'
                then 'Vence entre 61 e 90 dias'
                else 'Acima de 90 dias'
            end as faixa_vencimento,
            case
                when p.data_inicio <= current_date
                    and p.data_vencimento >= current_date
                then 'Vigente'
                when p.data_vencimento < current_date then 'Encerrado/Vencido'
                when p.data_inicio > current_date then 'Ainda não iniciado'
                else 'Indefinido'
            end as status_vigencia,
            case
                when nullif(trim(coalesce(p.numero_siafi, '')), '') is null
                then 'Sem número SIAFI no SGAC'
                else 'Com número SIAFI no SGAC'
            end as status_numero_siafi,
            case
                when s.sgac_id is not null then 'Com cruzamento SIAFI/TransfereGov'
                else 'Sem cruzamento SIAFI/TransfereGov'
            end as status_cruzamento_siafi,
            case
                when s.sgac_id is null then 'Sem dados SIAFI'
                when coalesce(s.despesas_pagas_exercicio, 0)
                    + coalesce(s.despesas_pagas_rap, 0) = 0
                then 'Sem pagamento registrado'
                when coalesce(s.despesas_pagas_exercicio, 0)
                    + coalesce(s.despesas_pagas_rap, 0)
                    >= coalesce(
                        s.valor_firmado,
                        coalesce(p.recursos_orcamentarios, 0)
                        + coalesce(p.recursos_nao_orcamentarios, 0)
                    )
                then 'Pagamento total ou acima do valor de referência'
                else 'Pagamento parcial'
            end as status_financeiro,
            case
                when nullif(trim(coalesce(p.termos_aditivos, '')), '') is null
                then 'Sem termo aditivo informado'
                else 'Com termo aditivo informado'
            end as status_termo_aditivo,
            case
                when nullif(trim(coalesce(p.entidades_externas, '')), '') is null
                then 'Sem entidade externa informada'
                else 'Com entidade externa informada'
            end as status_entidade_externa,
            case
                when coalesce(
                    s.valor_firmado,
                    coalesce(p.recursos_orcamentarios, 0)
                    + coalesce(p.recursos_nao_orcamentarios, 0)
                ) > 0
                then round(
                    (
                        coalesce(s.despesas_pagas_exercicio, 0)
                        + coalesce(s.despesas_pagas_rap, 0)
                    )
                    / nullif(
                        coalesce(
                            s.valor_firmado,
                            coalesce(p.recursos_orcamentarios, 0)
                            + coalesce(p.recursos_nao_orcamentarios, 0)
                        ),
                        0
                    ) * 100,
                    2
                )
            end as percentual_execucao_financeira,
            case
                when s.valor_firmado is not null and s.valor_firmado > 0
                then round(
                    (
                        coalesce(s.empenhado, 0) - coalesce(s.empenho_anulado, 0)
                    ) / nullif(s.valor_firmado, 0) * 100,
                    2
                )
            end as percentual_empenhado,
            case
                when s.valor_firmado is not null and s.valor_firmado > 0
                then round(
                    coalesce(s.financeiro_recebido, 0)
                    / nullif(s.valor_firmado, 0) * 100,
                    2
                )
            end as percentual_financeiro_recebido
        from projetos as p
        left join teds_siafi as s
            on s.sgac_id = p.id
    )

select *
from relatorio
order by inicio_execucao desc, data_de_conclusao, diretoria, entidades_externas
