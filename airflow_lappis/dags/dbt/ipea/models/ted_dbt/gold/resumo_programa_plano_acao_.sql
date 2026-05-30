with
    percent_vigencia as (
        select
            planos_acao.id_plano_acao,
            planos_acao.tx_objeto_plano_acao as objeto_plano_acao,
            planos_acao.dt_inicio_vigencia,
            planos_acao.dt_fim_vigencia,
            case
                when planos_acao.dt_fim_vigencia = planos_acao.dt_inicio_vigencia
                then 100
                when current_date < planos_acao.dt_inicio_vigencia
                then 0
                when current_date >= planos_acao.dt_fim_vigencia
                then 1
                else
                    (
                        round(
                            (current_date - planos_acao.dt_inicio_vigencia)::numeric
                            / nullif(
                                (
                                    planos_acao.dt_fim_vigencia
                                    - planos_acao.dt_inicio_vigencia
                                ),
                                0
                            )
                            * 100,
                            2
                        )
                        / 100
                    )
            end as percentual_conclusao,
            programas.id_programa as programa,
            programas.sigla_unidade_descentralizadora,
            programas.sigla_unidade_responsavel_acompanhamento,
            programas.tx_nome_institucional_programa as nome_institucional_programa,
            planos_acao.dt_ingest as dt_ingest_pa
        from {{ ref("planos_acao") }} as planos_acao
        inner join
            {{ source("transfere_gov", "programas") }} as programas
            on planos_acao.id_programa = programas.id_programa
    )

select
    ro.plano_acao,
    ro.num_transf,
    ro.sigla_unidade_descentralizada,
    ro.ted_beneficiario_emitente,
    ro.valor_firmado,
    ro.orcamento_recebido,
    ro.orcamento_devolvido,
    ro.empenhado,
    ro.empenho_anulado,
    ro.despesas_pagas_exercicio,
    ro.despesas_pagas_rap,
    ro.restos_a_pagar,
    ro.despesas_liquidada,
    ro.financeiro_recebido,
    ro.financeiro_devolvido,
    ro.financeiro_cancelado,
    pv.objeto_plano_acao,
    pv.dt_inicio_vigencia,
    pv.dt_fim_vigencia,
    pv.percentual_conclusao,
    pv.programa,
    pv.sigla_unidade_descentralizadora as sigla_unidade_descentralizadora_programa,
    pv.sigla_unidade_responsavel_acompanhamento,
    pv.nome_institucional_programa,
    case
        when ro.ted_beneficiario_emitente = 'emitente'
        then
            case
                when ro.financeiro_recebido >= ro.valor_firmado
                then 1
                when ro.financeiro_recebido = 0
                then 0
                else
                    (
                        round(
                            (ro.financeiro_recebido / nullif(ro.valor_firmado, 0)) * 100,
                            2
                        )
                        / 100
                    )
            end
        else
            case
                when
                    ro.despesas_pagas_exercicio + ro.despesas_pagas_rap
                    >= ro.valor_firmado
                then 1
                when ro.despesas_pagas_exercicio + ro.despesas_pagas_rap = 0
                then 0
                else
                    (
                        round(
                            (
                                (ro.despesas_pagas_exercicio + ro.despesas_pagas_rap)
                                / nullif(ro.valor_firmado, 0)
                            )
                            * 100,
                            2
                        )
                        / 100
                    )
            end
    end as percentual_conclusao_orcamentaria,
    greatest(ro.dt_ingest, pv.dt_ingest_pa) as dt_ingest
from {{ ref("ted_resumo_orcamentario") }} as ro
full join percent_vigencia as pv on ro.plano_acao = pv.id_plano_acao
