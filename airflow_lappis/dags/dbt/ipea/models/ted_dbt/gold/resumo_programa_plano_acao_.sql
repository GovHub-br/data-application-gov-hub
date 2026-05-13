WITH percent_vigencia AS (
    select
        planos_acao.id_plano_acao,
        planos_acao.tx_objeto_plano_acao as objeto_plano_acao,
        planos_acao.dt_inicio_vigencia,
        planos_acao.dt_fim_vigencia,
        CASE
            WHEN planos_acao.dt_fim_vigencia = planos_acao.dt_inicio_vigencia THEN 100
            WHEN CURRENT_DATE < planos_acao.dt_inicio_vigencia THEN 0
            WHEN CURRENT_DATE >= planos_acao.dt_fim_vigencia THEN 1
            ELSE (ROUND(
                (CURRENT_DATE - planos_acao.dt_inicio_vigencia)::numeric /
                NULLIF((planos_acao.dt_fim_vigencia - planos_acao.dt_inicio_vigencia), 0) * 100,
                2) / 100
            )
        END AS percentual_conclusao,
        programas.id_programa as programa,
        programas.sigla_unidade_descentralizadora,
        programas.sigla_unidade_responsavel_acompanhamento,
        programas.tx_nome_institucional_programa as nome_institucional_programa,
        planos_acao.dt_ingest as dt_ingest_pa
    from {{ ref('planos_acao') }} as planos_acao
    inner join {{ source('transfere_gov', 'programas') }} as programas
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
    CASE
        WHEN ro.ted_beneficiario_emitente = 'emitente' THEN
            CASE
                WHEN ro.financeiro_recebido >= ro.valor_firmado THEN 1
                WHEN ro.financeiro_recebido = 0 THEN 0
                ELSE (ROUND(
                    (ro.financeiro_recebido / NULLIF(ro.valor_firmado, 0)) * 100,
                    2) / 100
                )
            END
        ELSE
            CASE
                WHEN ro.despesas_pagas_exercicio + ro.despesas_pagas_rap >= ro.valor_firmado THEN 1
                WHEN ro.despesas_pagas_exercicio + ro.despesas_pagas_rap = 0 THEN 0
                ELSE (ROUND(
                    ((ro.despesas_pagas_exercicio + ro.despesas_pagas_rap) / NULLIF(ro.valor_firmado, 0)) * 100,
                    2) / 100
                )
            END
    END AS percentual_conclusao_orcamentaria,
    greatest(ro.dt_ingest, pv.dt_ingest_pa) as dt_ingest
from {{ ref('ted_resumo_orcamentario') }} as ro
full join percent_vigencia as pv
    on ro.plano_acao = pv.id_plano_acao
