with
    percent_vigencia as (
        select
            pa.id_plano_acao,
            pa.tx_objeto_plano_acao as objeto_plano_acao,
            cast(pa.dt_inicio_vigencia as date) as dt_inicio_vigencia,
            cast(pa.dt_fim_vigencia as date) as dt_fim_vigencia,
            pg.id_programa as programa,
            pg.sigla_unidade_descentralizadora,
            pg.sigla_unidade_responsavel_acompanhamento,
            pg.tx_nome_institucional_programa as nome_institucional_programa,
            case
                when
                    cast(pa.dt_fim_vigencia as date) = cast(pa.dt_inicio_vigencia as date)
                then 100
                when current_date < cast(pa.dt_inicio_vigencia as date)
                then 0
                when current_date >= cast(pa.dt_fim_vigencia as date)
                then 1
                else
                    (
                        round(
                            cast(
                                (
                                    current_date - cast(pa.dt_inicio_vigencia as date)
                                ) as numeric
                            ) / nullif(
                                (
                                    cast(pa.dt_fim_vigencia as date)
                                    - cast(pa.dt_inicio_vigencia as date)
                                ),
                                0
                            )
                            * 100,
                            2
                        )
                        / 100
                    )
            end as percentual_conclusao
        from {{ ref("planos_acao") }} as pa
        inner join {{ ref("programas") }} as pg on pa.id_programa = pg.id_programa
    )

select
    *,
    case
        when ted_beneficiario_emitente = 'emitente'
        then
            case
                when financeiro_recebido >= valor_firmado
                then 1
                when financeiro_recebido = 0
                then 0
                else
                    (
                        round((financeiro_recebido / nullif(valor_firmado, 0)) * 100, 2)
                        / 100
                    )
            end
        when despesas_pagas_exercicio + despesas_pagas_rap >= valor_firmado
        then 1
        when despesas_pagas_exercicio + despesas_pagas_rap = 0
        then 0
        else
            (
                round(
                    (
                        (despesas_pagas_exercicio + despesas_pagas_rap)
                        / nullif(valor_firmado, 0)
                    )
                    * 100,
                    2
                )
                / 100
            )
    end as percentual_conclusao_orcamentaria
from {{ ref("ted_resumo_orcamentario") }} as ro
full join percent_vigencia as pv on ro.plano_acao = pv.id_plano_acao
