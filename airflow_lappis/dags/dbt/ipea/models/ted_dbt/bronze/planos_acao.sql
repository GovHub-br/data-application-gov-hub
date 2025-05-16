with

    planos_acao as (
        select
            id_plano_acao,
            id_programa,
            sigla_unidade_descentralizada,
            case
                when sigla_unidade_descentralizada = 'IPEA'
                then 'beneficiario'
                else 'emitente'
            end as ted_beneficiario_emitente,
            unidade_descentralizada,
            sigla_unidade_responsavel_execucao,
            unidade_responsavel_execucao,
            vl_total_plano_acao::numeric(15, 2) as vl_total_plano_acao,
            to_date(dt_inicio_vigencia, 'YYYY-mm-dd') as dt_inicio_vigencia,
            to_date(dt_fim_vigencia, 'YYYY-mm-dd') as dt_fim_vigencia,
            tx_objeto_plano_acao,
            tx_justificativa_plano_acao,
            in_forma_execucao_direta,
            in_forma_execucao_particulares,
            in_forma_execucao_descentralizada,
            tx_situacao_plano_acao,
            aa_ano_plano_acao,
            vl_beneficiario_especifico::numeric(15, 2) as vl_beneficiario_especifico,
            vl_chamamento_publico::numeric(15, 2) as vl_chamamento_publico,
            sq_instrumento,
            aa_instrumento
        from {{ source("transfere_gov", "planos_acao") }}
    )

--
select *
from planos_acao
