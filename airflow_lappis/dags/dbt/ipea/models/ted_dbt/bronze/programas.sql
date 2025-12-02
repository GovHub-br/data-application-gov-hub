with

    programas as (
        select
            id_programa,
            tx_codigo_programa,
            aa_ano_programa,
            tx_situacao_programa,
            tx_nome_programa,
            sigla_unidade_descentralizadora,
            unidade_descentralizadora,
            sigla_unidade_responsavel_acompanhamento,
            unidade_responsavel_acompanhamento,
            tx_nome_institucional_programa,
            tx_objetivo_programa,
            tx_descricao_programa,
            in_grupo_investimento_obra,
            in_grupo_investimento_servico,
            in_grupo_investimento_equipamento,
            in_autoriza_subdescentralizacao_outro,
            in_autoriza_realizacao_despesas,
            in_autoriza_execucao_creditos_descentralizada,
            in_beneficiario_especifico,
            dt_recebimento_plano_beneficiario_inicio,
            dt_recebimento_plano_beneficiario_fim,
            in_chamamento_publico,
            dt_recebimento_plano_chamamento_inicio,
            dt_recebimento_plano_chamamento_fim,
            dt_ingest
        from {{ source("transfere_gov", "programas") }}
    )

--
select *
from programas
