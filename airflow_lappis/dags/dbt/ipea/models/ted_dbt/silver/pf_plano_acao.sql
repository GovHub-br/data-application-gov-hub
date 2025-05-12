with

    programacoes_financeira as (
        select
            pf,
            pf_inscricao as num_transf,
            emissao_mes,
            emissao_dia,
            ug_emitente,
            ug_favorecido,
            pf_evento,
            pf_evento_descricao,
            substring(pf_acao_descricao, '(\w+) ') as pf_acao,
            pf_valor_linha
        from {{ ref("pf_tesouro") }}
    ),

    pf_transfere_gov as (
        select
            id_plano_acao as plano_acao,
            ug_emitente_programacao as ug_emitente,
            tx_numero_programacao as pf
        from {{ source("transfere_gov", "programacao_financeira") }}
    ),

    joined_table as (
        select *
        from programacoes_financeira
        inner join pf_transfere_gov using (pf, ug_emitente)
    )

--
select *
from joined_table
