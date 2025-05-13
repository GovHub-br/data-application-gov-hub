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
            tx_numero_programacao as pf,
            ug_emitente_programacao as ug_emitente,
            id_plano_acao as plano_acao
        from {{ source("transfere_gov", "programacao_financeira") }}
    ),

    joined_by_transfere_gov as (
        select pf.*, t.plano_acao
        from programacoes_financeira pf
        inner join pf_transfere_gov t using (pf, ug_emitente)
    ),

    joined_by_num_transf as (
        select pf.*, v.plano_acao
        from programacoes_financeira pf
        inner join {{ ref("num_transf_n_plano_acao") }} v using (num_transf)
    )

select *
from joined_by_transfere_gov
union
select *
from joined_by_num_transf
