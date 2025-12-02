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
            pf_valor_linha,
            substring(pf_acao_descricao, '(\w+) ') as pf_acao
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
        from programacoes_financeira as pf
        inner join
            pf_transfere_gov as t on pf.pf = t.pf and pf.ug_emitente = t.ug_emitente
    ),

    joined_by_num_transf as (
        select pf.*, v.plano_acao
        from programacoes_financeira as pf
        inner join
            {{ ref("num_transf_n_plano_acao") }} as v on pf.num_transf = v.num_transf
    )

select *
from joined_by_transfere_gov
union
select *
from joined_by_num_transf
