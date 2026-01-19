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
            pf_valor_linha,
            dt_ingest as dt_ingest_pf
        from {{ ref("pf_tesouro") }}
    ),

    pf_transfere_gov as (
        select
            tx_numero_programacao as pf,
            ug_emitente_programacao as ug_emitente,
            id_plano_acao as plano_acao,
            (dt_ingest || '-03:00')::timestamptz as dt_ingest_tg 
        from {{ source("transfere_gov", "programacao_financeira") }} -- raw
    ),

    joined_by_transfere_gov as (
        select 
            pf,
            num_transf,
            emissao_mes,
            emissao_dia,
            ug_emitente,
            ug_favorecido,
            pf_evento,
            pf_evento_descricao,
            pf_acao,
            pf_valor_linha,
            t.plano_acao, 
            greatest(pf.dt_ingest_pf, t.dt_ingest_tg) as dt_ingest
        from programacoes_financeira pf
        inner join pf_transfere_gov t using (pf, ug_emitente)
    ),

    joined_by_num_transf as (
        select 
            pf.pf,
            pf.num_transf,
            pf.emissao_mes,
            pf.emissao_dia,
            pf.ug_emitente,
            pf.ug_favorecido,
            pf.pf_evento,
            pf.pf_evento_descricao,
            pf.pf_acao,
            pf.pf_valor_linha,
            v.plano_acao,
            pf.dt_ingest_pf as dt_ingest
        from programacoes_financeira pf
        inner join {{ ref("num_transf_n_plano_acao") }} v using (num_transf)
        -- Exclui registros que já existem em joined_by_transfere_gov
        where not exists (
            select 1 
            from pf_transfere_gov t 
            where t.pf = pf.pf and t.ug_emitente = pf.ug_emitente
        )
    )

select *
from joined_by_transfere_gov
union all
select *
from joined_by_num_transf