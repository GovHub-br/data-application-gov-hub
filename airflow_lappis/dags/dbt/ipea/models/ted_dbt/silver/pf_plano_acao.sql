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
        from {{ source("transfere_gov", "programacao_financeira") }}
    ),

    joined_by_transfere_gov as (
        select 
            pf.*,
            t.plano_acao,
            LEAST(pf.dt_ingest_pf, t.dt_ingest_tg) as dt_ingest
        from programacoes_financeira pf
        inner join pf_transfere_gov t using (pf, ug_emitente)
    ),

    joined_by_num_transf as (
        select 
            pf.*, 
            v.plano_acao,
            pf.dt_ingest_pf as dt_ingest
        from programacoes_financeira pf
        inner join {{ ref("num_transf_n_plano_acao") }} v using (num_transf)
    )

select 
    *,
    {{ brasilia_now_iso() }}::timestamptz as dt_transform
from joined_by_transfere_gov
union
select 
    *,
    {{ brasilia_now_iso() }}::timestamptz as dt_transform
from joined_by_num_transf
