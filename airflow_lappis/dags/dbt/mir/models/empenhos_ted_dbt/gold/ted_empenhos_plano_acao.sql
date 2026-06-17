{{ config(materialized="table") }}

with
    empenhos_mir as (
        select
            emissao_mes,
            emissao_dia,
            ne_ccor,
            ne_num_processo,
            ne_info_complementar,
            ne_ccor_descricao,
            doc_observacao,
            natureza_despesa,
            natureza_despesa_descricao,
            ne_ccor_favorecido,
            ne_ccor_favorecido_descricao,
            ne_ccor_ano_emissao,
            ptres,
            fonte_recursos_detalhada,
            fonte_recursos_detalhada_descricao,
            despesas_empenhadas,
            despesas_liquidadas,
            despesas_pagas,
            restos_a_pagar_inscritos,
            restos_a_pagar_pagos,
            ne,
            orgao_id,
            nc,
            num_transf,
            plano_acao,
            dt_ingest
        from {{ ref("empenhos_por_plano_acao") }}
        where plano_acao is not null
    )

select *
from empenhos_mir
