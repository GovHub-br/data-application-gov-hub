with
    empenhos_ids as (
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
            dt_ingest,
            -- Uma série de extrações que servirão de identificadores 
            right(ne_ccor, 12) as ne,
            replace(
                (
                    regexp_match(
                        ne_ccor_descricao,
                        '(FERENCIA|NUMERO|Nº|TED|CRICAO|TRANSF.|CAO|TRANSFERENCIA )(\s|^|-|)([0-9]{6}|1\w{5}|[0-9]{3}\.[0-9]{3})(\s|$|\.|,|-|\/)'
                    )
                )[3],
                '.',
                ''
            ) as num_transf,
            {{ target.schema }}.format_nc(
                regexp_substr(ne_ccor_descricao, '([0-9]{4}NC[0-9]+)')
            ) as nc
        from {{ ref("empenhos_tesouro") }}
    ),
    empenhos_filtrados as (
        select * from empenhos_ids where (nc != '') or (num_transf is not null)
    ),
    planos_de_acao as (
        select * from {{ ref("num_transf_n_plano_acao") }} where plano_acao is not null
    ),
    result_table as (
        select distinct *
        from empenhos_filtrados
        left join planos_de_acao using (num_transf)
    )  --

select 
    *,
    {{ brasilia_now_iso() }}::timestamptz as dt_transform
from result_table
