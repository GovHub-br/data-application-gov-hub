with
    empenhos_ids as (
        select
            *,
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
        select distinct
            empenhos_filtrados.emissao_mes,
            empenhos_filtrados.emissao_dia,
            empenhos_filtrados.ne_ccor,
            empenhos_filtrados.ne_info_complementar,
            empenhos_filtrados.ne_ccor_descricao,
            empenhos_filtrados.doc_observacao,
            empenhos_filtrados.natureza_despesa,
            empenhos_filtrados.natureza_despesa_descricao,
            empenhos_filtrados.ne_ccor_favorecido_descricao,
            empenhos_filtrados.ne_ccor_ano_emissao,
            empenhos_filtrados.ptres,
            empenhos_filtrados.fonte_recursos_detalhada,
            empenhos_filtrados.fonte_recursos_detalhada_descricao,
            empenhos_filtrados.ne_num_processo,
            empenhos_filtrados.ne_ccor_favorecido,
            empenhos_filtrados.despesas_empenhadas,
            empenhos_filtrados.despesas_liquidadas,
            empenhos_filtrados.despesas_pagas,
            empenhos_filtrados.restos_a_pagar_inscritos,
            empenhos_filtrados.restos_a_pagar_pagos,
            empenhos_filtrados.ne,
            empenhos_filtrados.num_transf,
            empenhos_filtrados.nc,
            planos_de_acao.plano_acao
        from empenhos_filtrados
        left join
            planos_de_acao on empenhos_filtrados.num_transf = planos_de_acao.num_transf
    )  --

select *
from result_table
