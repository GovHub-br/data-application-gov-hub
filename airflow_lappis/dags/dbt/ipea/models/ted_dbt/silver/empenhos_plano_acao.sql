with
    empenhos_ids as (
        select
            *,
            -- Uma série de extrações que servirão de identificadores 
            right(ne_ccor, 12) as ne,
            (
                regexp_match(
                    ne_ccor_descricao,
                    '(FERENCIA|NUMERO|Nº|TED|CRICAO|TRANSF.)(\s|^|-|)([0-9]{6}|1\w{5}|([0-9]{3}?:\.[0-9]{3}))(\s|$|\.|,|-|\/)'
                )
            )[3] as num_transf,
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

select *
from result_table
