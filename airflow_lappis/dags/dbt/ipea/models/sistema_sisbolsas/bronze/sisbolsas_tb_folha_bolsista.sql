{{ config(materialized="table") }}

with
    sisbolsas_tb_folha_bolsista as (
        select
            co_folha_pagamento::text as co_folha_pagamento,
            co_usuario::text as co_usuario,
            co_selecao::text as co_selecao,
            nu_bolsa::text as nu_bolsa,
            co_dado_bancario::text as co_dado_bancario,
            co_diretoria::text as co_diretoria,
            co_unidade::text as co_unidade,
            co_fonte_financeira::text as co_fonte_financeira,
            nu_dias_pago::text as nu_dias_pago,
            {{ safe_numeric('vl_dia_pago') }} as vl_dia_pago,
            {{ safe_numeric('vl_total_pago') }} as vl_total_pago,
            {{ safe_boolean('in_conferencia') }} as in_conferencia
        from {{ source("sisbolsas", "tb_folha_bolsista") }}
    )

select *
from sisbolsas_tb_folha_bolsista
