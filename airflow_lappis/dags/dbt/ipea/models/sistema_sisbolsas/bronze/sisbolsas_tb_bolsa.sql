{{ config(materialized="table") }}

with
    sisbolsas_tb_bolsa as (
        select
            co_selecao::text as co_selecao,
            nu_bolsa::text as nu_bolsa,
            co_situacao_bolsa::text as co_situacao_bolsa,
            dt_inicio_bolsa::text as dt_inicio_bolsa,
            dt_fim_bolsa::text as dt_fim_bolsa,
            qt_duracao::text as qt_duracao
        from {{ source("sisbolsas", "tb_bolsa") }}
    )

select *
from sisbolsas_tb_bolsa
