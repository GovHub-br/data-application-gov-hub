{{ config(materialized="table") }}

with
    sisbolsas_tb_bolsista as (
        select
            co_usuario::text as co_usuario,
            co_selecao::text as co_selecao,
            nu_bolsa::text as nu_bolsa,
            co_situacao_bolsista::text as co_situacao_bolsista,
            ds_numero_sei::text as ds_numero_sei,
            dt_inicio::text as dt_inicio,
            dt_fim::text as dt_fim
        from {{ source("sisbolsas", "tb_bolsista") }}
    )

select *
from sisbolsas_tb_bolsista
