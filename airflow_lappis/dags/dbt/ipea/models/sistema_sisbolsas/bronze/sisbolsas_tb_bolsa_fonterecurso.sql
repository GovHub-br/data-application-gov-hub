{{ config(materialized="table") }}

with
    sisbolsas_tb_bolsa_fonterecurso as (
        select
            co_bolsa_fonterecurso::text as co_bolsa_fonterecurso,
            co_selecao::text as co_selecao,
            nu_bolsa::text as nu_bolsa,
            co_fonte_financeira::text as co_fonte_financeira,
            dt_fim_vigencia::text as dt_fim_vigencia,
            dt_inicio_vigencia::text as dt_inicio_vigencia,
            st_situacao::text as st_situacao
        from {{ source("sisbolsas", "tb_bolsa_fonterecurso") }}
    )

select *
from sisbolsas_tb_bolsa_fonterecurso
