{{ config(materialized="table") }}

with
    sisbolsas_tb_instituicao_financeira as (
        select
            co_instituicao_financeira::text as co_instituicao_financeira,
            ds_nome::text as ds_nome,
            nu_codigo::text as nu_codigo
        from {{ source("sisbolsas", "tb_instituicao_financeira") }}
    )

select *
from sisbolsas_tb_instituicao_financeira
