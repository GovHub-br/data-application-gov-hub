{{ config(materialized="table") }}

with
    sisbolsas_tb_bolsa_coordenador as (
        select
            co_bolsa_coordenador::text as co_bolsa_coordenador,
            co_selecao::text as co_selecao,
            nu_bolsa::text as nu_bolsa,
            regexp_replace(ds_cpf, '[^0-9]', '', 'g')::text as ds_cpf,
            dt_inicio_vigencia::text as dt_inicio_vigencia,
            dt_fim_vigencia::text as dt_fim_vigencia,
            st_situacao::text as st_situacao
        from {{ source("sisbolsas", "tb_bolsa_coordenador") }}
    )

select *
from sisbolsas_tb_bolsa_coordenador
