{{ config(materialized="table") }}

with
    sisbolsas_tb_coordenador as (
        select
            co_chamada_publica::text as co_chamada_publica,
            regexp_replace(ds_cpf, '[^0-9]', '', 'g')::text as ds_cpf
        from {{ source("sisbolsas", "tb_coordenador") }}
    )

select *
from sisbolsas_tb_coordenador
