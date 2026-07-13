{{ config(materialized="table") }}

with
    sisbolsas_tb_dado_bancario as (
        select
            co_dado_bancario::text as co_dado_bancario,
            co_instituicao_financeira::text as co_instituicao_financeira,
            ds_numero_conta::text as ds_numero_conta,
            ds_numero_agencia::text as ds_numero_agencia,
            {{ safe_boolean('in_ativo') }} as in_ativo,
            co_dado_pessoal::text as co_dado_pessoal
        from {{ source("sisbolsas", "tb_dado_bancario") }}
    )

select *
from sisbolsas_tb_dado_bancario
