{{ config(materialized="table") }}

with
    sisbolsas_tb_hist_lanca_despesa as (
        select
            co_hist_lanca_despesa::text as co_hist_lanca_despesa,
            co_lancamento_despesa::text as co_lancamento_despesa,
            st_lancamento_despesa::text as st_lancamento_despesa,
            co_usuario::text as co_usuario,
            tx_observacao::text as tx_observacao,
            dt_criacao::text as dt_criacao
        from {{ source("sisbolsas", "tb_hist_lanca_despesa") }}
    )

select *
from sisbolsas_tb_hist_lanca_despesa
