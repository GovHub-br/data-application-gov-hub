{{ config(materialized="table") }}

with
    sisbolsas_tb_lancamento_despesa as (
        select
            co_lancamento_despesa::text as co_lancamento_despesa,
            co_usuario::text as co_usuario,
            co_selecao::text as co_selecao,
            nu_bolsa::text as nu_bolsa,
            co_tipo_despesa::text as co_tipo_despesa,
            st_lancamento_despesa::text as st_lancamento_despesa,
            {{ safe_numeric('vl_despesa') }} as vl_despesa,
            tp_comprovante::text as tp_comprovante,
            ds_titulo_comprovante::text as ds_titulo_comprovante,
            dt_comprovante::text as dt_comprovante,
            tx_lancamento_despesa::text as tx_lancamento_despesa,
            dt_criacao::text as dt_criacao,
            {{ safe_numeric('vl_aprovado') }} as vl_aprovado,
            {{ safe_numeric('vl_glossado') }} as vl_glossado,
            ds_numero_nota::text as ds_numero_nota
        from {{ source("sisbolsas", "tb_lancamento_despesa") }}
    )

select *
from sisbolsas_tb_lancamento_despesa
