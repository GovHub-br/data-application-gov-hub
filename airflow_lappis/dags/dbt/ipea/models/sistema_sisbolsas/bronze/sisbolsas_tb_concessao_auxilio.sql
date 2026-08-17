{{ config(materialized="table") }}

with
    sisbolsas_tb_concessao_auxilio as (
        select
            co_usuario::text as co_usuario,
            co_selecao::text as co_selecao,
            nu_bolsa::text as nu_bolsa,
            co_situacao_conceauxi::text as co_situacao_conceauxi,
            {{ safe_boolean('in_cadin') }} as in_cadin,
            dt_inicio::text as dt_inicio,
            dt_fim::text as dt_fim,
            ds_numero_sei::text as ds_numero_sei,
            ds_token_aceite::text as ds_token_aceite,
            dt_ordem_bancaria::text as dt_ordem_bancaria,
            nu_titularidade::text as nu_titularidade,
            {{ safe_boolean('in_pendente_prestacao') }} as in_pendente_prestacao
        from {{ source("sisbolsas", "tb_concessao_auxilio") }}
    )

select *
from sisbolsas_tb_concessao_auxilio
