{{ config(materialized="table") }}

with
    sisbolsas_tb_usuario as (
        select
            co_usuario::text as co_usuario,
            ds_nome::text as ds_nome,
            ds_login::text as ds_login,
            co_perfil::text as co_perfil,
            ds_email::text as ds_email,
            ds_senha::text as ds_senha,
            dt_criacao::text as dt_criacao,
            co_tipo_login::text as co_tipo_login,
            st_usuario::text as st_usuario,
            co_anexo::text as co_anexo,
            ds_token_redefinir_senha::text as ds_token_redefinir_senha,
            dt_token_senha::text as dt_token_senha
        from {{ source("sisbolsas", "tb_usuario") }}
    )

select *
from sisbolsas_tb_usuario
