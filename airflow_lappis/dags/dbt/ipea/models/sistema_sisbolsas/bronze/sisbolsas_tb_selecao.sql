{{ config(materialized="table") }}

with
    sisbolsas_tb_selecao as (
        select
            co_selecao::text as co_selecao,
            co_chamada_publica::text as co_chamada_publica,
            co_modalidade::text as co_modalidade,
            co_estado::text as co_estado,
            co_area_formacao::text as co_area_formacao,
            co_nivel_escolaridade::text as co_nivel_escolaridade,
            tp_atuacao::text as tp_atuacao,
            st_nivel_escolaridade::text as st_nivel_escolaridade,
            qt_duracao::text as qt_duracao,
            {{ safe_numeric('vl_total') }} as vl_total,
            qt_bolsa::text as qt_bolsa,
            ds_area_especializacao::text as ds_area_especializacao,
            ds_outra_formacao::text as ds_outra_formacao,
            {{ safe_boolean('in_anexo_projeto') }} as in_anexo_projeto,
            {{ safe_boolean('in_anexo_idioma') }} as in_anexo_idioma,
            {{ safe_boolean('in_anexo_historico_escolar') }} as in_anexo_historico_escolar,
            dt_criacao::text as dt_criacao,
            co_usuario::text as co_usuario,
            co_cidade::text as co_cidade,
            nu_passo_avaliacao::text as nu_passo_avaliacao,
            st_avaliacao::text as st_avaliacao,
            {{ safe_boolean('in_entrevista') }} as in_entrevista,
            nu_selecao::text as nu_selecao,
            {{ safe_boolean('in_anexo_outro') }} as in_anexo_outro,
            ds_anexo_outro::text as ds_anexo_outro
        from {{ source("sisbolsas", "tb_selecao") }}
    )

select *
from sisbolsas_tb_selecao
