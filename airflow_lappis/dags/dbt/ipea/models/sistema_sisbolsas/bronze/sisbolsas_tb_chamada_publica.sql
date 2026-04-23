{{ config(materialized="table") }}

with
    sisbolsas_tb_chamada_publica as (
        select
            co_chamada_publica::text as co_chamada_publica,
            co_projeto::text as co_projeto,
            co_situacao_chamada::text as co_situacao_chamada,
            co_usuario_criacao::text as co_usuario_criacao,
            co_programa::text as co_programa,
            nu_chamada_publica::text as nu_chamada_publica,
            nu_ano::text as nu_ano,
            ds_chamada_publica::text as ds_chamada_publica,
            ds_numero_sei::text as ds_numero_sei,
            {{ safe_numeric('vl_global_estimado') }} as vl_global_estimado,
            dt_ini_pesquisa::text as dt_ini_pesquisa,
            dt_fim_pesquisa::text as dt_fim_pesquisa,
            dt_publicacao_dou::text as dt_publicacao_dou,
            dt_previsao_resultado::text as dt_previsao_resultado,
            dt_ini_bolsa::text as dt_ini_bolsa,
            dt_criacao::text as dt_criacao,
            dt_inicio_inscricao::text as dt_inicio_inscricao,
            dt_fim_inscricao::text as dt_fim_inscricao,
            dt_inicio_julgamento::text as dt_inicio_julgamento,
            dt_fim_julgamento::text as dt_fim_julgamento,
            dt_publicacao_resultado::text as dt_publicacao_resultado,
            tp_moeda::text as tp_moeda,
            dt_fim_recurso::text as dt_fim_recurso,
            dt_inicio_recurso::text as dt_inicio_recurso,
            dt_inicio_previsao_bolsa::text as dt_inicio_previsao_bolsa
        from {{ source("sisbolsas", "tb_chamada_publica") }}
    )

select *
from sisbolsas_tb_chamada_publica
